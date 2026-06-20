package ethindex

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

var errReorg = errors.New("chain reorged")

const (
	finalizedCP = "checkpoint-finalized"
	danglingCP  = "checkpoint-dangling"
)

type BlockRef struct {
	Number uint64
	Hash   common.Hash
}

type Indexer struct {
	// Configs
	newHeadsBuffer int
	maxBlockRange  uint64
	finalityDepth  int
	maxBackoff     time.Duration
	retryFunc      func(err error, attempt int) bool

	c Client
	l *slog.Logger
	h Handler
	s Store

	// State
	mu       sync.RWMutex
	dangling *BlockRef
	head     *BlockRef
	isLive   bool

	// Chans
	subs map[chan BlockRef]struct{}
	stop chan struct{}
}

// New creates a new indexer instance
func New(c Client, h Handler, s Store, cfg Config) *Indexer {
	if cfg.FinalityDepth == 0 {
		cfg.FinalityDepth = 64
	}
	if cfg.NewHeadsBuffer == 0 {
		cfg.NewHeadsBuffer = 128
	}
	if cfg.MaxBlockRange == 0 {
		cfg.MaxBlockRange = 10_000
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = 2 * time.Second
	}
	if cfg.Retry == nil {
		cfg.Retry = func(err error, attempt int) bool { return false }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Indexer{
		newHeadsBuffer: cfg.NewHeadsBuffer,
		maxBlockRange:  cfg.MaxBlockRange,
		finalityDepth:  cfg.FinalityDepth,
		maxBackoff:     cfg.MaxBackoff,
		retryFunc:      cfg.Retry,

		l: cfg.Logger,
		c: c,
		h: h,
		s: s,

		stop: make(chan struct{}),
	}
}

// Run starts the indexer
func (idx *Indexer) Run(ctx context.Context) error {
	waitTime := idx.maxBackoff / 10
	attempt := 0

	for {
		start := time.Now()

		err := idx.run(ctx)
		if err == nil {
			return nil
		}
		if errors.Is(err, errReorg) {
			continue
		}
		if !idx.retryFunc(err, attempt) {
			return err
		}

		if time.Since(start) > idx.maxBackoff {
			waitTime = idx.maxBackoff / 10
		} else {
			waitTime = min(waitTime*2, idx.maxBackoff)
		}

		idx.l.Error("Indexer error",
			"error", err,
			"attempt", attempt,
			"retrying-in", waitTime)

		attempt++
	}
}

func (idx *Indexer) Subscribe() (ch chan BlockRef, unsub func()) {
	ch = make(chan BlockRef)
	idx.subs[ch] = struct{}{}
	return ch, func() { close(ch); delete(idx.subs, ch) }
}

// Stop gracefully shuts down the indexer.
func (idx *Indexer) Stop() {
	close(idx.stop)
}

func (idx *Indexer) run(ctx context.Context) error {
	if err := idx.restore(ctx); err != nil {
		return err
	}

	if err := idx.backfill(ctx); err != nil {
		return err
	}

	return idx.live(ctx)
}

func (idx *Indexer) live(ctx context.Context) error {
	ch := make(chan *types.Header, idx.newHeadsBuffer)
	sub, err := idx.c.SubscribeNewHead(ctx, ch)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	idx.l.Info("Subscribed to new heads")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return err
		case <-idx.stop:
			return nil
		case h := <-ch:
			if err := idx.processHead(ctx, h); err != nil {
				return err
			}
		}
	}
}

func (idx *Indexer) processHead(ctx context.Context, h *types.Header) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	ih := idx.head

	// Only check for reorgs when appending the strictly sequential next block.
	// This safely bypasses the check during the brief transition from the
	// backfilled state to the live network head.
	if ih.Number == h.Number.Uint64()-1 && ih.Hash != h.ParentHash {
		idx.l.Warn("reorg detected",
			"old", ih.Hash,
			"new", h.Hash)

		return errReorg
	}

	if err := idx.processRange(ctx, ih.Number+1, h.Number.Uint64()); err != nil {
		return err
	}

	idx.head = &BlockRef{
		Number: h.Number.Uint64(),
		Hash:   h.Hash(),
	}

	for s := range idx.subs {
		select {
		case s <- *idx.head:
		default:
			idx.l.Debug("Subscriber is busy, skipping")
		}
	}

	return nil
}

func (idx *Indexer) backfill(ctx context.Context) error {
	final, err := idx.c.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		return err
	}

	from := idx.h.Filter().FromBlock
	if idx.head != nil {
		from = idx.head.Number + 1
	}
	to := final.Number.Uint64()

	if from > to {
		idx.l.Info("Backfill skipped, already up to date with finalized block")

		return nil
	}

	idx.l.Info("Starting backfill",
		"from", from,
		"to", to)

	if err := idx.processRange(ctx, from, to); err != nil {
		return err
	}

	idx.head = &BlockRef{
		Number: final.Number.Uint64(),
		Hash:   final.Hash(),
	}

	return nil
}

func (idx *Indexer) processRange(ctx context.Context, from, to uint64) error {
	if from > to {
		panic(fmt.Errorf("invalid block range: from (%d) > to (%d)", from, to))
	}

	total := to - from + 1

	for start := from; start <= to; start += uint64(idx.maxBlockRange) {
		end := min(start+uint64(idx.maxBlockRange)-1, to)

		logs, err := idx.getLogs(ctx, start, end)
		if err != nil {
			return fmt.Errorf("fetch logs for blocks %d-%d: %w", start, end, err)
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		st := time.Now()
		if err := idx.h.Process(ctx, logs); err != nil {
			return fmt.Errorf("process logs: %w", err)
		}
		dur := time.Since(st)

		processed := end - from + 1
		progress := float64(processed) / float64(total) * 100.0

		idx.l.Info("Processed chunk",
			"progress", fmt.Sprintf("%.2f%%", progress),
			"duration", dur,
			"from", start,
			"to", end)
	}

	return nil
}

func (idx *Indexer) filterQuery(from, to uint64) ethereum.FilterQuery {
	f := idx.h.Filter()
	return ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(to),
		Addresses: f.Addresses,
		Topics:    f.Topics,
	}
}

func (idx *Indexer) getLogs(ctx context.Context, from, to uint64) ([]types.Log, error) {
	key := fmt.Sprintf("logs-%d-%d", from, to)

	if !idx.isLive {
		logsb, err := idx.s.Load(key)
		if err != nil {
			return nil, fmt.Errorf("loading logs: %w", err)
		}
		if len(logsb) > 0 {
			var logs Logs
			if err := logs.UnmarshalBinary(logsb); err != nil {
				return nil, fmt.Errorf("unmarshal logs: %w", err)
			}
			return logs, nil
		}
	}

	q := idx.filterQuery(from, to)

	st := time.Now()
	logs, err := idx.c.FilterLogs(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("fetch logs: %w", err)
	}
	dur := time.Since(st)

	idx.l.Debug("Fetched logs",
		"duration", dur,
		"count", len(logs),
		"from", from,
		"to", to)

	if !idx.isLive {
		logsb, err := Logs(logs).MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("marshal logs: %w", err)
		}
		if err := idx.s.Save(key, logsb); err != nil {
			return nil, fmt.Errorf("saving logs: %w", err)
		}
	}

	return logs, nil
}

func (idx *Indexer) restore(ctx context.Context) error {
	cpb, err := idx.s.Load(finalizedCP)
	if err != nil {
		return fmt.Errorf("loading checkpoint: %w", err)
	}
	if len(cpb) == 0 {
		return nil
	}

	var cp checkpoint
	if err := cp.UnmarshalBinary(cpb); err != nil {
		return fmt.Errorf("parsing checkpoint: %w", err)
	}

	if err := idx.h.Restore(ctx, cp.State); err != nil {
		return fmt.Errorf("restore checkpoint %s: %w", cp.BlockHash, err)
	}

	ref := &BlockRef{
		Number: cp.BlockNumber,
		Hash:   cp.BlockHash,
	}

	idx.head = ref

	idx.l.Info("Restored checkpoint",
		"number", cp.BlockNumber,
		"hash", cp.BlockHash)

	return nil
}

func (idx *Indexer) promote(ctx context.Context) error {
	var d, f *types.Header
	var dErr, fErr error

	var wg sync.WaitGroup

	wg.Go(func() { d, dErr = idx.c.HeaderByNumber(ctx, big.NewInt(int64(idx.dangling.Number))) })
	wg.Go(func() { f, fErr = idx.c.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber))) })

	wg.Wait()

	if dErr != nil {
		return dErr
	}
	if fErr != nil {
		return fErr
	}

	// Make sure dangling hash matches the node's
	// otherwise it means there was a reorg,
	// so we discard the dangling checkpoint
	if idx.dangling.Hash != d.Hash() {
		if err := idx.s.Delete(danglingCP); err != nil {
			return err
		}

		idx.dangling = nil

		return nil
	}

	// If dangling is older than finalized, we promote
	if d.Number.Cmp(f.Number) > 0 {
		return nil
	}

	// TODO

	// cpb, err := idx.s.Load(danglingCP)
	// if err != nil {
	// 	return fmt.Errorf("loading checkpoint: %w", err)
	// }

	// var cp checkpoint
	// if err := cp.UnmarshalBinary(cpb); err != nil {
	// 	return fmt.Errorf("unmarshal checkpoint: %w", err)
	// }

	// idx.finalized = idx.dangling

	return nil
}

func (idx *Indexer) checkpoint(ctx context.Context) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	state, err := idx.h.Snapshot(ctx)
	if err != nil {
		return err
	}

	cp := checkpoint{
		BlockNumber: idx.head.Number,
		BlockHash:   idx.head.Hash,
		State:       state,
	}

	cpb, err := cp.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	if err := idx.s.Save(danglingCP, cpb); err != nil {
		return fmt.Errorf("saving checkpoint: %w", err)
	}

	idx.dangling = &BlockRef{
		Number: cp.BlockNumber,
		Hash:   cp.BlockHash,
	}

	idx.l.Info("Saved dangling checkpoint",
		"number", cp.BlockNumber,
		"hash", cp.BlockHash)

	return nil
}
