package ethindex

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/sync/errgroup"
)

// Indexer indexes Ethereum logs from a finalized block onward, handling reorgs and checkpointing.
type Indexer struct {
	c Client
	h Handler
	f Filter
	l Logger
	s Store

	// Configs
	finalityDepth uint64
	maxBlockRange uint64
	maxConcurrent int

	// Guards
	processing atomic.Bool

	// State
	head        BlockRef
	dangling    BlockRef   // latest dangling checkpoint saved.
	pendingSave chan error // in-flight async dangling checkpoint save.
}

// NewIndexer builds the indexer, restores the finalized checkpoint, backfills
// to the node's current finalized block, and saves a fresh finalized checkpoint.
func NewIndexer(ctx context.Context, cfg Config) (*Indexer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	idx := &Indexer{
		c: cfg.Client,
		f: cfg.Filter,
		h: cfg.Handler,
		s: cfg.Store,
		l: cfg.Logger,

		finalityDepth: cfg.FinalityDepth,
		maxBlockRange: cfg.MaxBlockRange,
		maxConcurrent: cfg.MaxConcurrency,
	}

	idx.l.Info("starting indexer",
		"from_block", cfg.Filter.FromBlock,
		"finality_depth", cfg.FinalityDepth,
		"max_block_range", cfg.MaxBlockRange)

	if err := idx.restore(ctx); err != nil {
		return nil, err
	}

	if err := idx.sync(ctx); err != nil {
		return nil, err
	}

	idx.l.Info("indexer ready", "head", idx.head.Number)

	return idx, nil
}

// Process ingests a new head and handles gaps and reorgs.
func (idx *Indexer) Process(ctx context.Context, h *types.Header) error {
	if !idx.processing.CompareAndSwap(false, true) {
		panic("Process called concurrently")
	}
	defer idx.processing.Store(false)

	return idx.process(ctx, h)
}

// restore loads and applies the finalized checkpoint, if one exists.
func (idx *Indexer) restore(ctx context.Context) error {
	cp, ok, err := loadCheckpoint(ctx, idx.s, finalized)
	if err != nil {
		return fmt.Errorf("load finalized: %w", err)
	}
	if !ok {
		return nil
	}
	return idx.applyCheckpoint(ctx, cp)
}

// sync backfills from the restored head (or FromBlock on a fresh
// run) up to the node's finalized block, then saves a finalized checkpoint.
func (idx *Indexer) sync(ctx context.Context) error {
	start := time.Now()
	final, err := idx.c.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		return err
	}
	idx.l.Debug("fetched finalized header",
		"number", final.Number.Uint64(),
		"duration", time.Since(start))

	from := idx.f.FromBlock
	if idx.head != (BlockRef{}) {
		from = idx.head.Number + 1
	}
	to := final.Number.Uint64()

	if from > to {
		idx.l.Info("no backfill required", "head", idx.head.Number, "finalized", to)
		return nil
	}

	if err := idx.backfill(ctx, from, to); err != nil {
		return fmt.Errorf("backfill: %w", err)
	}

	idx.head = BlockRef{Number: to, Hash: final.Hash()}

	snapSt := time.Now()
	state, err := idx.h.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	snapDur := time.Since(snapSt)

	saveSt := time.Now()
	if err := saveCheckpoint(ctx, idx.s, finalized, checkpoint{idx.head, state}); err != nil {
		return fmt.Errorf("save finalized: %w", err)
	}
	saveDur := time.Since(saveSt)

	idx.l.Info("saved finalized checkpoint",
		"head", idx.head.Number,
		"snapshot", snapDur,
		"save", saveDur)

	return nil
}

// process handles a single received head, dispatching to fillGap/handleReorg/processHead.
func (idx *Indexer) process(ctx context.Context, h *types.Header) error {
	idxNum := idx.head.Number
	headNum := h.Number.Uint64()

	if headNum <= idxNum {
		idx.l.Warn("ignoring old head",
			"current", idxNum,
			"received", headNum)

		return nil
	}

	// Fill any gap between the current head and the received head.
	if headNum != idxNum+1 {
		return idx.fillGap(ctx, idxNum+1, headNum)
	}

	// Roll back to the finalized checkpoint on a parent-hash mismatch.
	if idx.head.Hash != h.ParentHash {
		idx.l.Warn("reorg detected",
			"head", idx.head.Number,
			"expected_parent", idx.head.Hash,
			"got_parent", h.ParentHash)

		return idx.handleReorg(ctx, h)
	}

	return idx.processHead(ctx, h)
}

// fillGap fetches and processes the missing headers between from and to.
func (idx *Indexer) fillGap(ctx context.Context, from, to uint64) error {
	start := time.Now()

	heads, err := idx.headersRange(ctx, from, to)
	if err != nil {
		return fmt.Errorf("headers range: %w", err)
	}

	idx.l.Info("filled missing heads", "from", from, "to", to, "duration", time.Since(start))

	for _, h := range heads {
		if err := idx.process(ctx, h); err != nil {
			return err
		}
	}

	return nil
}

// waitPending waits for the current pending save, if any.
func (idx *Indexer) waitPending(ctx context.Context) error {
	if idx.pendingSave == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-idx.pendingSave:
		idx.pendingSave = nil
		return err
	}
}

// handleReorg restores the finalized checkpoint and reprocesses the divergent head.
func (idx *Indexer) handleReorg(ctx context.Context, h *types.Header) error {
	if err := idx.waitPending(ctx); err != nil {
		return err
	}

	idx.head = BlockRef{}
	idx.dangling = BlockRef{}

	cp, ok, err := loadCheckpoint(ctx, idx.s, finalized)
	if err != nil {
		return fmt.Errorf("load finalized: %w", err)
	}
	if !ok {
		return errors.New("reorg detected but no finalized checkpoint found")
	}

	if err := idx.applyCheckpoint(ctx, cp); err != nil {
		return err
	}

	return idx.process(ctx, h)
}

// applyCheckpoint applies the handler state from a checkpoint snapshot
// and records the restored head.
func (idx *Indexer) applyCheckpoint(ctx context.Context, cp *checkpoint) error {
	start := time.Now()

	if err := idx.h.Restore(ctx, cp.State); err != nil {
		return fmt.Errorf("restore: %w", err)
	}

	idx.head = cp.Head

	idx.l.Info("restored from finalized checkpoint",
		"head", cp.Head.Number,
		"duration", time.Since(start))

	return nil
}

// process handles a new header and assumes it is strictly consecutive to idx.head.
func (idx *Indexer) processHead(ctx context.Context, h *types.Header) error {
	start := time.Now()

	logs, err := idx.c.FilterLogs(ctx, idx.f.blockQuery(h.Hash()))
	if err != nil {
		return fmt.Errorf("get logs: %w", err)
	}

	if err := idx.h.Process(ctx, logs); err != nil {
		return fmt.Errorf("process logs: %w", err)
	}

	idx.head = BlockRef{Number: h.Number.Uint64(), Hash: h.Hash()}

	idx.l.Debug("processed head",
		"number", h.Number.Uint64(),
		"logs", len(logs),
		"duration", time.Since(start))

	return idx.checkpoint(ctx)
}

// checkpoint saves a dangling checkpoint if none is pending, then promotes the
// dangling checkpoint to finalized once the head has aged past finalityDepth.
func (idx *Indexer) checkpoint(ctx context.Context) error {
	if idx.dangling == (BlockRef{}) {
		return idx.saveDanglingAsync(ctx)
	}

	if idx.head.Number >= idx.dangling.Number+idx.finalityDepth {
		return idx.promoteDangling(ctx)
	}

	return nil
}

// promoteDangling moves the dangling checkpoint to finalized.
func (idx *Indexer) promoteDangling(ctx context.Context) error {
	if err := idx.waitPending(ctx); err != nil {
		return err
	}

	start := time.Now()
	if err := idx.s.Move(ctx, string(dangling), string(finalized)); err != nil {
		return fmt.Errorf("move: %w", err)
	}

	idx.l.Info("promoted dangling checkpoint to finalized",
		"head", idx.dangling.Number,
		"duration", time.Since(start))

	idx.dangling = BlockRef{}

	return nil
}

// saveDanglingAsync spawns a goroutine that persists a dangling checkpoint.
func (idx *Indexer) saveDanglingAsync(ctx context.Context) error {
	start := time.Now()

	state, err := idx.h.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	snapDur := time.Since(start)

	cp := checkpoint{Head: idx.head, State: state}

	saveCh := make(chan error, 1)
	idx.pendingSave = saveCh

	go func() {
		saveSt := time.Now()
		err := saveCheckpoint(ctx, idx.s, dangling, cp)

		if err != nil {
			idx.l.Error("async save dangling failed",
				"head", cp.Head.Number,
				"error", err)
		} else {
			idx.l.Debug("saved dangling checkpoint",
				"head", cp.Head.Number,
				"snapshot", snapDur,
				"save", time.Since(saveSt),
				"duration", time.Since(start))
		}

		saveCh <- err
	}()

	idx.dangling = cp.Head

	return nil
}

// headersRange fetches headers [from, to] concurrently, preserving order.
func (idx *Indexer) headersRange(ctx context.Context, from, to uint64) ([]*types.Header, error) {
	if from > to {
		panic("invalid range: from > to")
	}

	total := to - from + 1

	heads := make([]*types.Header, total)
	eg, ctx := errgroup.WithContext(ctx)

	eg.SetLimit(idx.maxConcurrent)

	for i := range total {
		eg.Go(func() error {
			h, e := idx.c.HeaderByNumber(ctx, big.NewInt(int64(from+i)))
			heads[i] = h
			return e
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return heads, nil
}

// logsRange returns logs for [from, to], serving from the cache when present
// and fetching+caching from the client on miss.
func (idx *Indexer) logsRange(ctx context.Context, from, to uint64) ([]types.Log, error) {
	q := idx.f.rangeQuery(from, to)

	cached, err := loadLogs(ctx, idx.s, q)
	if err != nil {
		return nil, fmt.Errorf("load logs: %w", err)
	}
	if cached != nil {
		return cached, nil
	}

	logs, err := idx.c.FilterLogs(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("filter logs: %w", err)
	}

	if err := saveLogs(ctx, idx.s, q, logs); err != nil {
		return nil, fmt.Errorf("save logs: %w", err)
	}

	return logs, nil
}

// backfill fetches logs in chunks over [from, to] and processes them in order,
// caching each chunk on disk so a restart can resume without re-fetching.
func (idx *Indexer) backfill(ctx context.Context, from, to uint64) error {
	chunks := chunkBlockRange(from, to, idx.maxBlockRange)

	idx.l.Info("backfilling", "from", from, "to", to, "chunks", len(chunks))

	start := time.Now()

	for _, ch := range chunks {
		chStart := time.Now()

		logs, err := idx.logsRange(ctx, ch.from, ch.to)
		if err != nil {
			return fmt.Errorf("get logs: %w", err)
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		if err := idx.h.Process(ctx, logs); err != nil {
			return fmt.Errorf("process logs: %w", err)
		}

		idx.l.Debug("backfill chunk processed", "from", ch.from, "to", ch.to, "logs", len(logs), "duration", time.Since(chStart))
	}

	idx.l.Info("backfill complete", "from", from, "to", to, "duration", time.Since(start))

	return nil
}
