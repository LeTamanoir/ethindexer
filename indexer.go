package ethindexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/sync/errgroup"
)

const (
	checkpointKey       = "checkpoint"
	checkpointStagedKey = "checkpoint.staged"
)

// Indexer indexes Ethereum logs from a finalized block onward, handling reorgs and checkpointing.
type Indexer struct {
	c ChainReader
	h Handler
	s BlobStore

	log func(msg string, args ...any)
	cfg Config

	head   *blockRef // head of the last indexed block
	staged *blockRef // head of the staged checkpoint
}

// NewIndexer returns an unsynced Indexer.
func NewIndexer(o Options) *Indexer {
	if o.Client == nil || o.Handler == nil || o.Store == nil {
		panic("nil client, handler, or store")
	}

	o.Config.applyDefaults()

	log := func(msg string, args ...any) {}
	if o.LogFunc != nil {
		log = o.LogFunc
	}

	return &Indexer{c: o.Client, h: o.Handler, s: o.Store, log: log, cfg: o.Config}
}

// Open returns an Indexer synced to the finalized head.
func Open(o Options) (*Indexer, error) {
	return OpenContext(context.Background(), o)
}

// OpenContext returns an Indexer synced to the finalized head.
func OpenContext(ctx context.Context, o Options) (*Indexer, error) {
	idx := NewIndexer(o)
	if err := idx.Sync(ctx); err != nil {
		return nil, fmt.Errorf("sync: %w", err)
	}
	return idx, nil
}

// Sync restores state and catches up to the current finalized head.
func (i *Indexer) Sync(ctx context.Context) error {
	if i.head != nil {
		return errors.New("indexer already synced")
	}

	i.log("Syncing indexer",
		"finality_depth", i.cfg.FinalityDepth,
		"max_block_range", i.cfg.MaxBlockRange,
		"max_concurrent", i.cfg.MaxConcurrency)

	if _, err := i.restoreFinalized(ctx); err != nil {
		return err
	}

	if err := i.syncFinalized(ctx); err != nil {
		return err
	}

	i.log("Indexer synced", "head", i.head.Number)

	return nil
}

// Process ingests a new head and handles gaps and reorgs.
func (i *Indexer) Process(ctx context.Context, h *types.Header) error {
	if i.head == nil {
		return errors.New("indexer not synced")
	}

	idxNum := i.head.Number
	headNum := h.Number.Uint64()

	if headNum < idxNum {
		i.log("Ignoring older head", "current", idxNum, "received", headNum)
		return nil
	}

	// Same-height heads are either duplicates or reorgs.
	if idxNum == headNum {
		if h.Hash() == i.head.Hash {
			i.log("Ignoring duplicate head", "number", idxNum)
			return nil
		}

		return i.handleReorg(ctx, h)
	}

	// Ensure contiguous block processing.
	if headNum != idxNum+1 {
		return i.backfillUnfinalized(ctx, idxNum+1, headNum)
	}

	// Ensure chain continuity.
	if i.head.Hash != h.ParentHash {
		return i.handleReorg(ctx, h)
	}

	return i.processHead(ctx, h)
}

// syncFinalized backfills from the restored head (or FromBlock on a fresh run)
// up to the node's finalized block, then saves a finalized checkpoint.
func (i *Indexer) syncFinalized(ctx context.Context) error {
	final, err := i.c.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		return err
	}

	from := i.h.Filter().FromBlock
	if i.head != nil {
		from = i.head.Number + 1
	}
	to := final.Number.Uint64()

	if from > to {
		i.log("No backfill required", "head", i.head.Number, "finalized", to)

		return nil
	}

	if err := i.backfillFinalized(ctx, from, to); err != nil {
		return fmt.Errorf("backfill: %w", err)
	}

	i.head = &blockRef{Number: to, Hash: final.Hash()}

	if err := i.stageCheckpoint(ctx); err != nil {
		return fmt.Errorf("stage checkpoint: %w", err)
	}
	if err := i.promoteCheckpoint(ctx); err != nil {
		return fmt.Errorf("promote checkpoint: %w", err)
	}

	i.log("Saved checkpoint", "head", i.head.Number)

	return nil
}

// backfillUnfinalized fetches and processes the missing headers in [from, to].
//
// The range is assumed to be unfinalized, so each header is fetched
// individually and logs are queried by block hash to preserve reorg safety.
func (i *Indexer) backfillUnfinalized(ctx context.Context, from, to uint64) error {
	heads, err := i.headersRange(ctx, from, to)
	if err != nil {
		return fmt.Errorf("headers range: %w", err)
	}

	for _, h := range heads {
		if err := i.Process(ctx, h); err != nil {
			return err
		}
	}

	return nil
}

// handleReorg restores the finalized checkpoint and reprocesses the divergent head.
func (i *Indexer) handleReorg(ctx context.Context, h *types.Header) error {
	i.log("Reorg detected", "head", i.head.Number, "expected_parent", i.head.Hash, "got_parent", h.ParentHash)

	i.head = nil
	i.staged = nil

	ok, err := i.restoreFinalized(ctx)
	if err != nil {
		return fmt.Errorf("restore finalized: %w", err)
	}
	if !ok {
		return errors.New("reorg detected but no finalized checkpoint found")
	}

	return i.Process(ctx, h)
}

// restoreFinalized restores handler state from a checkpoint and records the head.
func (i *Indexer) restoreFinalized(ctx context.Context) (bool, error) {
	bin, err := i.s.Read(ctx, checkpointKey)
	if err != nil {
		return false, fmt.Errorf("store read: %w", err)
	}
	if len(bin) == 0 {
		return false, nil
	}

	cp, err := unmarshalCheckpoint(bin)
	if err != nil {
		return false, fmt.Errorf("unmarshal: %w", err)
	}

	if err := i.h.Restore(ctx, cp.state); err != nil {
		return false, fmt.Errorf("restore: %w", err)
	}

	h := cp.head // prevent escaping the whole checkpoint to the heap
	i.head = &h

	i.log("Restored checkpoint", "head", h.Number)

	return true, nil
}

// processHead handles a new header and assumes it is strictly consecutive to idx.head.
func (i *Indexer) processHead(ctx context.Context, h *types.Header) error {
	logs, err := i.c.FilterLogs(ctx, i.h.Filter().blockQuery(h.Hash()))
	if err != nil {
		return fmt.Errorf("filter logs: %w", err)
	}

	if err := i.h.Process(ctx, logs); err != nil {
		return fmt.Errorf("process logs: %w", err)
	}

	i.head = &blockRef{Number: h.Number.Uint64(), Hash: h.Hash()}

	// save a checkpoint if none is staged
	if i.staged == nil {
		return i.stageCheckpoint(ctx)
	}

	// promote staged to finalized once the head has aged past finalityDepth.
	if i.head.Number >= i.staged.Number+i.cfg.FinalityDepth {
		return i.promoteCheckpoint(ctx)
	}

	return nil
}

// promoteCheckpoint moves the staged checkpoint to finalized.
func (i *Indexer) promoteCheckpoint(ctx context.Context) error {
	if err := i.s.Move(ctx, checkpointStagedKey, checkpointKey); err != nil {
		return fmt.Errorf("move: %w", err)
	}

	i.log("Promoted checkpoint", "head", i.staged.Number)

	i.staged = nil

	return nil
}

// stageCheckpoint saves a staged checkpoint.
func (i *Indexer) stageCheckpoint(ctx context.Context) error {
	state, err := i.h.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}

	h := *i.head
	cp := checkpoint{h, state}

	bin, err := marshalCheckpoint(cp)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	if err := i.s.Write(ctx, checkpointStagedKey, bin); err != nil {
		return fmt.Errorf("store write: %w", err)
	}

	i.log("Staged checkpoint", "head", cp.head.Number)

	i.staged = &h

	return nil
}

// headersRange fetches headers [from, to] concurrently, preserving order.
func (i *Indexer) headersRange(ctx context.Context, from, to uint64) ([]*types.Header, error) {
	if from > to {
		panic("invalid range: from > to")
	}

	total := to - from + 1

	heads := make([]*types.Header, total)
	eg, ctx := errgroup.WithContext(ctx)

	eg.SetLimit(i.cfg.MaxConcurrency)

	for j := range total {
		eg.Go(func() error {
			h, e := i.c.HeaderByNumber(ctx, big.NewInt(int64(from+j)))
			heads[j] = h
			return e
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return heads, nil
}

// logsRange returns logs for [from, to], caching fetched results.
func (i *Indexer) logsRange(ctx context.Context, from, to uint64) ([]types.Log, error) {
	q := i.h.Filter().rangeQuery(from, to)

	{
		bin, err := i.s.Read(ctx, logsKey(q))
		if err != nil {
			return nil, fmt.Errorf("store read: %w", err)
		}
		if len(bin) > 0 {
			logs, err := unmarshalLogs(bin)
			if err != nil {
				return nil, fmt.Errorf("unmarshal: %w", err)
			}
			return logs, nil
		}
	}

	logs, err := i.c.FilterLogs(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("filter logs: %w", err)
	}

	{
		bin, err := marshalLogs(logs)
		if err != nil {
			return nil, fmt.Errorf("marshal: %w", err)
		}
		if err := i.s.Write(ctx, logsKey(q), bin); err != nil {
			return nil, fmt.Errorf("store write: %w", err)
		}
	}

	return logs, nil
}

// backfillFinalized fetches and processes logs over [from, to] in chunks.
//
// The range is assumed to be finalized, allowing logs to be queried by block
// range with FilterLogs instead of by block hash. This is more efficient but
// does not provide reorg safety.
func (i *Indexer) backfillFinalized(ctx context.Context, from, to uint64) error {
	chunks := chunkBlockRange(from, to, i.cfg.MaxBlockRange)

	i.log("Starting backfill", "from", from, "to", to, "chunks", len(chunks))

	for _, ch := range chunks {
		logs, err := i.logsRange(ctx, ch.from, ch.to)
		if err != nil {
			return fmt.Errorf("get logs: %w", err)
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		if err := i.h.Process(ctx, logs); err != nil {
			return fmt.Errorf("process logs: %w", err)
		}

		i.log("Backfill chunk processed", "from", ch.from, "to", ch.to, "logs", len(logs))
	}

	i.log("Backfill complete", "from", from, "to", to)

	return nil
}

type blockRange struct {
	from uint64
	to   uint64
}

func chunkBlockRange(from, to, size uint64) []blockRange {
	if size == 0 {
		panic("invalid block range size: 0")
	}
	var chunks []blockRange
	for start := from; start <= to; start += size {
		end := min(start+size-1, to)
		chunks = append(chunks, blockRange{start, end})
	}
	return chunks
}

func logsKey(q ethereum.FilterQuery) string {
	var b []byte

	for _, a := range q.Addresses {
		b = append(b, a[:]...)
	}
	for _, tt := range q.Topics {
		b = append(b, '-')
		for _, t := range tt {
			b = append(b, t[:]...)
		}
	}

	hash := crypto.Keccak256Hash(b)

	return fmt.Sprintf("logs-%d-%d-%s", q.FromBlock, q.ToBlock, hash)
}
