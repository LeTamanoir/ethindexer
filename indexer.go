package ethindexer

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/sync/errgroup"
)

const (
	checkpointKey       = "checkpoint"
	checkpointStagedKey = "checkpoint.staged"
)

// Indexer indexes Ethereum logs from a finalized block onward, handling reorgs and checkpointing.
type Indexer struct {
	opts Options

	head   *blockRef // head of the last indexed block
	staged *blockRef // head of the staged checkpoint

	lastStagedNum uint64 // block number of the most recent staged checkpoint
}

// NewIndexer returns an unsynced Indexer.
func NewIndexer(o Options) *Indexer {
	if o.Client == nil || o.Store == nil {
		panic("nil client or store")
	}
	if o.ProcessFunc == nil || o.SnapshotFunc == nil || o.RestoreFunc == nil {
		panic("nil process, snapshot, or restore function")
	}

	o.applyDefaults()

	return &Indexer{opts: o}
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

	start := time.Now()

	i.opts.LogFunc("Syncing indexer",
		"finality_depth", i.opts.FinalityDepth,
		"checkpoint_interval", i.opts.CheckpointInterval,
		"max_block_range", i.opts.MaxBlockRange,
		"max_concurrent", i.opts.MaxConcurrency)

	restored, err := i.restoreFinalized(ctx)
	if err != nil {
		return err
	}

	if !restored {
		if i.opts.InitFunc != nil {
			if err := i.opts.InitFunc(ctx, i.opts.Client); err != nil {
				return fmt.Errorf("init: %w", err)
			}
		}
	}

	if err := i.syncFinalized(ctx); err != nil {
		return err
	}

	i.opts.LogFunc("Indexer synced", "head", i.head.number, "duration", time.Since(start))

	return nil
}

// Process ingests a new head and handles gaps and reorgs.
func (i *Indexer) Process(ctx context.Context, h *types.Header) error {
	if i.head == nil {
		return errors.New("indexer not synced")
	}

	idxNum := i.head.number
	headNum := h.Number.Uint64()

	if headNum < idxNum {
		i.opts.LogFunc("Ignoring older head", "current", idxNum, "received", headNum)
		return nil
	}

	// same-height heads are either duplicates or reorgs.
	if idxNum == headNum {
		if h.Hash() == i.head.hash {
			i.opts.LogFunc("Ignoring duplicate head", "head", idxNum)
			return nil
		}

		return i.handleReorg(ctx, h)
	}

	// ensure contiguous block processing.
	if headNum != idxNum+1 {
		return i.backfillUnfinalized(ctx, idxNum+1, headNum)
	}

	// ensure chain continuity.
	if i.head.hash != h.ParentHash {
		return i.handleReorg(ctx, h)
	}

	return i.processHead(ctx, h)
}

// syncFinalized backfills from the restored head (or FromBlock on a fresh run)
// up to the node's finalized block, then saves a finalized checkpoint.
func (i *Indexer) syncFinalized(ctx context.Context) error {
	final, err := i.opts.Client.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		return err
	}

	from := i.opts.FromBlock
	if i.head != nil {
		from = i.head.number + 1
	}
	to := final.Number.Uint64()

	if from > to {
		i.opts.LogFunc("No backfill required", "head", i.head.number, "finalized", to)

		return nil
	}

	if err := i.backfillFinalized(ctx, from, to); err != nil {
		return fmt.Errorf("backfill: %w", err)
	}

	i.head = &blockRef{number: to, hash: final.Hash()}

	if err := i.stageCheckpoint(ctx); err != nil {
		return fmt.Errorf("stage checkpoint: %w", err)
	}
	if err := i.promoteCheckpoint(ctx); err != nil {
		return fmt.Errorf("promote checkpoint: %w", err)
	}

	return nil
}

// backfillUnfinalized fetches and processes the missing headers in [from, to].
//
// The range is assumed to be unfinalized, so each header is fetched
// individually and logs are queried by block hash to preserve reorg safety.
func (i *Indexer) backfillUnfinalized(ctx context.Context, from, to uint64) error {
	start := time.Now()

	heads, err := i.headersRange(ctx, from, to)
	if err != nil {
		return fmt.Errorf("headers range: %w", err)
	}

	i.opts.LogFunc("Fetched headers", "from", from, "to", to, "count", len(heads), "duration", time.Since(start))

	for _, h := range heads {
		if err := i.Process(ctx, h); err != nil {
			return err
		}
	}

	i.opts.LogFunc("Backfill unfinalized complete", "from", from, "to", to, "duration", time.Since(start))

	return nil
}

// handleReorg restores the finalized checkpoint and reprocesses the divergent head.
func (i *Indexer) handleReorg(ctx context.Context, h *types.Header) error {
	if i.head.number == h.Number.Uint64() {
		i.opts.LogFunc("Reorg detected at current head", "head", i.head.number, "current_hash", i.head.hash, "received_hash", h.Hash())
	} else {
		i.opts.LogFunc("Reorg detected", "head", i.head.number, "expected_parent", i.head.hash, "got_parent", h.ParentHash)
	}

	i.head = nil
	i.staged = nil
	i.lastStagedNum = 0

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
	start := time.Now()

	bin, err := i.opts.Store.Read(ctx, checkpointKey)
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

	if err := i.opts.RestoreFunc(ctx, cp.state); err != nil {
		return false, fmt.Errorf("restore: %w", err)
	}

	h := cp.head // prevent escaping the whole checkpoint to the heap
	i.head = &h
	i.lastStagedNum = h.number

	i.opts.LogFunc("Restored checkpoint", "head", h.number, "duration", time.Since(start))

	return true, nil
}

// processHead handles a new header and assumes it is strictly consecutive to idx.head.
func (i *Indexer) processHead(ctx context.Context, h *types.Header) error {
	logs, err := i.opts.Client.FilterLogs(ctx, i.opts.Filter.blockQuery(h.Hash()))
	if err != nil {
		return fmt.Errorf("filter logs: %w", err)
	}

	if err := i.opts.ProcessFunc(ctx, logs); err != nil {
		return fmt.Errorf("process logs: %w", err)
	}

	i.head = &blockRef{number: h.Number.Uint64(), hash: h.Hash()}

	// save a checkpoint if none is staged and enough blocks have passed
	if i.staged == nil {
		if i.head.number >= i.lastStagedNum+i.opts.CheckpointInterval {
			return i.stageCheckpoint(ctx)
		}
		return nil
	}

	// promote staged to finalized once the head has aged past finalityDepth.
	if i.head.number >= i.staged.number+i.opts.FinalityDepth {
		return i.promoteCheckpoint(ctx)
	}

	return nil
}

// promoteCheckpoint moves the staged checkpoint to finalized.
func (i *Indexer) promoteCheckpoint(ctx context.Context) error {
	start := time.Now()

	if err := i.opts.Store.Move(ctx, checkpointStagedKey, checkpointKey); err != nil {
		return fmt.Errorf("move: %w", err)
	}

	i.opts.LogFunc("Promoted checkpoint", "head", i.staged.number, "duration", time.Since(start))

	i.staged = nil

	return nil
}

// stageCheckpoint saves a staged checkpoint.
func (i *Indexer) stageCheckpoint(ctx context.Context) error {
	start := time.Now()

	state, err := i.opts.SnapshotFunc(ctx)
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}

	h := *i.head
	cp := checkpoint{h, state}

	bin, err := marshalCheckpoint(cp)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	if err := i.opts.Store.Write(ctx, checkpointStagedKey, bin); err != nil {
		return fmt.Errorf("store write: %w", err)
	}

	i.opts.LogFunc("Staged checkpoint", "head", cp.head.number, "duration", time.Since(start))

	i.staged = &h
	i.lastStagedNum = h.number

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

	eg.SetLimit(i.opts.MaxConcurrency)

	for j := range total {
		eg.Go(func() error {
			h, e := i.opts.Client.HeaderByNumber(ctx, big.NewInt(int64(from+j)))
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
	q := i.opts.Filter.rangeQuery(from, to)

	{
		bin, err := i.opts.Store.Read(ctx, logsCacheKey(q))
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

	logs, err := i.opts.Client.FilterLogs(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("filter logs: %w", err)
	}

	{
		bin, err := marshalLogs(logs)
		if err != nil {
			return nil, fmt.Errorf("marshal: %w", err)
		}
		if err := i.opts.Store.Write(ctx, logsCacheKey(q), bin); err != nil {
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
	chunks := chunkBlockRange(from, to, i.opts.MaxBlockRange)

	start := time.Now()

	i.opts.LogFunc("Starting backfill", "from", from, "to", to, "chunks", len(chunks))

	for _, ch := range chunks {
		chunkStart := time.Now()

		logs, err := i.logsRange(ctx, ch.from, ch.to)
		if err != nil {
			return fmt.Errorf("get logs: %w", err)
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		if err := i.opts.ProcessFunc(ctx, logs); err != nil {
			return fmt.Errorf("process logs: %w", err)
		}

		i.opts.LogFunc("Backfill chunk processed", "from", ch.from, "to", ch.to, "logs", len(logs), "duration", time.Since(chunkStart))
	}

	i.opts.LogFunc("Backfill complete", "from", from, "to", to, "duration", time.Since(start))

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

func logsCacheKey(q ethereum.FilterQuery) string {
	if q.BlockHash != nil || q.ToBlock == nil || q.FromBlock == nil {
		panic("logs cache key requires a range query")
	}

	var b []byte

	b = binary.LittleEndian.AppendUint64(b, uint64(len(q.Addresses)))
	for _, a := range q.Addresses {
		b = append(b, a[:]...)
	}
	b = binary.LittleEndian.AppendUint64(b, uint64(len(q.Topics)))
	for _, tt := range q.Topics {
		b = binary.LittleEndian.AppendUint64(b, uint64(len(tt)))
		for _, t := range tt {
			b = append(b, t[:]...)
		}
	}

	hash := sha256.Sum256(b)

	return fmt.Sprintf("logs-%d-%d-%s", q.FromBlock, q.ToBlock, hex.EncodeToString(hash[:]))
}
