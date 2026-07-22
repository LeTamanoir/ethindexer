package ethindexer

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/sync/errgroup"
)

const (
	checkpointBlobName       = "checkpoint.gz"
	checkpointStagedBlobName = "checkpoint.staged.gz"
)

// Indexer indexes Ethereum logs from a finalized block onward, handling reorgs and checkpointing.
type Indexer struct {
	// Client provides access to Ethereum logs and block headers.
	Client ChainReader

	// DataDir stores checkpoints and cached log batches.
	DataDir string

	// FromBlock is the first block to index.
	FromBlock uint64

	// Filter specifies which logs the indexer fetches.
	Filter Filter

	// InitFunc optionally initializes application state on a fresh start. It
	// receives Client and a LogsRangeFunc that caches block-range queries in
	// DataDir.
	InitFunc func(context.Context, ChainReader, LogsRangeFunc) error

	// ProcessFunc applies matching logs in block order.
	ProcessFunc func(context.Context, []types.Log) error

	// SnapshotFunc returns the current application state.
	SnapshotFunc func(context.Context) ([]byte, error)

	// RestoreFunc restores previously captured application state.
	RestoreFunc func(context.Context, []byte) error

	// LogFunc receives indexer log events.
	LogFunc func(msg string, args ...any)

	// MaxBlockRange is the maximum block span per backfill request.
	MaxBlockRange uint64

	// FinalityDepth is the block depth considered finalized.
	FinalityDepth uint64

	// CheckpointInterval is the minimum number of blocks between staged checkpoints.
	CheckpointInterval uint64

	// MaxConcurrency bounds concurrent header fetches.
	MaxConcurrency int

	head   *blockRef // head of the last indexed block
	staged *blockRef // head of the staged checkpoint

	lastStagedNum uint64 // block number of the most recent staged checkpoint
}

func (i *Indexer) applyDefaults() {
	if i.LogFunc == nil {
		i.LogFunc = func(string, ...any) {}
	}
	if i.MaxBlockRange == 0 {
		i.MaxBlockRange = 10_000
	}
	if i.FinalityDepth == 0 {
		i.FinalityDepth = 64
	}
	if i.CheckpointInterval == 0 {
		i.CheckpointInterval = 10_000
	}
	if i.MaxConcurrency == 0 {
		i.MaxConcurrency = 16
	}
}

// Sync restores state and catches up to the current finalized head.
func (i *Indexer) Sync(ctx context.Context) error {
	if i.head != nil {
		return errors.New("indexer already synced")
	}
	if i.Client == nil {
		return errors.New("nil client")
	}
	if i.DataDir == "" {
		return errors.New("empty data directory")
	}
	if i.ProcessFunc == nil || i.SnapshotFunc == nil || i.RestoreFunc == nil {
		return errors.New("nil process, snapshot, or restore function")
	}

	i.applyDefaults()

	start := time.Now()

	i.LogFunc("Syncing indexer",
		"finality_depth", i.FinalityDepth,
		"checkpoint_interval", i.CheckpointInterval,
		"max_block_range", i.MaxBlockRange,
		"max_concurrent", i.MaxConcurrency)

	restored, err := i.restoreFinalized(ctx)
	if err != nil {
		return err
	}

	if !restored {
		if i.InitFunc != nil {
			if err := i.InitFunc(ctx, i.Client, i.logsRange); err != nil {
				return fmt.Errorf("init: %w", err)
			}
		}
	}

	if err := i.syncFinalized(ctx); err != nil {
		return err
	}

	i.LogFunc("Indexer synced", "head", i.head.number, "duration", time.Since(start))

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
		i.LogFunc("Ignoring older head", "current", idxNum, "received", headNum)
		return nil
	}

	// same-height heads are either duplicates or reorgs.
	if idxNum == headNum {
		if h.Hash() == i.head.hash {
			i.LogFunc("Ignoring duplicate head", "head", idxNum)
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
	final, err := i.Client.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		return err
	}

	from := i.FromBlock
	if i.head != nil {
		from = i.head.number + 1
	}
	to := final.Number.Uint64()

	if from > to {
		i.LogFunc("No backfill required", "head", i.head.number, "finalized", to)

		return nil
	}

	if err := i.backfillFinalized(ctx, from, to); err != nil {
		return fmt.Errorf("backfill: %w", err)
	}

	i.head = &blockRef{number: to, hash: final.Hash()}

	if err := i.stageCheckpoint(ctx); err != nil {
		return fmt.Errorf("stage checkpoint: %w", err)
	}
	if err := i.promoteCheckpoint(); err != nil {
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

	i.LogFunc("Fetched headers", "from", from, "to", to, "count", len(heads), "duration", time.Since(start))

	for _, h := range heads {
		if err := i.Process(ctx, h); err != nil {
			return err
		}
	}

	i.LogFunc("Backfill unfinalized complete", "from", from, "to", to, "duration", time.Since(start))

	return nil
}

// handleReorg restores the finalized checkpoint and reprocesses the divergent head.
func (i *Indexer) handleReorg(ctx context.Context, h *types.Header) error {
	if i.head.number == h.Number.Uint64() {
		i.LogFunc("Reorg detected at current head", "head", i.head.number, "current_hash", i.head.hash, "received_hash", h.Hash())
	} else {
		i.LogFunc("Reorg detected", "head", i.head.number, "expected_parent", i.head.hash, "got_parent", h.ParentHash)
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

	bin, err := readBlob(i.DataDir, checkpointBlobName)
	if err != nil {
		return false, fmt.Errorf("read checkpoint: %w", err)
	}
	if len(bin) == 0 {
		return false, nil
	}

	cp, err := unmarshalCheckpoint(bin)
	if err != nil {
		return false, fmt.Errorf("unmarshal: %w", err)
	}

	if err := i.RestoreFunc(ctx, cp.state); err != nil {
		return false, fmt.Errorf("restore: %w", err)
	}

	h := cp.head // prevent escaping the whole checkpoint to the heap
	i.head = &h
	i.lastStagedNum = h.number

	i.LogFunc("Restored checkpoint", "head", h.number, "duration", time.Since(start))

	return true, nil
}

// processHead handles a new header and assumes it is strictly consecutive to idx.head.
func (i *Indexer) processHead(ctx context.Context, h *types.Header) error {
	logs, err := i.Client.FilterLogs(ctx, i.Filter.blockQuery(h.Hash()))
	if err != nil {
		return fmt.Errorf("filter logs: %w", err)
	}

	if err := i.ProcessFunc(ctx, logs); err != nil {
		return fmt.Errorf("process logs: %w", err)
	}

	i.head = &blockRef{number: h.Number.Uint64(), hash: h.Hash()}

	// save a checkpoint if none is staged and enough blocks have passed
	if i.staged == nil {
		if i.head.number >= i.lastStagedNum+i.CheckpointInterval {
			return i.stageCheckpoint(ctx)
		}
		return nil
	}

	// promote staged to finalized once the head has aged past finalityDepth.
	if i.head.number >= i.staged.number+i.FinalityDepth {
		return i.promoteCheckpoint()
	}

	return nil
}

// promoteCheckpoint moves the staged checkpoint to finalized.
func (i *Indexer) promoteCheckpoint() error {
	start := time.Now()

	if err := os.Rename(filepath.Join(i.DataDir, checkpointStagedBlobName), filepath.Join(i.DataDir, checkpointBlobName)); err != nil {
		return fmt.Errorf("move: %w", err)
	}

	i.LogFunc("Promoted checkpoint", "head", i.staged.number, "duration", time.Since(start))

	i.staged = nil

	return nil
}

// stageCheckpoint saves a staged checkpoint.
func (i *Indexer) stageCheckpoint(ctx context.Context) error {
	start := time.Now()

	state, err := i.SnapshotFunc(ctx)
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}

	h := *i.head
	cp := checkpoint{h, state}

	bin, err := marshalCheckpoint(cp)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	if err := writeBlob(i.DataDir, checkpointStagedBlobName, bin); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}

	i.LogFunc("Staged checkpoint", "head", cp.head.number, "duration", time.Since(start))

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

	eg.SetLimit(i.MaxConcurrency)

	for j := range total {
		eg.Go(func() error {
			h, e := i.Client.HeaderByNumber(ctx, big.NewInt(int64(from+j)))
			heads[j] = h
			return e
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return heads, nil
}

func (i *Indexer) logsRange(ctx context.Context, filter Filter, from, to uint64) ([]types.Log, error) {
	q := filter.rangeQuery(from, to)
	key := logsBlobName(q)

	bin, err := readBlob(i.DataDir, key)
	if err != nil {
		return nil, fmt.Errorf("read cache: %w", err)
	}
	if len(bin) > 0 {
		logs, err := unmarshalLogs(bin)
		if err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}
		return logs, nil
	}

	logs, err := i.Client.FilterLogs(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("filter logs: %w", err)
	}

	bin, err = marshalLogs(logs)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	if err := writeBlob(i.DataDir, key, bin); err != nil {
		return nil, fmt.Errorf("write cache: %w", err)
	}

	return logs, nil
}

// backfillFinalized fetches and processes logs over [from, to] in chunks.
//
// The range is assumed to be finalized, allowing logs to be queried by block
// range with FilterLogs instead of by block hash. This is more efficient but
// does not provide reorg safety.
func (i *Indexer) backfillFinalized(ctx context.Context, from, to uint64) error {
	chunks := chunkBlockRange(from, to, i.MaxBlockRange)

	start := time.Now()

	i.LogFunc("Starting backfill", "from", from, "to", to, "chunks", len(chunks))

	for _, ch := range chunks {
		chunkStart := time.Now()

		logs, err := i.logsRange(ctx, i.Filter, ch.from, ch.to)
		if err != nil {
			return fmt.Errorf("get logs: %w", err)
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		if err := i.ProcessFunc(ctx, logs); err != nil {
			return fmt.Errorf("process logs: %w", err)
		}

		i.LogFunc("Backfill chunk processed", "from", ch.from, "to", ch.to, "logs", len(logs), "duration", time.Since(chunkStart))
	}

	i.LogFunc("Backfill complete", "from", from, "to", to, "duration", time.Since(start))

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

func logsBlobName(q ethereum.FilterQuery) string {
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

	return fmt.Sprintf("logs-%d-%d-%s.gz", q.FromBlock, q.ToBlock, hex.EncodeToString(hash[:]))
}
