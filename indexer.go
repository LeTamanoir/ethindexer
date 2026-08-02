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

// State processes Ethereum logs into checkpointed application state.
type State interface {
	Process(context.Context, []types.Log) error
}

// Indexer indexes Ethereum logs into State, handling backfills, reorgs, and
// gob-encoded checkpoints. The finalized checkpoint is the durable restart
// point; a newer staged checkpoint is promoted once it has aged past
// FinalityDepth.
//
//	Start block          Finalized checkpoint        Staged      Latest
//	     |                          |                    |           |
//	     S --------[...]----------- F ------------------ S --------- L
//	                                  <- FinalityDepth ->
type Indexer[S State] struct {
	// Client provides access to Ethereum logs and block headers.
	Client ChainReader

	// DataDir stores checkpoints and cached log batches.
	DataDir string

	// FromBlock is the first block to index.
	FromBlock uint64

	// Filter specifies which logs the indexer fetches.
	Filter Filter

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

	// State receives matching logs and is persisted in checkpoints with
	// encoding/gob. It must be a pointer so checkpoints can restore it in place.
	State S

	head   *blockRef // head of the last indexed block
	staged *blockRef // head of the staged checkpoint

	lastStagedNum uint64 // block number of the most recent staged checkpoint
}

func (i *Indexer[S]) defaults() {
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

// HasCheckpoint reports whether a finalized checkpoint exists in DataDir.
func (i *Indexer[S]) HasCheckpoint() (bool, error) {
	if i.DataDir == "" {
		return false, errors.New("empty data directory")
	}

	_, err := os.Stat(filepath.Join(i.DataDir, checkpointBlobName))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("stat %s: %w", checkpointBlobName, err)
}

// ClearCheckpoint removes finalized and staged checkpoints from DataDir while
// preserving cached log ranges.
func (i *Indexer[S]) ClearCheckpoint() error {
	if i.DataDir == "" {
		return errors.New("empty data directory")
	}

	for _, name := range [...]string{checkpointBlobName, checkpointStagedBlobName} {
		if err := os.Remove(filepath.Join(i.DataDir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove %s: %w", name, err)
		}
	}

	return nil
}

func (i *Indexer[S]) init() error {
	i.defaults()

	if i.Client == nil {
		return errors.New("nil client")
	}
	if i.DataDir == "" {
		return errors.New("empty data directory")
	}

	i.LogFunc("Initializing indexer",
		"finality_depth", i.FinalityDepth,
		"checkpoint_interval", i.CheckpointInterval,
		"max_block_range", i.MaxBlockRange,
		"max_concurrent", i.MaxConcurrency)

	_, err := i.restore()
	return err
}

// backfillGap fills the missing blocks through h. The finalized prefix is
// queried by block range and checkpointed; only the suffix after the finalized
// head is queried block-by-block by hash.
func (i *Indexer[S]) backfillGap(ctx context.Context, h *types.Header) error {
	from := i.FromBlock
	if i.head != nil {
		from = i.head.Number + 1
	}
	to := h.Number.Uint64()
	if from > to {
		return fmt.Errorf("target block %d before next block %d", to, from)
	}

	final, err := i.Client.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		return fmt.Errorf("finalized header: %w", err)
	}
	finalizedNumber := final.Number.Uint64()

	// A finalized range query skips the intermediate headers, so verify that
	// it continues from the current head before processing any logs.
	if i.head != nil && from <= finalizedNumber {
		first, err := i.Client.HeaderByNumber(ctx, new(big.Int).SetUint64(from))
		if err != nil {
			return fmt.Errorf("first finalized gap header: %w", err)
		}
		if first.ParentHash != i.head.Hash {
			return i.reorg(ctx, h)
		}
	}

	switch {
	// The entire gap is newer than the finalized head.
	case finalizedNumber < from:
		if i.head == nil {
			return fmt.Errorf("from block %d is newer than finalized block %d", from, finalizedNumber)
		}
		return i.backfillUnsafe(ctx, from, to)

	// The entire gap is finalized.
	case to <= finalizedNumber:
		canonical := final
		if to < finalizedNumber {
			canonical, err = i.Client.HeaderByNumber(ctx, new(big.Int).SetUint64(to))
			if err != nil {
				return fmt.Errorf("target header: %w", err)
			}
		}
		if canonical.Hash() != h.Hash() {
			return fmt.Errorf("target header %d is not canonical", to)
		}
		return i.backfillFinalized(ctx, from, canonical)

	// The gap crosses the finalized head.
	default:
		if err := i.backfillFinalized(ctx, from, final); err != nil {
			return err
		}
		return i.backfillUnsafe(ctx, finalizedNumber+1, to)
	}
}

// Process ingests a target head, restoring State on its first call. A nil head
// selects the node's latest head.
func (i *Indexer[S]) Process(ctx context.Context, h *types.Header) error {
	if i.head == nil {
		if err := i.init(); err != nil {
			return err
		}
	}

	if h == nil {
		var err error
		h, err = i.Client.HeaderByNumber(ctx, nil)
		if err != nil {
			return fmt.Errorf("latest header: %w", err)
		}
	}

	if i.head == nil {
		return i.backfillGap(ctx, h)
	}

	idxNum := i.head.Number
	headNum := h.Number.Uint64()

	if headNum < idxNum {
		i.LogFunc("Ignoring older head", "current", idxNum, "received", headNum)
		return nil
	}

	// same-height heads are either duplicates or reorgs.
	if idxNum == headNum {
		if h.Hash() == i.head.Hash {
			i.LogFunc("Ignoring duplicate head", "head", idxNum)
			return nil
		}

		return i.reorg(ctx, h)
	}

	// ensure contiguous block processing.
	if headNum != idxNum+1 {
		return i.backfillGap(ctx, h)
	}

	// ensure chain continuity.
	if i.head.Hash != h.ParentHash {
		return i.reorg(ctx, h)
	}

	return i.process(ctx, h)
}

// backfillUnsafe fetches and processes the missing headers in [from, to].
//
// The range is assumed to be after the finalized head, so each header is fetched
// individually and logs are queried by block hash to preserve reorg safety.
func (i *Indexer[S]) backfillUnsafe(ctx context.Context, from, to uint64) error {
	start := time.Now()

	heads, err := i.headers(ctx, from, to)
	if err != nil {
		return fmt.Errorf("headers range: %w", err)
	}

	i.LogFunc("Fetched headers", "from", from, "to", to, "count", len(heads), "duration", time.Since(start))

	for _, h := range heads {
		if err := i.Process(ctx, h); err != nil {
			return err
		}
	}

	i.LogFunc("Backfill unsafe complete", "from", from, "to", to, "duration", time.Since(start))

	return nil
}

// reorg restores the finalized checkpoint and reprocesses the divergent head.
func (i *Indexer[S]) reorg(ctx context.Context, h *types.Header) error {
	if i.head.Number == h.Number.Uint64() {
		i.LogFunc("Reorg detected at current head", "head", i.head.Number, "current_hash", i.head.Hash, "received_hash", h.Hash())
	} else {
		i.LogFunc("Reorg detected", "head", i.head.Number, "expected_parent", i.head.Hash, "got_parent", h.ParentHash)
	}

	i.head = nil
	i.staged = nil
	i.lastStagedNum = 0

	ok, err := i.restore()
	if err != nil {
		return fmt.Errorf("restore finalized: %w", err)
	}
	if !ok {
		return errors.New("reorg detected but no finalized checkpoint found")
	}

	return i.Process(ctx, h)
}

// restore restores State from the finalized checkpoint and records its head.
func (i *Indexer[S]) restore() (bool, error) {
	start := time.Now()

	cp := checkpoint[S]{State: i.State}
	ok, err := readBlob(i.DataDir, checkpointBlobName, &cp)
	if err != nil {
		return false, fmt.Errorf("read checkpoint: %w", err)
	}
	if !ok {
		return false, nil
	}

	i.State = cp.State
	i.head = &cp.Head
	i.lastStagedNum = cp.Head.Number

	i.LogFunc("Restored checkpoint", "head", cp.Head.Number, "duration", time.Since(start))

	return true, nil
}

// process handles a new header that is strictly consecutive to i.head.
func (i *Indexer[S]) process(ctx context.Context, h *types.Header) error {
	logs, err := i.Client.FilterLogs(ctx, i.Filter.blockQuery(h.Hash()))
	if err != nil {
		return fmt.Errorf("filter logs: %w", err)
	}

	if err := i.State.Process(ctx, logs); err != nil {
		return fmt.Errorf("process logs: %w", err)
	}

	i.head = &blockRef{Number: h.Number.Uint64(), Hash: h.Hash()}

	// save a checkpoint if none is staged and enough blocks have passed
	if i.staged == nil {
		if i.head.Number >= i.lastStagedNum+i.CheckpointInterval {
			return i.stage()
		}
		return nil
	}

	// promote staged to finalized once the head has aged past finalityDepth.
	if i.head.Number >= i.staged.Number+i.FinalityDepth {
		return i.promote()
	}

	return nil
}

// promote moves the staged checkpoint to finalized.
func (i *Indexer[S]) promote() error {
	start := time.Now()

	if err := os.Rename(filepath.Join(i.DataDir, checkpointStagedBlobName), filepath.Join(i.DataDir, checkpointBlobName)); err != nil {
		return fmt.Errorf("move: %w", err)
	}

	i.LogFunc("Promoted checkpoint", "head", i.staged.Number, "duration", time.Since(start))

	i.staged = nil

	return nil
}

// stage saves State and the current head as a staged checkpoint.
func (i *Indexer[S]) stage() error {
	start := time.Now()

	cp := checkpoint[S]{Head: *i.head, State: i.State}
	if err := writeBlob(i.DataDir, checkpointStagedBlobName, cp); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}

	i.LogFunc("Staged checkpoint", "head", cp.Head.Number, "duration", time.Since(start))

	i.staged = &cp.Head
	i.lastStagedNum = cp.Head.Number

	return nil
}

// headers fetches [from, to] concurrently, preserving order.
func (i *Indexer[S]) headers(ctx context.Context, from, to uint64) ([]*types.Header, error) {
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

// CachedFilterLogs returns logs matching filter in the inclusive block range,
// using the cache in DataDir when available.
func (i *Indexer[S]) CachedFilterLogs(ctx context.Context, f Filter, r BlockRange) ([]types.Log, error) {
	q := f.rangeQuery(r)
	key := logsKey(q)

	var logs logBatch
	ok, err := readBlob(i.DataDir, key, &logs)
	if err != nil {
		return nil, fmt.Errorf("read cache: %w", err)
	}
	if ok {
		return logs, nil
	}

	fetched, err := i.Client.FilterLogs(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("filter logs: %w", err)
	}

	if err := writeBlob(i.DataDir, key, logBatch(fetched)); err != nil {
		return nil, fmt.Errorf("write cache: %w", err)
	}

	return fetched, nil
}

// backfillFinalized fetches and processes logs over [from, to.Number] in
// chunks, then records to as the finalized checkpoint.
//
// The range is assumed to be finalized, allowing logs to be queried by block
// range with FilterLogs instead of by block hash. This is more efficient but
// does not provide reorg safety.
func (i *Indexer[S]) backfillFinalized(ctx context.Context, from uint64, to *types.Header) error {
	toNumber := to.Number.Uint64()
	chunks := ChunkBlockRange(from, toNumber, i.MaxBlockRange)

	start := time.Now()

	i.LogFunc("Starting backfill", "from", from, "to", toNumber, "chunks", len(chunks))

	for _, ch := range chunks {
		chunkStart := time.Now()

		logs, err := i.CachedFilterLogs(ctx, i.Filter, ch)
		if err != nil {
			return fmt.Errorf("get logs: %w", err)
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		if err := i.State.Process(ctx, logs); err != nil {
			return fmt.Errorf("process logs: %w", err)
		}

		i.LogFunc("Backfill chunk processed", "from", ch.From, "to", ch.To, "logs", len(logs), "duration", time.Since(chunkStart))
	}

	i.LogFunc("Backfill complete", "from", from, "to", toNumber, "duration", time.Since(start))

	i.head = &blockRef{Number: toNumber, Hash: to.Hash()}

	if err := i.stage(); err != nil {
		return fmt.Errorf("stage checkpoint: %w", err)
	}
	if err := i.promote(); err != nil {
		return fmt.Errorf("promote checkpoint: %w", err)
	}

	return nil
}

// BlockRange is an inclusive block range.
type BlockRange struct {
	From uint64
	To   uint64
}

// ChunkBlockRange splits the inclusive block range [from, to] into ranges
// containing at most size blocks.
func ChunkBlockRange(from, to, size uint64) []BlockRange {
	if size == 0 {
		panic("invalid block range size: 0")
	}
	var chunks []BlockRange
	for start := from; start <= to; {
		end := to
		if size-1 < to-start {
			end = start + size - 1
		}
		chunks = append(chunks, BlockRange{From: start, To: end})
		if end == to {
			break
		}
		start = end + 1
	}
	return chunks
}

func logsKey(q ethereum.FilterQuery) string {
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

	return fmt.Sprintf("logs-v%d-%d-%d-%s.gz", logBatchEncodingVersion, q.FromBlock, q.ToBlock, hex.EncodeToString(hash[:]))
}
