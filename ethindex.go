package ethindex

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"slices"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/sync/errgroup"
)

var ErrReorg = errors.New("chain reorg detected")

type Filter struct {
	FromBlock uint64
	Addresses []common.Address
	Topics    [][]common.Hash
}

type Handler interface {
	Snapshot(context.Context) ([]byte, error)
	Restore(context.Context, []byte) error
	Filter() Filter
	Process(context.Context, []types.Log) error
}

type checkpoint struct {
	BlockNumber uint64
	BlockHash   common.Hash
	State       []byte
}

type CheckpointStore interface {
	Load(common.Hash) (*checkpoint, error)
	Save(common.Hash, *checkpoint) error
	Delete(common.Hash) error
}

type LogStore interface {
	Load(ethereum.FilterQuery) ([]types.Log, error)
	Save(ethereum.FilterQuery, []types.Log) error
}

type Client interface {
	ethereum.LogFilterer
	ethereum.ChainReader
}

type blockHeader struct {
	Number uint64      `json:"number"`
	Hash   common.Hash `json:"hash"`
}

type indexerConfig struct {
	retryFunc          func(err error, attempt int) bool
	newHeadsBuffer     int
	maxConcurrentCalls int
	maxBlockRange      uint64
	checkpointInterval uint64
}

type indexerState struct {
	Finalized *blockHeader `json:"finalized"`
	Dangling  *blockHeader `json:"dangling"`
}

type Indexer struct {
	cfg    *indexerConfig
	logger *slog.Logger

	httpC Client
	wsC   LiveClient

	hMu sync.RWMutex
	h   Handler

	// cm checkpointManager
	cs CheckpointStore
	ls LogStore

	state *indexerState

	head   *blockHeader
	isLive bool

	stopCh   chan struct{}
	stopOnce sync.Once
}

// Run starts the indexer and blocks until the context is canceled.
// It automatically retries transient errors via the configured RetryFunc,
// returning only on a fatal error, context cancellation, or a call to Stop.
func (idx *Indexer) Run(ctx context.Context) error {
	for attempt := 1; ; attempt++ {
		err := idx.run(ctx)

		// if coming from [Indexer.Stop]
		if err == nil {
			return nil
		}
		// Retry on reorgs
		if errors.Is(err, ErrReorg) {
			continue
		}

		retryable := idx.cfg.retryFunc != nil && idx.cfg.retryFunc(err, attempt)

		idx.logger.Error("runtime error", "err", err, "retryable", retryable)

		if !retryable {
			return err
		}
	}
}

// Stop gracefully shuts down the indexer.
func (idx *Indexer) Stop() {
	idx.stopOnce.Do(func() { close(idx.stopCh) })
}

func (idx *Indexer) run(ctx context.Context) error {
	idx.logger.Info("starting indexer")

	if err := idx.restore(ctx); err != nil {
		return err
	}

	if err := idx.backfill(ctx); err != nil {
		return err
	}

	return idx.live(ctx)

}

func (idx *Indexer) live(ctx context.Context) error {
	ch := make(chan *types.Header, idx.cfg.newHeadsBuffer)
	sub, err := idx.wsC.SubscribeNewHead(ctx, ch)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	idx.logger.Info("subscribed to new heads")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return err
		case <-idx.stopCh:
			return nil
		case h := <-ch:
			if err := idx.processHead(ctx, h); err != nil {
				return err
			}
		}
	}
}

func (idx *Indexer) processHead(ctx context.Context, h *types.Header) error {
	ch := idx.head

	// Only check for reorgs when appending the strictly sequential next block.
	// This safely bypasses the check during the brief transition from the
	// backfilled state to the live network head.
	if ch.Number.Uint64() == h.Number.Uint64()-1 &&
		ch.Hash() != h.ParentHash {
		idx.logger.Warn("reorg detected", "old", ch.Number, "new", h.Number)

		return ErrReorg
	}

	if err := idx.processRange(ctx, ch.Number.Uint64()+1, h.Number.Uint64()); err != nil {
		return err
	}

	idx.head = h

	// if num%idx.cfg.checkpointInterval == 0 {
	// 	if err := idx.checkpoint(ctx); err != nil {
	// 		return err
	// 	}
	// 	if err := idx.prune(ctx); err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

func (idx *Indexer) backfill(ctx context.Context) error {
	final, err := idx.httpC.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		return err
	}

	from := idx.h.Filter().FromBlock
	if idx.head != nil {
		from = idx.head.Number.Uint64() + 1
	}
	to := final.Number.Uint64()

	if from <= to {
		idx.logger.Info("starting backfill", "from", from, "to", to)

		if err := idx.processRange(ctx, from, to); err != nil {
			return err
		}
	} else {
		idx.logger.Info("backfill skipped, already up to date with finalized block")
	}

	idx.head = final

	return nil
}

func (idx *Indexer) processRange(ctx context.Context, from, to uint64) error {
	if from > to {
		return fmt.Errorf("invalid block range: from (%d) > to (%d)", from, to)
	}

	totalBlocks := to - from + 1

	for start := from; start <= to; start += idx.cfg.maxBlockRange {
		end := min(start+idx.cfg.maxBlockRange-1, to)

		fetchStart := time.Now()
		logs, err := idx.fetchLogs(ctx, start, end)
		if err != nil {
			return fmt.Errorf("fetch logs for blocks %d-%d: %w", start, end, err)
		}
		fetchDur := time.Since(fetchStart)

		if err := ctx.Err(); err != nil {
			return err
		}

		processStart := time.Now()
		if err := idx.h.Process(ctx, logs); err != nil {
			return err
		}
		processDur := time.Since(processStart)

		processed := end - from + 1
		progress := float64(processed) / float64(totalBlocks) * 100.0

		idx.logger.Debug("processed chunk",
			"progress", fmt.Sprintf("%.2f%%", progress),
			"from", start,
			"to", end,
			"fetch_dur", fetchDur,
			"process_dur", processDur,
		)
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

func (idx *Indexer) fetchLogs(ctx context.Context, from, to uint64) ([]types.Log, error) {
	q := idx.filterQuery(from, to)

	if idx.isLive {
		return idx.httpC.FilterLogs(ctx, q)
	}

	cached, err := idx.ls.Load(q)
	if err != nil {
		return nil, err
	}
	if len(cached) > 0 {
		return cached, nil
	}

	logs, err := idx.httpC.FilterLogs(ctx, q)
	if err != nil {
		return nil, err
	}
	if err := idx.ls.Save(q, logs); err != nil {
		return nil, err
	}

	return logs, nil
}

func (idx *Indexer) restore(ctx context.Context) error {
	cp, err := idx.cs.Load(idx.state.Finalized.Hash)
	if err != nil {
		return err
	}
	if cp == nil {
		idx.logger.Info("no checkpoint to restore")
		return nil
	}

	if err := idx.h.Restore(ctx, cp.State); err != nil {
		return fmt.Errorf("restore checkpoint %s: %w", cp.Header.Hash, err)
	}

	idx.head = cp.Header

	idx.logger.Info("Restored checkpoint",
		"number", cp.Header.Number,
		"hash", cp.Header.Hash)

	return nil
}

func (idx *Indexer) promote(ctx context.Context) error {
	var dangling, finalized *types.Header

	var eg errgroup.Group

	eg.Go(func() (err error) {
		dangling, err = idx.httpC.HeaderByNumber(ctx, idx.state.Dangling.Number)
		return
	})
	eg.Go(func() (err error) {
		finalized, err = idx.httpC.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
		return
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	// Make sure dangling hash matches the node
	// otherwise it means there was a reorg,
	// so we discard the dangling checkpoint
	if idx.dangling.Hash() != dangling.Hash() {
		if err := idx.cs.Delete(idx.dangling.Hash()); err != nil {
			return err
		}

		idx.dangling = nil

		return nil
	}

	// If dangling is older than finalized, we promote
	if dangling.Number.Cmp(finalized.Number) > 0 {
		return nil
	}

	idx.finalized = idx.dangling

	// if dangling.Nonce

	// 1. Find latest finalized checkpoint
	{
		final, err := idx.finalizedHeader(ctx)
		if err != nil {
			return err
		}
		finalNum := final.Number.Uint64()

		var found bool

		for i := len(idx.checkpoints) - 1; i >= 0; i-- {
			finalHeader = idx.checkpoints[i]

			if finalHeader.Number < finalNum ||
				(finalHeader.Number == finalNum && finalHeader.Hash == final.Hash()) {
				found = true
				break
			}
		}

		if !found {
			return nil
		}
	}

	// 2. Promote checkpoint
	{
		var cp checkpoint
		ok, err := idx.cache.Load(checkpointKey(finalHeader), &cp)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("checkpoint from memory not found in store")
		}
		if err := idx.cache.Save(finalizedCheckpointKey(), cp); err != nil {
			return err
		}
		idx.logger.Debug("finalized checkpoint", "block", cp.Header.Number, "hash", cp.Header.Hash)
	}

	// 3. Prune old checkpoints
	{
		deletedCount := 0
		// Ensure the slice is compacted exactly once, even if we return an error early.
		defer func() {
			if deletedCount > 0 {
				idx.checkpoints = slices.Delete(idx.checkpoints, 0, deletedCount)
			}
		}()
		for deletedCount < len(idx.checkpoints) && idx.checkpoints[deletedCount].Number <= finalHeader.Number {
			err := idx.cache.Delete(checkpointKey(idx.checkpoints[deletedCount]))
			if err != nil {
				return err
			}
			deletedCount++
		}

		idx.logger.Debug("pruned old checkpoints", "up-to-block", finalHeader.Number, "count", deletedCount)
	}

	return nil
}

func (idx *Indexer) checkpoint(ctx context.Context) error {
	idx.hMu.RLock()
	state, err := idx.h.Snapshot(ctx)
	idx.hMu.RUnlock()
	if err != nil {
		return err
	}

	cp := checkpoint{Header: idx.head, State: state}
	h := cp.Header.Hash()

	if err := idx.cs.Save(h, cp); err != nil {
		return err
	}

	idx.dangling = h

	idx.logger.Info("Saved dangling checkpoint",
		"number", cp.Header.Number,
		"hash", cp.Header.Hash)

	return nil
}
