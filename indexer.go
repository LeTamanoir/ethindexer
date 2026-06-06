package ethindex

import (
	"context"
	"encoding"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"slices"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

var ErrReorg = errors.New("chain reorg detected")

type Client interface {
	ethereum.ChainReader
	ethereum.LogFilterer
}

type Handler interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler

	Filter() Filter
	Process(ctx context.Context, log *types.Log) error
}

type Cache interface {
	Load(name string, out any) (ok bool, err error)
	Save(name string, v any) error
	Delete(name string) error
}

type blockHeader struct {
	Number uint64
	Hash   common.Hash
}

type checkpoint struct {
	Header blockHeader
	State  []byte
}

type Filter struct {
	FromBlock uint64
	Addresses []common.Address
	Topics    [][]common.Hash
}

type Indexer struct {
	retryFunc          func(err error, attempt int) bool
	newHeadsBuffer     int
	maxConcurrentCalls int
	maxBlockRange      uint64
	checkpointInterval uint64
	logger             *slog.Logger

	http    *ethclient.Client
	ws      *ethclient.Client
	cache   Cache
	handler Handler

	head   *blockHeader
	isLive bool

	checkpoints []blockHeader

	stopCh chan struct{}
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

		retryable := idx.retryFunc != nil && idx.retryFunc(err, attempt)

		idx.logger.Error("runtime error", "err", err, "retryable", retryable)

		if !retryable {
			return err
		}
	}
}

// Stop gracefully shuts down the indexer.
func (idx *Indexer) Stop() {
	close(idx.stopCh)
}

func (idx *Indexer) run(ctx context.Context) error {
	idx.isLive = false

	idx.logger.Info("starting indexer")

	if err := idx.restore(); err != nil {
		return err
	}

	if err := idx.backfill(ctx); err != nil {
		return err
	}

	return idx.live(ctx)

}

func (idx *Indexer) live(ctx context.Context) error {
	idx.isLive = true

	headCh := make(chan *types.Header, idx.newHeadsBuffer)

	sub, err := idx.ws.SubscribeNewHead(ctx, headCh)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	idx.logger.Info("subscribed to new heads")

	for {
		select {
		case h := <-headCh:
			num := h.Number.Uint64()
			hash := h.Hash()

			// check for reorgs only when we follow the head
			if idx.head.Number == num-1 && idx.head.Hash != h.ParentHash {
				idx.logger.Warn("reorg detected", "old", idx.head.Number, "new", num)

				return ErrReorg
			}

			if err := idx.processRange(ctx, idx.head.Number+1, num); err != nil {
				return err
			}

			idx.head = &blockHeader{Number: num, Hash: hash}

			if num%idx.checkpointInterval == 0 {
				if err := idx.checkpoint(); err != nil {
					return err
				}
				if err := idx.prune(ctx); err != nil {
					return err
				}
			}

		case err := <-sub.Err():
			return err

		case <-idx.stopCh:
			return nil
		}
	}
}

func (idx *Indexer) backfill(ctx context.Context) error {
	final, err := idx.http.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		return err
	}

	from := idx.handler.Filter().FromBlock
	if idx.head != nil {
		from = idx.head.Number + 1
	}
	to := final.Number.Uint64()

	idx.logger.Info("starting backfill", "from", from, "to", to)

	if err := idx.processRange(ctx, from, to); err != nil {
		return err
	}

	idx.head = &blockHeader{
		Number: to,
		Hash:   final.Hash(),
	}

	return nil
}

func (idx *Indexer) processRange(ctx context.Context, from, to uint64) error {
	if from > to {
		return fmt.Errorf("invalid block range: from (%d) > to (%d)", from, to)
	}

	totalBlocks := to - from + 1

	for start := from; start <= to; start += idx.maxBlockRange {
		end := min(start+idx.maxBlockRange-1, to)

		processed := end - from + 1
		progress := float64(processed) / float64(totalBlocks) * 100.0

		idx.logger.Debug("processing chunk", "progress", fmt.Sprintf("%.2f%%", progress), "from", start, "to", end)

		logs, err := idx.fetchLogs(ctx, start, end)
		if err != nil {
			return err
		}

		for i := range logs {
			if err := idx.handler.Process(ctx, &logs[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

func (idx *Indexer) fetchLogs(ctx context.Context, from, to uint64) ([]types.Log, error) {
	filter := idx.handler.Filter()

	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(from)),
		ToBlock:   big.NewInt(int64(to)),
		Addresses: filter.Addresses,
		Topics:    filter.Topics,
	}

	if idx.isLive {
		return idx.http.FilterLogs(ctx, query)
	}

	key := filterQueryKey(query)

	var cached []types.Log
	ok, err := idx.cache.Load(key, &cached)
	if err != nil {
		return nil, err
	}
	if ok {
		return cached, nil
	}

	logs, err := idx.http.FilterLogs(ctx, query)
	if err != nil {
		return nil, err
	}
	if err := idx.cache.Save(key, logs); err != nil {
		return nil, err
	}

	return logs, nil
}

func (idx *Indexer) restore() error {
	var cp checkpoint
	ok, err := idx.cache.Load(finalizedCheckpointKey(), &cp)
	if err != nil {
		return err
	}
	if !ok {
		idx.logger.Info("no checkpoint to restore")
		return nil
	}

	if err := idx.handler.UnmarshalBinary(cp.State); err != nil {
		return err
	}

	idx.head = &cp.Header

	idx.logger.Info("restored checkpoint", "block", cp.Header.Number, "hash", cp.Header.Hash)

	return nil
}

func (idx *Indexer) prune(ctx context.Context) error {
	var finalHeader blockHeader

	// 1. Find latest finalized checkpoint
	{
		final, err := idx.http.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
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
			return errors.New("no finalized checkpoint found")
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

func (idx *Indexer) checkpoint() error {
	state, err := idx.handler.MarshalBinary()
	if err != nil {
		return err
	}

	cp := checkpoint{
		Header: *idx.head,
		State:  state,
	}

	if err := idx.cache.Save(checkpointKey(cp.Header), cp); err != nil {
		return err
	}

	idx.checkpoints = append(idx.checkpoints, cp.Header)

	idx.logger.Info("saved checkpoint", "block", cp.Header.Number, "hash", cp.Header.Hash)

	return nil
}
