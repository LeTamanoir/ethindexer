package ethindex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

type Indexer struct {
	retryFunc          func(err error, attempt int) bool
	newHeadsBuffer     int
	maxConcurrentCalls int
	maxBlockRange      uint64
	checkpointInterval uint64
	logger             *slog.Logger

	http    Caller
	ws      Subscriber
	cache   Cache
	handler Handler

	head        *blockHeader
	isLive      bool
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
		// Retry on reorgs
		if errors.Is(err, ErrReorg) {
			continue
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
	idx.checkpoints = nil

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
	idx.isLive = true

	headCh := make(chan *types.Header, idx.newHeadsBuffer)
	sub, err := idx.ws.Subscribe(ctx, "eth", headCh, "newHeads")
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

			// Only check for reorgs when appending the strictly sequential next block.
			// This safely bypasses the check during the brief transition from the
			// backfilled state to the live network head.
			if idx.head.Number == num-1 && idx.head.Hash != h.ParentHash {
				idx.logger.Warn("reorg detected", "old", idx.head.Number, "new", num)

				return ErrReorg
			}

			if err := idx.processRange(ctx, idx.head.Number+1, num); err != nil {
				return err
			}

			idx.head = &blockHeader{Number: num, Hash: hash}

			if num%idx.checkpointInterval == 0 {
				if err := idx.checkpoint(ctx); err != nil {
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

func (idx *Indexer) finalizedHeader(ctx context.Context) (*types.Header, error) {
	var final *types.Header
	if err := idx.http.CallContext(ctx, &final, "eth_getBlockByNumber", "finalized", false); err != nil {
		return nil, err
	}
	return final, nil
}

func (idx *Indexer) backfill(ctx context.Context) error {
	final, err := idx.finalizedHeader(ctx)
	if err != nil {
		return err
	}

	from := idx.handler.Filter().FromBlock
	if idx.head != nil {
		from = idx.head.Number + 1
	}
	to := final.Number.Uint64()

	if from <= to {
		idx.logger.Info("starting backfill", "from", from, "to", to)

		if err := idx.processRange(ctx, from, to); err != nil {
			return fmt.Errorf("backfill blocks %d-%d: %w", from, to, err)
		}
	} else {
		idx.logger.Info("backfill skipped, already up to date with finalized block")
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
			return fmt.Errorf("fetch logs for blocks %d-%d: %w", start, end, err)
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		for i := range logs {
			if err := idx.handler.Process(ctx, logs[i].toGethLog()); err != nil {
				return fmt.Errorf("process log at block %d index %d tx %s: %w", logs[i].BlockNumber, logs[i].Index, logs[i].TxHash, err)
			}
		}
	}

	return nil
}

func buildFilterQuery(f Filter, from, to uint64) map[string]any {
	arg := map[string]any{}
	if len(f.Addresses) > 0 {
		arg["address"] = f.Addresses
	}
	if f.Topics != nil {
		arg["topics"] = f.Topics
	}
	arg["fromBlock"] = fmt.Sprintf("0x%x", from)
	arg["toBlock"] = fmt.Sprintf("0x%x", to)
	return arg
}

func (idx *Indexer) fetchLogs(ctx context.Context, from, to uint64) ([]log, error) {
	filter := idx.handler.Filter()

	query := buildFilterQuery(filter, from, to)

	if idx.isLive {
		var logs []log
		if err := idx.http.CallContext(ctx, &logs, "eth_getLogs", query); err != nil {
			return nil, err
		}
		return logs, nil
	}

	queryJSON, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}
	key := hexutil.Encode(crypto.Keccak256(queryJSON))

	var cached []log
	ok, err := idx.cache.Load(key, &cached)
	if err != nil {
		return nil, err
	}
	if ok {
		return cached, nil
	}

	var logs []log
	if err := idx.http.CallContext(ctx, &logs, "eth_getLogs", query); err != nil {
		return nil, err
	}
	if err := idx.cache.Save(key, logs); err != nil {
		return nil, err
	}

	return logs, nil
}

func (idx *Indexer) restore(ctx context.Context) error {
	var cp checkpoint
	ok, err := idx.cache.Load(finalizedCheckpointKey(), &cp)
	if err != nil {
		return err
	}
	if !ok {
		idx.logger.Info("no checkpoint to restore")
		return nil
	}

	if err := idx.handler.Restore(ctx, cp.State); err != nil {
		return fmt.Errorf("restore checkpoint %d: %w", cp.Header.Number, err)
	}

	idx.head = &cp.Header

	idx.logger.Info("restored checkpoint", "block", cp.Header.Number, "hash", cp.Header.Hash)

	return nil
}

func (idx *Indexer) prune(ctx context.Context) error {
	var finalHeader blockHeader

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
	state, err := idx.handler.Snapshot(ctx)
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
