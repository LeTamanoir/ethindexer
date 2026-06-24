package ethindex

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

type BlockRef struct {
	Number uint64
	Hash   common.Hash
}

type Indexer struct {
	c Client
	h Handler
	f Filter
	s Store
	l Logger

	// Configs
	maxBlockRange uint64
	finalityDepth uint64
	maxConcurrent int

	// State
	head        BlockRef
	dangling    BlockRef   // dangling tracks the latest dangling checkpoint saved.
	pendingSave chan error // pendingSave tracks the in-flight async dangling checkpoint save.
}

func (idx *Indexer) waitPendingSave() error {
	ch := idx.pendingSave
	idx.pendingSave = nil
	if ch == nil {
		return nil
	}
	return <-ch
}

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

		maxBlockRange: cfg.MaxBlockRange,
		finalityDepth: cfg.FinalityDepth,
		maxConcurrent: cfg.MaxConcurrency,
	}

	idx.l.Info("starting indexer", "from_block", cfg.Filter.FromBlock, "finality_depth", cfg.FinalityDepth, "max_block_range", cfg.MaxBlockRange)

	cp, ok, err := loadCheckpoint(ctx, idx.s, finalized)
	if err != nil {
		return nil, fmt.Errorf("load finalized: %w", err)
	}
	if ok {
		start := time.Now()

		if err := idx.h.Restore(ctx, cp.State); err != nil {
			return nil, fmt.Errorf("restore finalized: %w", err)
		}

		idx.head = cp.Head

		idx.l.Info("restored from finalized checkpoint", "head", cp.Head.Number, "duration", time.Since(start))
	}

	start := time.Now()
	final, err := idx.c.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		return nil, err
	}
	idx.l.Debug("fetched finalized header", "number", final.Number.Uint64(), "duration", time.Since(start))

	from := idx.f.FromBlock
	if idx.head != (BlockRef{}) {
		from = idx.head.Number + 1
	}
	to := final.Number.Uint64()

	if from <= to {
		if err := idx.backfill(ctx, from, to); err != nil {
			return nil, fmt.Errorf("backfill: %w", err)
		}

		idx.head = BlockRef{Number: to, Hash: final.Hash()}

		snapStart := time.Now()
		state, err := idx.h.Snapshot(ctx)
		if err != nil {
			return nil, fmt.Errorf("snapshot: %w", err)
		}
		snapDuration := time.Since(snapStart)

		saveStart := time.Now()
		if err := saveCheckpoint(ctx, idx.s, finalized, checkpoint{idx.head, state}); err != nil {
			return nil, fmt.Errorf("save finalized: %w", err)
		}

		idx.l.Info("saved finalized checkpoint", "head", idx.head.Number, "snapshot", snapDuration, "save", time.Since(saveStart))
	} else {
		idx.l.Info("no backfill required", "head", idx.head.Number, "finalized", to)
	}

	idx.l.Info("indexer ready", "head", idx.head.Number)

	return idx, nil
}

// Process ingests a new head and handles reorgs.
func (idx *Indexer) Process(ctx context.Context, h *types.Header) error {
	inum := idx.head.Number
	hnum := h.Number.Uint64()

	if hnum <= inum {
		idx.l.Warn("ignoring old head", "current", inum, "received", hnum)

		return nil
	}

	// Enforce strict consecutive heads
	if hnum != inum+1 {
		start := time.Now()

		heads, err := headersRange(ctx, idx.c, inum+1, hnum, idx.maxConcurrent)
		if err != nil {
			return fmt.Errorf("headers range: %w", err)
		}

		idx.l.Info("filled missing heads", "from", inum+1, "to", hnum, "duration", time.Since(start))

		for _, h := range heads {
			if err := idx.Process(ctx, h); err != nil {
				return err
			}
		}

		return nil
	}

	// Handle reorg
	if idx.head.Hash != h.ParentHash {
		idx.l.Warn("reorg detected", "head", idx.head.Number, "expected_parent", idx.head.Hash, "got_parent", h.ParentHash)

		if err := idx.waitPendingSave(); err != nil {
			return fmt.Errorf("pending dangling save: %w", err)
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

		start := time.Now()

		if err := idx.h.Restore(ctx, cp.State); err != nil {
			return fmt.Errorf("restore: %w", err)
		}

		idx.head = cp.Head

		idx.l.Info("restored from finalized checkpoint", "head", cp.Head.Number, "duration", time.Since(start))

		return idx.Process(ctx, h)
	}

	start := time.Now()

	logs, err := idx.c.FilterLogs(ctx, newFilterQuery(idx.f, hnum, hnum))
	if err != nil {
		return fmt.Errorf("get logs: %w", err)
	}

	if err := idx.h.Process(ctx, logs); err != nil {
		return fmt.Errorf("process logs: %w", err)
	}

	idx.head = BlockRef{Number: hnum, Hash: h.Hash()}

	idx.l.Debug("processed head", "number", hnum, "logs", len(logs), "duration", time.Since(start))

	if idx.dangling == (BlockRef{}) {
		if err := idx.waitPendingSave(); err != nil {
			return fmt.Errorf("pending dangling save: %w", err)
		}

		start := time.Now()

		snapStart := time.Now()
		state, err := idx.h.Snapshot(ctx)
		if err != nil {
			return fmt.Errorf("snapshot: %w", err)
		}
		snapDuration := time.Since(snapStart)

		cp := checkpoint{Head: idx.head, State: state}

		saveCh := make(chan error, 1)
		idx.pendingSave = saveCh
		go func() {
			saveStart := time.Now()
			err := saveCheckpoint(ctx, idx.s, dangling, cp)
			if err != nil {
				idx.l.Error("async save dangling failed", "head", cp.Head.Number, "error", err, "save", time.Since(saveStart))
			} else {
				idx.l.Debug("saved dangling checkpoint", "head", cp.Head.Number, "snapshot", snapDuration, "save", time.Since(saveStart), "duration", time.Since(start))
			}
			saveCh <- err
		}()

		idx.dangling = cp.Head
	}

	if idx.dangling != (BlockRef{}) && idx.head.Number >= idx.dangling.Number+idx.finalityDepth {
		if err := idx.waitPendingSave(); err != nil {
			return fmt.Errorf("pending dangling save: %w", err)
		}

		start := time.Now()

		if err := idx.s.Move(ctx, string(dangling), string(finalized)); err != nil {
			return fmt.Errorf("promote dangling to finalized: %w", err)
		}

		idx.l.Info("promoted dangling checkpoint to finalized", "head", idx.dangling.Number, "duration", time.Since(start))

		idx.dangling = BlockRef{}
	}

	return nil
}

func (idx *Indexer) backfill(ctx context.Context, from, to uint64) error {
	chunks := chunkBlockRange(from, to, idx.maxBlockRange)

	idx.l.Info("backfilling", "from", from, "to", to, "chunks", len(chunks))

	start := time.Now()

	for _, ch := range chunks {
		chStart := time.Now()

		logs, err := cachedFilterLogs(ctx, idx.c, idx.s, newFilterQuery(idx.f, ch.from, ch.to))
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
