package ethindex

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"

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
	l *slog.Logger

	// Configs
	maxBlockRange uint64
	finalityDepth uint64

	// State
	dangling BlockRef
	head     BlockRef
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
	}

	idx.l.Info("starting indexer", "from_block", cfg.Filter.FromBlock, "finality_depth", cfg.FinalityDepth, "max_block_range", cfg.MaxBlockRange)

	cp, ok, err := loadFinalized(ctx, idx.s)
	if err != nil {
		return nil, fmt.Errorf("load finalized: %w", err)
	}
	if ok {
		idx.l.Info("restoring from finalized checkpoint", "head", cp.Head.Number)

		if err := idx.h.Restore(ctx, cp.State); err != nil {
			return nil, fmt.Errorf("restore finalized: %w", err)
		}

		idx.head = cp.Head
	}

	final, err := idx.c.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		return nil, err
	}

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

		state, err := idx.h.Snapshot(ctx)
		if err != nil {
			return nil, fmt.Errorf("snapshot: %w", err)
		}

		if err := saveFinalized(ctx, idx.s, checkpoint{idx.head, state}); err != nil {
			return nil, fmt.Errorf("save finalized: %w", err)
		}

		idx.l.Info("saved finalized checkpoint", "head", idx.head.Number)
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
		idx.l.Info("filling missing heads", "from", inum+1, "to", hnum)

		heads, err := headersRange(ctx, idx.c, inum+1, hnum)
		if err != nil {
			return fmt.Errorf("headers range: %w", err)
		}

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

		idx.head = BlockRef{}
		idx.dangling = BlockRef{}

		cp, ok, err := loadFinalized(ctx, idx.s)
		if err != nil {
			return fmt.Errorf("load finalized: %w", err)
		}
		if !ok {
			return errors.New("reorg detected but no finalized checkpoint found")
		}

		idx.l.Info("restoring from finalized checkpoint", "head", cp.Head.Number)

		if err := idx.h.Restore(ctx, cp.State); err != nil {
			return fmt.Errorf("restore: %w", err)
		}

		idx.head = cp.Head

		return idx.Process(ctx, h)
	}

	logs, err := idx.c.FilterLogs(ctx, newFilterQuery(idx.f, hnum, hnum))
	if err != nil {
		return fmt.Errorf("get logs: %w", err)
	}

	if err := idx.h.Process(ctx, logs); err != nil {
		return fmt.Errorf("process logs: %w", err)
	}

	idx.head = BlockRef{Number: hnum, Hash: h.Hash()}

	idx.l.Debug("processed head", "number", hnum, "logs", len(logs))

	if idx.dangling == (BlockRef{}) {
		state, err := idx.h.Snapshot(ctx)
		if err != nil {
			return fmt.Errorf("snapshot: %w", err)
		}

		cp := checkpoint{Head: idx.head, State: state}
		if err := saveDangling(ctx, idx.s, cp); err != nil {
			return fmt.Errorf("save dangling: %w", err)
		}

		idx.l.Debug("saved dangling checkpoint", "head", idx.head.Number)

		idx.dangling = cp.Head
	}

	if idx.head.Number >= idx.dangling.Number+idx.finalityDepth {
		if err := promoteDangling(ctx, idx.s); err != nil {
			return fmt.Errorf("promote dangling: %w", err)
		}

		idx.l.Info("promoted dangling checkpoint to finalized", "head", idx.dangling.Number)

		idx.dangling = BlockRef{}
	}

	return nil
}

func (idx *Indexer) backfill(ctx context.Context, from, to uint64) error {
	chunks := chunkBlockRange(from, to, idx.maxBlockRange)

	idx.l.Info("backfilling", "from", from, "to", to, "chunks", len(chunks))

	for _, ch := range chunks {
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

		idx.l.Debug("backfill chunk processed", "from", ch.from, "to", ch.to, "logs", len(logs))
	}

	idx.l.Info("backfill complete", "from", from, "to", to)

	return nil
}
