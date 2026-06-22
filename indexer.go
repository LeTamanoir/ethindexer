package ethindex

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

const (
	finalizedCP = "checkpoint-finalized"
	danglingCP  = "checkpoint-dangling"
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

	// Configs
	maxBlockRange uint64
	finalityDepth uint64

	// State
	dangling *BlockRef
	head     *BlockRef
}

func NewIndexer(ctx context.Context, c Client, h Handler, f Filter, s Store, cfg *Config) (*Indexer, error) {
	finalityDepth := uint64(64)
	maxBlockRange := uint64(10_000)
	var progress chan Progress

	if cfg != nil {
		if cfg.FinalityDepth != 0 {
			finalityDepth = cfg.FinalityDepth
		}
		if cfg.MaxBlockRange != 0 {
			maxBlockRange = cfg.MaxBlockRange
		}
		if cfg.ProgressCh != nil {
			progress = cfg.ProgressCh
		}
	}

	idx := &Indexer{
		c: c,
		f: f,
		h: h,
		s: s,

		maxBlockRange: maxBlockRange,
		finalityDepth: finalityDepth,
	}

	{
		head, ok, err := restoreCheckpoint(ctx, s, h)
		if err != nil {
			return nil, err
		}
		if ok {
			idx.head = head
		}
	}

	from := f.FromBlock
	if idx.head != nil {
		from = idx.head.Number + 1
	}

	head, err := backfill(ctx, c, h, s, f, from, maxBlockRange, progress)
	if err != nil {
		return nil, err
	}
	idx.head = head

	return idx, nil
}

// Process ingests a new head.
//
// If the header does not extend the current chain (its parent hash does not
// match the known head), a reorg has occurred. Process handles it internally
// by restoring handler state from the last finalized checkpoint and
// re-indexing the divergent range up to the new head. The caller therefore
// only sees errors for genuine failures, not for reorgs.
func (idx *Indexer) Process(ctx context.Context, h *types.Header) error {
	hn := h.Number.Uint64()

	// Only check for reorgs when appending the strictly sequential next block.
	// This safely bypasses the check during the brief transition from the
	// backfilled state to the live network head.
	if idx.head.Number == hn-1 && idx.head.Hash != h.ParentHash {
		idx.head = nil
		idx.dangling = nil

		head, ok, err := restoreCheckpoint(ctx, idx.s, idx.h)
		if err != nil {
			return fmt.Errorf("restore after reorg: %w", err)
		}
		if !ok {
			return errors.New("reorg detected before any finalized checkpoint was taken")
		}
		idx.head = head
	}

	if err := index(ctx, idx.c, idx.h, idx.f, idx.head.Number+1, hn, idx.maxBlockRange); err != nil {
		return err
	}

	head := BlockRef{Number: hn, Hash: h.Hash()}

	if idx.dangling == nil {
		if err := saveCheckpoint(ctx, idx.h, idx.s, head); err != nil {
			return fmt.Errorf("checkpoint: %w", err)
		}

		idx.dangling = &head
	}

	if idx.head.Number >= idx.dangling.Number+idx.finalityDepth {
		if err := promoteCheckpoint(ctx, idx.s, *idx.dangling); err != nil {
			return fmt.Errorf("promote checkpoint: %w", err)
		}

		idx.dangling = nil
	}

	idx.head = &head

	return nil
}

func backfill(
	ctx context.Context,
	c Client,
	h Handler,
	s Store,
	f Filter,
	from, maxBlockRange uint64,
	progress chan<- Progress,
) (*BlockRef, error) {
	final, err := c.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		return nil, err
	}

	to := final.Number.Uint64()

	if from > to {
		return nil, nil
	}

	if progress != nil {
		if err := reportProgress(ctx, progress, Progress{from, to}); err != nil {
			return nil, err
		}
	}

	for _, ch := range chunkBlockRange(from, to, maxBlockRange) {
		q := newFilterQuery(f, ch.from, ch.to)

		logs, err := cachedFilterLogs(ctx, c, s, q)
		if err != nil {
			return nil, fmt.Errorf("get logs: %w", err)
		}

		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if err := h.Process(ctx, logs); err != nil {
			return nil, fmt.Errorf("process logs: %w", err)
		}

		if progress != nil {
			if err := reportProgress(ctx, progress, Progress{ch.to, to}); err != nil {
				return nil, err
			}
		}
	}

	return &BlockRef{Number: to, Hash: final.Hash()}, nil
}

func index(
	ctx context.Context,
	c Client,
	h Handler,
	f Filter,
	from, to, maxBlockRange uint64,
) error {
	if from > to {
		panic(fmt.Errorf("invalid block range: from (%d) > to (%d)", from, to))
	}

	for _, ch := range chunkBlockRange(from, to, maxBlockRange) {
		q := newFilterQuery(f, ch.from, ch.to)

		logs, err := c.FilterLogs(ctx, q)
		if err != nil {
			return fmt.Errorf("get logs: %w", err)
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		if err := h.Process(ctx, logs); err != nil {
			return fmt.Errorf("process logs: %w", err)
		}
	}

	return nil
}

func reportProgress(ctx context.Context, ch chan<- Progress, p Progress) error {
	select {
	case ch <- p:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func restoreCheckpoint(ctx context.Context, s Store, h Handler) (*BlockRef, bool, error) {
	cpb, err := s.Load(ctx, finalizedCP)
	if err != nil {
		return nil, false, fmt.Errorf("load finalized checkpoint: %w", err)
	}
	if len(cpb) == 0 {
		return nil, false, nil
	}

	var cp checkpoint
	if err := cp.UnmarshalBinary(cpb); err != nil {
		return nil, false, fmt.Errorf("parse finalized checkpoint: %w", err)
	}

	if err := h.Restore(ctx, cp.State); err != nil {
		return nil, false, fmt.Errorf("restore checkpoint %s: %w", cp.BlockHash, err)
	}

	return &BlockRef{Number: cp.BlockNumber, Hash: cp.BlockHash}, true, nil
}

func promoteCheckpoint(
	ctx context.Context,
	s Store,
	confirm BlockRef,
) error {
	cpb, err := s.Load(ctx, danglingCP)
	if err != nil {
		return fmt.Errorf("load dangling checkpoint: %w", err)
	}
	if len(cpb) == 0 {
		return errors.New("dangling checkpoint missing from store")
	}

	var cp checkpoint
	if err := cp.UnmarshalBinary(cpb); err != nil {
		return fmt.Errorf("parse dangling checkpoint: %w", err)
	}

	if cp.BlockNumber != confirm.Number || cp.BlockHash != confirm.Hash {
		return nil
	}

	if err := s.Save(ctx, finalizedCP, cpb); err != nil {
		return fmt.Errorf("save finalized checkpoint: %w", err)
	}

	return nil
}

func saveCheckpoint(ctx context.Context, h Handler, s Store, head BlockRef) error {
	state, err := h.Snapshot(ctx)
	if err != nil {
		return err
	}

	cp := checkpoint{
		BlockNumber: head.Number,
		BlockHash:   head.Hash,
		State:       state,
	}

	cpb, err := cp.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	if err := s.Save(ctx, danglingCP, cpb); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	return nil
}
