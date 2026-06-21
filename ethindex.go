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
	s Store

	// Configs
	maxBlockRange uint64
	finalityDepth uint64

	// State
	dangling    *BlockRef
	head        *BlockRef
	initialized bool
}

// New creates a new indexer instance
func New(c Client, h Handler, s Store, cfg *Config) *Indexer {
	if cfg == nil {
		cfg = &Config{}
	}
	if cfg.FinalityDepth == 0 {
		cfg.FinalityDepth = 64
	}
	if cfg.MaxBlockRange == 0 {
		cfg.MaxBlockRange = 10_000
	}

	return &Indexer{
		c: c,
		h: h,
		s: s,

		maxBlockRange: cfg.MaxBlockRange,
		finalityDepth: cfg.FinalityDepth,
	}
}

// Init starts the indexer
func (idx *Indexer) Init(ctx context.Context) error {
	if idx.initialized {
		panic("Init called on an initialized indexer")
	}

	if err := idx.restore(); err != nil {
		return err
	}

	if err := idx.backfill(ctx); err != nil {
		return err
	}

	idx.initialized = true

	return nil
}

// Process ingests a new head
func (idx *Indexer) Process(ctx context.Context, h *types.Header) error {
	if !idx.initialized {
		panic("Process called an non initialized indexer")
	}

	hn := h.Number.Uint64()

	// Only check for reorgs when appending the strictly sequential next block.
	// This safely bypasses the check during the brief transition from the
	// backfilled state to the live network head.
	if idx.head.Number == hn-1 && idx.head.Hash != h.ParentHash {
		return ErrReorg
	}

	if err := idx.processRange(ctx, idx.head.Number+1, hn); err != nil {
		return err
	}

	idx.head = &BlockRef{Number: hn, Hash: h.Hash()}

	if err := idx.maybeCheckpoint(); err != nil {
		return err
	}

	return nil
}

func (idx *Indexer) backfill(ctx context.Context) error {
	final, err := idx.c.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		return err
	}

	from := idx.h.Filter().FromBlock
	if idx.head != nil {
		from = idx.head.Number + 1
	}
	to := final.Number.Uint64()

	if from > to {
		return nil
	}

	if err := idx.processRange(ctx, from, to); err != nil {
		return err
	}

	idx.head = &BlockRef{
		Number: final.Number.Uint64(),
		Hash:   final.Hash(),
	}

	return nil
}

func (idx *Indexer) processRange(ctx context.Context, from, to uint64) error {
	if from > to {
		panic(fmt.Errorf("invalid block range: from (%d) > to (%d)", from, to))
	}

	for start := from; start <= to; start += uint64(idx.maxBlockRange) {
		end := min(start+uint64(idx.maxBlockRange)-1, to)

		logs, err := idx.getLogs(ctx, start, end)
		if err != nil {
			return fmt.Errorf("get logs: %w", err)
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		if err := idx.h.Process(ctx, logs); err != nil {
			return fmt.Errorf("process logs: %w", err)
		}
	}

	return nil
}

func (idx *Indexer) getLogs(ctx context.Context, from, to uint64) ([]types.Log, error) {
	q := newFilterQuery(idx.h.Filter(), from, to)

	if !idx.initialized {
		logs, err := loadCachedLogs(idx.s, q)
		if err != nil {
			return nil, err
		}
		if logs != nil {
			return logs, nil
		}
	}

	logs, err := idx.c.FilterLogs(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("filter logs: %w", err)
	}

	if !idx.initialized {
		if err := saveCachedLogs(idx.s, q, logs); err != nil {
			return nil, err
		}
	}

	return logs, nil
}

func (idx *Indexer) restore() error {
	cpb, err := idx.s.Load(finalizedCP)
	if err != nil {
		return fmt.Errorf("load finalized checkpoint: %w", err)
	}
	if len(cpb) == 0 {
		return nil
	}

	var cp checkpoint
	if err := cp.UnmarshalBinary(cpb); err != nil {
		return fmt.Errorf("parse finalized checkpoint: %w", err)
	}

	if err := idx.h.Restore(cp.State); err != nil {
		return fmt.Errorf("restore checkpoint %s: %w", cp.BlockHash, err)
	}

	idx.head = &BlockRef{Number: cp.BlockNumber, Hash: cp.BlockHash}

	return nil
}

func (idx *Indexer) maybeCheckpoint() error {
	if idx.head == nil {
		return nil
	}

	if idx.dangling == nil {
		if err := idx.saveDangling(); err != nil {
			return fmt.Errorf("initial checkpoint: %w", err)
		}
	}

	if idx.head.Number < idx.dangling.Number+idx.finalityDepth {
		return nil
	}

	if err := idx.promoteDangling(); err != nil {
		return fmt.Errorf("promote checkpoint: %w", err)
	}

	if err := idx.saveDangling(); err != nil {
		return fmt.Errorf("next checkpoint: %w", err)
	}

	return nil
}

func (idx *Indexer) promoteDangling() error {
	cpb, err := idx.s.Load(danglingCP)
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

	if cp.BlockNumber != idx.dangling.Number || cp.BlockHash != idx.dangling.Hash {
		idx.dangling = nil

		return nil
	}

	if err := idx.s.Save(finalizedCP, cpb); err != nil {
		return fmt.Errorf("save finalized checkpoint: %w", err)
	}

	idx.dangling = nil

	return nil
}

func (idx *Indexer) saveDangling() error {
	state, err := idx.h.Snapshot()
	if err != nil {
		return err
	}

	cp := checkpoint{
		BlockNumber: idx.head.Number,
		BlockHash:   idx.head.Hash,
		State:       state,
	}

	cpb, err := cp.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	if err := idx.s.Save(danglingCP, cpb); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	idx.dangling = &BlockRef{Number: cp.BlockNumber, Hash: cp.BlockHash}

	return nil
}
