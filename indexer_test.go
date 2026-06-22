package ethindex

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

func TestIndexer_Backfill(t *testing.T) {
	ctx := t.Context()

	finalizedBlockNum := uint64(100)

	client := &mockClient{
		headerByNumberFunc: func(ctx context.Context, number *big.Int) (*types.Header, error) {
			if number.Int64() == int64(rpc.FinalizedBlockNumber) {
				return &types.Header{
					Number: big.NewInt(int64(finalizedBlockNum)),
				}, nil
			}
			return nil, nil
		},
		filterLogsFunc: func(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			// Mock returning one log per block in the query
			var logs []types.Log
			for i := q.FromBlock.Uint64(); i <= q.ToBlock.Uint64(); i++ {
				logs = append(logs, types.Log{BlockNumber: i})
			}
			return logs, nil
		},
	}

	filter := Filter{FromBlock: 50}
	handler := &mockHandler{}

	indexer := NewIndexer(client, handler, filter, newMockStore(), nil)

	err := indexer.Init(ctx, nil)
	if err != nil && err != context.Canceled {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}

	if indexer.head.Number != finalizedBlockNum {
		t.Errorf("expected head number %d, got %d", finalizedBlockNum, indexer.head.Number)
	}

	if len(handler.processed) != int(finalizedBlockNum-50+1) {
		t.Errorf("expected %d processed logs, got %d", finalizedBlockNum-50+1, len(handler.processed))
	}
}

func TestIndexer_Live(t *testing.T) {
	ctx := t.Context()

	finalizedBlockNum := uint64(10)

	client := &mockClient{
		headerByNumberFunc: func(ctx context.Context, number *big.Int) (*types.Header, error) {
			return &types.Header{
				Number: big.NewInt(int64(finalizedBlockNum)),
			}, nil
		},
		filterLogsFunc: func(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			return []types.Log{{BlockNumber: q.FromBlock.Uint64()}}, nil
		},
	}

	filter := Filter{FromBlock: 10}
	handler := &mockHandler{}

	indexer := NewIndexer(client, handler, filter, newMockStore(), nil)

	if err := indexer.Init(ctx, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simulate the user feeding live new heads to the indexer.
	h11 := &types.Header{Number: big.NewInt(11), ParentHash: indexer.head.Hash}
	h12 := &types.Header{Number: big.NewInt(12), ParentHash: h11.Hash()}
	h13 := &types.Header{Number: big.NewInt(13), ParentHash: h12.Hash()}

	for _, h := range []*types.Header{h11, h12, h13} {
		if err := indexer.Process(ctx, h); err != nil {
			t.Fatalf("process head %d: %v", h.Number, err)
		}
	}

	if indexer.head.Number != 13 {
		t.Errorf("expected head number 13, got %d", indexer.head.Number)
	}
}

func TestIndexer_Reorg(t *testing.T) {
	ctx := t.Context()

	finalizedBlockNum := uint64(10)

	client := &mockClient{
		headerByNumberFunc: func(ctx context.Context, number *big.Int) (*types.Header, error) {
			return &types.Header{
				Number: big.NewInt(int64(finalizedBlockNum)),
			}, nil
		},
		filterLogsFunc: func(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			return nil, nil
		},
	}

	filter := Filter{FromBlock: 10}
	handler := &mockHandler{}

	store := newMockStore()
	indexer := NewIndexer(client, handler, filter, store, nil)

	if err := indexer.Init(ctx, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Save a finalized checkpoint so Process can recover from a reorg.
	cp := checkpoint{
		BlockNumber: indexer.head.Number,
		BlockHash:   indexer.head.Hash,
		State:       []byte("restored_state"),
	}
	cpb, err := cp.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(t.Context(), finalizedCP, cpb); err != nil {
		t.Fatal(err)
	}

	// Push a valid block.
	h11 := &types.Header{Number: big.NewInt(11), ParentHash: indexer.head.Hash}
	if err := indexer.Process(ctx, h11); err != nil {
		t.Fatalf("process h11: %v", err)
	}

	// Corrupt handler state to prove the reorg path restores it.
	handler.state = []byte("corrupted")

	// Push a block with the wrong parent hash to trigger a reorg.
	h12 := &types.Header{
		Number:     big.NewInt(12),
		ParentHash: common.HexToHash("0xdeadbeef"), // mismatch
	}
	if err := indexer.Process(ctx, h12); err != nil {
		t.Fatalf("process h12 after reorg: %v", err)
	}

	if string(handler.state) != "restored_state" {
		t.Errorf("expected handler state to be restored after reorg, got %q", handler.state)
	}

	if indexer.head.Number != 12 {
		t.Errorf("expected head to be 12 after reorg recovery, got %d", indexer.head.Number)
	}
}

func TestIndexer_Progress(t *testing.T) {
	ctx := t.Context()

	finalizedBlockNum := uint64(100)

	client := &mockClient{
		headerByNumberFunc: func(ctx context.Context, number *big.Int) (*types.Header, error) {
			if number.Int64() == int64(rpc.FinalizedBlockNumber) {
				return &types.Header{
					Number: big.NewInt(int64(finalizedBlockNum)),
				}, nil
			}
			return nil, nil
		},
		filterLogsFunc: func(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			// Slow each RPC call so the polling goroutine can observe progress
			// mid-backfill (and exercise concurrent Progress reads under -race).
			time.Sleep(10 * time.Millisecond)
			var logs []types.Log
			for i := q.FromBlock.Uint64(); i <= q.ToBlock.Uint64(); i++ {
				logs = append(logs, types.Log{BlockNumber: i})
			}
			return logs, nil
		},
	}

	filter := Filter{FromBlock: 50}
	handler := &mockHandler{}

	var lastProgress Progress
	progress := make(chan Progress)
	done := make(chan struct{})

	indexer := NewIndexer(client, handler, filter, newMockStore(), nil)

	go func() {
		for p := range progress {
			lastProgress = p
		}
		close(done)
	}()

	if err := indexer.Init(ctx, progress); err != nil && err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}
	close(progress)
	<-done

	if lastProgress.ToBlock != finalizedBlockNum {
		t.Errorf("expected to block %d, got %d", finalizedBlockNum, lastProgress.ToBlock)
	}
	if lastProgress.CurrentBlock != finalizedBlockNum {
		t.Errorf("expected current block %d, got %d", finalizedBlockNum, lastProgress.CurrentBlock)
	}
}

func TestIndexer_Restore(t *testing.T) {
	ctx := t.Context()

	finalizedBlockNum := uint64(50)

	cp := checkpoint{
		BlockNumber: 50,
		BlockHash:   common.HexToHash("0x123"),
		State:       []byte("restored_state"),
	}
	cpb, err := cp.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	store := newMockStore()
	store.Save(t.Context(), finalizedCP, cpb)

	client := &mockClient{
		headerByNumberFunc: func(ctx context.Context, number *big.Int) (*types.Header, error) {
			return &types.Header{
				Number: big.NewInt(int64(finalizedBlockNum)),
			}, nil
		},
		filterLogsFunc: func(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			return nil, nil
		},
	}

	filter := Filter{FromBlock: 10}
	handler := &mockHandler{}

	indexer := NewIndexer(client, handler, filter, store, nil)

	err = indexer.Init(ctx, nil)
	if err != nil && err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(handler.state) != "restored_state" {
		t.Errorf("expected handler state to be restored, got %s", handler.state)
	}

	if indexer.head.Number != 50 {
		t.Errorf("expected head to be 50, got %d", indexer.head.Number)
	}
}
