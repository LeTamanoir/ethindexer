package ethindex

import (
	"context"
	"math/big"
	"testing"

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
		// subscribeNewHeadFunc: func(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
		// 	// Stop the indexer as soon as it tries to go live
		// 	sub := newMockSubscription()
		// 	go func() { sub.errCh <- context.Canceled }()
		// 	return sub, nil
		// },
	}

	handler := &mockHandler{
		filter: Filter{FromBlock: 50},
	}

	indexer := New(client, handler, newMockStore(), nil)

	err := indexer.Init(ctx)
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

	handler := &mockHandler{
		filter: Filter{FromBlock: 10},
	}

	indexer := New(client, handler, newMockStore(), nil)

	if err := indexer.Init(ctx); err != nil {
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

	reorgTriggered := false

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

	handler := &mockHandler{
		filter: Filter{FromBlock: 10},
	}

	indexer := New(client, handler, newMockStore(), nil)

	if err := indexer.Init(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Push a valid block.
	h11 := &types.Header{Number: big.NewInt(11), ParentHash: indexer.head.Hash}
	if err := indexer.Process(ctx, h11); err != nil {
		t.Fatalf("process h11: %v", err)
	}

	// Push a block with the wrong parent hash to trigger a reorg.
	h12 := &types.Header{
		Number:     big.NewInt(12),
		ParentHash: common.HexToHash("0xdeadbeef"), // mismatch
	}
	if err := indexer.Process(ctx, h12); err != nil {
		if err == ErrReorg {
			reorgTriggered = true
		} else {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if !reorgTriggered {
		t.Errorf("expected reorg to be triggered")
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
	store.Save(finalizedCP, cpb)

	client := &mockClient{
		headerByNumberFunc: func(ctx context.Context, number *big.Int) (*types.Header, error) {
			return &types.Header{
				Number: big.NewInt(int64(finalizedBlockNum)),
			}, nil
		},
		filterLogsFunc: func(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			return nil, nil
		},
		// subscribeNewHeadFunc: func(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
		// 	sub := newMockSubscription()
		// 	go func() { sub.errCh <- context.Canceled }()
		// 	return sub, nil
		// },
	}

	handler := &mockHandler{
		filter: Filter{FromBlock: 10},
	}

	indexer := New(client, handler, store, nil)

	err = indexer.Init(ctx)
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
