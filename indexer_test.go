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
	}

	filter := Filter{FromBlock: 50}
	handler := &mockHandler{}

	indexer := NewIndexer(client, handler, filter, newMockStore(), testLogger(), Config{})
	if err := indexer.Sync(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
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
			var num uint64
			if q.FromBlock != nil {
				num = q.FromBlock.Uint64()
			}
			return []types.Log{{BlockNumber: num}}, nil
		},
	}

	filter := Filter{FromBlock: 10}
	handler := &mockHandler{}

	indexer := NewIndexer(client, handler, filter, newMockStore(), testLogger(), Config{})
	if err := indexer.Sync(ctx); err != nil {
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

func TestIndexer_Promote(t *testing.T) {
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

	indexer := NewIndexer(client, handler, filter, store, testLogger(), Config{FinalityDepth: 2})
	if err := indexer.Sync(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Build a consecutive chain so no reorg is triggered.
	h11 := &types.Header{Number: big.NewInt(11), ParentHash: indexer.head.Hash}
	h12 := &types.Header{Number: big.NewInt(12), ParentHash: h11.Hash()}
	h13 := &types.Header{Number: big.NewInt(13), ParentHash: h12.Hash()}

	for _, h := range []*types.Header{h11, h12, h13} {
		if err := indexer.Process(ctx, h); err != nil {
			t.Fatalf("process head %d: %v", h.Number, err)
		}
	}

	// Head 13 >= dangling(11) + finalityDepth(2), so the dangling checkpoint
	// at head 11 should have been promoted to finalized via Move.
	cp, ok, err := loadCheckpoint(ctx, store, finalized)
	if err != nil {
		t.Fatalf("load finalized: %v", err)
	}
	if !ok {
		t.Fatal("expected finalized checkpoint after promote")
	}
	if cp.Head.Number != 11 {
		t.Errorf("expected finalized head 11 after promote, got %d", cp.Head.Number)
	}

	// The dangling key should be gone after the move.
	if d, err := store.Read(ctx, string(dangling)); err != nil {
		t.Fatalf("unexpected error loading dangling: %v", err)
	} else if d != nil {
		t.Errorf("expected dangling checkpoint to be moved away, got %d bytes", len(d))
	}

	if indexer.dangling != (BlockRef{}) {
		t.Errorf("expected dangling to be reset after promote, got %d", indexer.dangling.Number)
	}
}

// TestIndexer_PromoteGuardNoDangling verifies that the promote check does
// not fire when idx.dangling is zero, even if head.Number >= finalityDepth.
func TestIndexer_PromoteGuardNoDangling(t *testing.T) {
	ctx := t.Context()

	finalizedBlockNum := uint64(100)

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

	filter := Filter{FromBlock: 100}
	handler := &mockHandler{}
	store := newMockStore()

	indexer := NewIndexer(client, handler, filter, store, testLogger(), Config{FinalityDepth: 2})
	if err := indexer.Sync(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simulate dangling being empty (e.g. after a restart that didn't
	// restore it) while head is well past finalityDepth. The promote check
	// must be a no-op, not a crash.
	indexer.dangling = BlockRef{}
	indexer.head = BlockRef{Number: 200, Hash: common.HexToHash("0xabc")}

	h201 := &types.Header{Number: big.NewInt(201), ParentHash: indexer.head.Hash}

	// This should NOT crash with "dangling checkpoint missing from store".
	if err := indexer.Process(ctx, h201); err != nil {
		t.Fatalf("unexpected error from promote guard: %v", err)
	}

	// After processing, a new dangling checkpoint should have been saved
	// (since dangling was empty), and no promote should have fired.
	if indexer.dangling == (BlockRef{}) {
		t.Error("expected dangling to be set after processing with empty dangling")
	}
}

func TestIndexer_Reorg(t *testing.T) {
	ctx := t.Context()

	finalizedBlockNum := uint64(10)

	// Build a deterministic chain so the mock client can serve headersRange.
	h10 := &types.Header{Number: big.NewInt(10)}
	h11 := &types.Header{Number: big.NewInt(11), ParentHash: h10.Hash()}
	h12 := &types.Header{Number: big.NewInt(12), ParentHash: h11.Hash()}

	client := &mockClient{
		headerByNumberFunc: func(ctx context.Context, number *big.Int) (*types.Header, error) {
			if number.Int64() == int64(rpc.FinalizedBlockNumber) {
				return h10, nil
			}
			switch number.Uint64() {
			case 10:
				return h10, nil
			case 11:
				return h11, nil
			case 12:
				return h12, nil
			}
			return nil, nil
		},
		filterLogsFunc: func(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			return nil, nil
		},
	}

	filter := Filter{FromBlock: 10}
	handler := &mockHandler{}

	store := newMockStore()

	// Save a finalized checkpoint so Process can recover from a reorg.
	cp := checkpoint{
		Head:  BlockRef{Number: finalizedBlockNum, Hash: h10.Hash()},
		State: []byte("restored_state"),
	}
	cpb, err := cp.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(t.Context(), string(finalized), cpb); err != nil {
		t.Fatal(err)
	}

	indexer := NewIndexer(client, handler, filter, store, testLogger(), Config{})
	if err := indexer.Sync(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Push a valid block.
	if err := indexer.Process(ctx, h11); err != nil {
		t.Fatalf("process h11: %v", err)
	}

	// Corrupt handler state to prove the reorg path restores it.
	handler.state = []byte("corrupted")

	// Push a block with the wrong parent hash to trigger a reorg.
	h12Bad := &types.Header{
		Number:     big.NewInt(12),
		ParentHash: common.HexToHash("0xdeadbeef"), // mismatch
	}
	if err := indexer.Process(ctx, h12Bad); err != nil {
		t.Fatalf("process h12 after reorg: %v", err)
	}

	if string(handler.state) != "restored_state" {
		t.Errorf("expected handler state to be restored after reorg, got %q", handler.state)
	}

	if indexer.head.Number != 12 {
		t.Errorf("expected head to be 12 after reorg recovery, got %d", indexer.head.Number)
	}
}

func TestIndexer_Restore(t *testing.T) {
	ctx := t.Context()

	finalizedBlockNum := uint64(50)

	cp := checkpoint{
		Head:  BlockRef{Number: 50, Hash: common.HexToHash("0x123")},
		State: []byte("restored_state"),
	}
	cpb, err := cp.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	store := newMockStore()
	store.Write(t.Context(), string(finalized), cpb)

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

	indexer := NewIndexer(client, handler, filter, store, testLogger(), Config{})
	if err := indexer.Sync(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(handler.state) != "restored_state" {
		t.Errorf("expected handler state to be restored, got %s", handler.state)
	}

	if indexer.head.Number != 50 {
		t.Errorf("expected head to be 50, got %d", indexer.head.Number)
	}
}
