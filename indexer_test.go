package ethindexer

import (
	"context"
	"errors"
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

	handler := &mockHandler{}

	indexer := indexerForHandler(client, handler, t.TempDir(), 50)
	if err := indexer.Sync(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if indexer.head.number != finalizedBlockNum {
		t.Errorf("expected head number %d, got %d", finalizedBlockNum, indexer.head.number)
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

	handler := &mockHandler{}

	indexer := indexerForHandler(client, handler, t.TempDir(), 10)
	if err := indexer.Sync(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simulate the user feeding live new heads to the indexer.
	h11 := &types.Header{Number: big.NewInt(11), ParentHash: indexer.head.hash}
	h12 := &types.Header{Number: big.NewInt(12), ParentHash: h11.Hash()}
	h13 := &types.Header{Number: big.NewInt(13), ParentHash: h12.Hash()}

	for _, h := range []*types.Header{h11, h12, h13} {
		if err := indexer.Process(ctx, h); err != nil {
			t.Fatalf("process head %d: %v", h.Number, err)
		}
	}

	if indexer.head.number != 13 {
		t.Errorf("expected head number 13, got %d", indexer.head.number)
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

	handler := &mockHandler{}
	dataDir := t.TempDir()

	indexer := indexerForHandler(client, handler, dataDir, 10)
	indexer.FinalityDepth = 2
	indexer.CheckpointInterval = 1
	if err := indexer.Sync(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Build a consecutive chain so no reorg is triggered.
	h11 := &types.Header{Number: big.NewInt(11), ParentHash: indexer.head.hash}
	h12 := &types.Header{Number: big.NewInt(12), ParentHash: h11.Hash()}
	h13 := &types.Header{Number: big.NewInt(13), ParentHash: h12.Hash()}

	for _, h := range []*types.Header{h11, h12, h13} {
		if err := indexer.Process(ctx, h); err != nil {
			t.Fatalf("process head %d: %v", h.Number, err)
		}
	}

	// Head 13 >= staged(11) + finalityDepth(2), so the staged checkpoint
	// at head 11 should have been promoted to finalized via Move.
	cpb, err := readBlob(dataDir, checkpointKey)
	if err != nil {
		t.Fatalf("load finalized: %v", err)
	}
	if len(cpb) == 0 {
		t.Fatal("expected finalized checkpoint after promote")
	}
	cp, err := unmarshalCheckpoint(cpb)
	if err != nil {
		t.Fatal("expected valid checkpoint")
	}
	if cp.head.number != 11 {
		t.Errorf("expected finalized head 11 after promote, got %d", cp.head.number)
	}

	// The staged key should be gone after the move.
	if d, err := readBlob(dataDir, checkpointStagedKey); err != nil {
		t.Fatalf("unexpected error loading staged: %v", err)
	} else if d != nil {
		t.Errorf("expected staged checkpoint to be moved away, got %d bytes", len(d))
	}

	if indexer.staged != nil {
		t.Errorf("expected staged to be reset after promote, got %d", indexer.staged.number)
	}
}

// TestIndexer_PromoteGuardNoStaged verifies that the promote check does
// not fire when idx.staged is zero, even if head.Number >= finalityDepth.
func TestIndexer_PromoteGuardNoStaged(t *testing.T) {
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

	handler := &mockHandler{}
	dataDir := t.TempDir()

	indexer := indexerForHandler(client, handler, dataDir, 100)
	indexer.FinalityDepth = 2
	indexer.CheckpointInterval = 1
	if err := indexer.Sync(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simulate staged being empty (e.g. after a restart that didn't
	// restore it) while head is well past finalityDepth. The promote check
	// must be a no-op, not a crash.
	indexer.staged = nil
	indexer.head = &blockRef{number: 200, hash: common.HexToHash("0xabc")}

	h201 := &types.Header{Number: big.NewInt(201), ParentHash: indexer.head.hash}

	// This should NOT crash with "staged checkpoint missing from store".
	if err := indexer.Process(ctx, h201); err != nil {
		t.Fatalf("unexpected error from promote guard: %v", err)
	}

	// After processing, a new staged checkpoint should have been saved
	// (since staged was empty), and no promote should have fired.
	if indexer.staged == nil {
		t.Error("expected staged to be set after processing with empty staged")
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

	handler := &mockHandler{}

	dataDir := t.TempDir()

	// Save a finalized checkpoint so Process can recover from a reorg.
	cp := checkpoint{
		head:  blockRef{number: finalizedBlockNum, hash: h10.Hash()},
		state: []byte("restored_state"),
	}
	cpb, err := marshalCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeBlob(dataDir, checkpointKey, cpb); err != nil {
		t.Fatal(err)
	}

	indexer := indexerForHandler(client, handler, dataDir, 10)
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

	if indexer.head.number != 12 {
		t.Errorf("expected head to be 12 after reorg recovery, got %d", indexer.head.number)
	}
}

func TestIndexer_Restore(t *testing.T) {
	ctx := t.Context()

	finalizedBlockNum := uint64(50)

	cp := checkpoint{
		head:  blockRef{number: 50, hash: common.HexToHash("0x123")},
		state: []byte("restored_state"),
	}
	cpb, err := marshalCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}

	dataDir := t.TempDir()
	if err := writeBlob(dataDir, checkpointKey, cpb); err != nil {
		t.Fatal(err)
	}

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

	handler := &mockHandler{}

	indexer := indexerForHandler(client, handler, dataDir, 10)
	if err := indexer.Sync(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(handler.state) != "restored_state" {
		t.Errorf("expected handler state to be restored, got %s", handler.state)
	}

	if indexer.head.number != 50 {
		t.Errorf("expected head to be 50, got %d", indexer.head.number)
	}
}

func TestIndexer_InitCalledOnFreshStart(t *testing.T) {
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

	handler := &mockHandler{}
	indexer := indexerForHandler(client, handler, t.TempDir(), 10)

	if err := indexer.Sync(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !handler.initCalled {
		t.Error("expected Init to be called on fresh start")
	}
	if handler.initClient == nil {
		t.Fatal("expected Init to receive a cached client")
	}
	if handler.initClient.client != client || handler.initClient.dataDir != indexer.DataDir {
		t.Error("expected cached client to wrap the indexer's client and data directory")
	}
}

func TestIndexer_InitSkippedOnRestore(t *testing.T) {
	ctx := t.Context()

	finalizedBlockNum := uint64(50)

	cp := checkpoint{
		head:  blockRef{number: 50, hash: common.HexToHash("0x123")},
		state: []byte("restored_state"),
	}
	cpb, err := marshalCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}

	dataDir := t.TempDir()
	if err := writeBlob(dataDir, checkpointKey, cpb); err != nil {
		t.Fatal(err)
	}

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

	handler := &mockHandler{}
	indexer := indexerForHandler(client, handler, dataDir, 10)

	if err := indexer.Sync(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if handler.initCalled {
		t.Error("expected Init to be skipped when a checkpoint is restored")
	}
}

func TestIndexer_InitError(t *testing.T) {
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

	wantErr := errors.New("init failed")
	handler := &mockHandler{initErr: wantErr}
	indexer := indexerForHandler(client, handler, t.TempDir(), 10)

	err := indexer.Sync(ctx)
	if err == nil {
		t.Fatal("expected error from Init, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}
	if !handler.initCalled {
		t.Error("expected Init to be called before failing")
	}
	if indexer.head != nil {
		t.Error("expected indexer head to remain nil when Init fails")
	}
}
