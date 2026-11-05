package ethindexer

import (
	"context"
	"math/big"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

type restoreItem struct {
	Name    string
	Balance uint64
}

type restoreNested struct {
	Counts map[string]int
	Flag   bool
}

// restoreState mirrors a typical handler: an index map into a slice, plain
// fields, a nested struct, and an unexported dependency that is not part of
// the checkpoint.
type restoreState struct {
	Index   map[string]int
	Items   []restoreItem
	Empty   map[string]bool
	Total   uint64
	Nested  restoreNested
	backend string
}

func (s *restoreState) Process(context.Context, []types.Log) error { return nil }

func (s *restoreState) add(name string, balance uint64) {
	s.Index[name] = len(s.Items)
	s.Items = append(s.Items, restoreItem{Name: name, Balance: balance})
}

func TestIndexer_ReorgRestoresCheckpointExactly(t *testing.T) {
	ctx := t.Context()

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

	state := &restoreState{
		Index:   map[string]int{},
		Empty:   map[string]bool{},
		Nested:  restoreNested{Counts: map[string]int{"a": 1}},
		backend: "rpc",
	}
	state.add("alice", 0)
	state.add("bob", 7)
	state.Total = 7

	indexer := &Indexer[*restoreState]{Client: client, DataDir: t.TempDir(), FromBlock: 10, State: state}
	if err := indexer.Process(ctx, h10); err != nil {
		t.Fatal(err)
	}
	want := &restoreState{
		Index:   map[string]int{"alice": 0, "bob": 1},
		Items:   []restoreItem{{Name: "alice"}, {Name: "bob", Balance: 7}},
		Empty:   map[string]bool{},
		Total:   7,
		Nested:  restoreNested{Counts: map[string]int{"a": 1}},
		backend: "rpc",
	}

	// Blocks after the checkpoint: a new entry, zero fields becoming nonzero,
	// and an entry removed from a map.
	if err := indexer.Process(ctx, h11); err != nil {
		t.Fatal(err)
	}
	state.add("carol", 3)
	state.Items[0].Balance = 5
	state.Empty["x"] = true
	state.Nested.Counts["b"] = 2
	state.Nested.Flag = true
	delete(state.Index, "bob")

	h12Bad := &types.Header{Number: big.NewInt(12), ParentHash: common.HexToHash("0xdeadbeef")}
	if err := indexer.Process(ctx, h12Bad); err != nil {
		t.Fatalf("process h12 after reorg: %v", err)
	}

	if indexer.State != state {
		t.Fatal("restore replaced the State pointer")
	}
	if !reflect.DeepEqual(state, want) {
		t.Fatalf("state after reorg = %+v\nwant %+v", state, want)
	}
	// Maps that were empty at the checkpoint stay usable.
	state.Empty["y"] = true
}
