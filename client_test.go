package ethindexer

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestCachedClient_FilterLogsCachesRangeQueries(t *testing.T) {
	filterCalls := 0
	client := &mockClient{
		filterLogsFunc: func(context.Context, ethereum.FilterQuery) ([]types.Log, error) {
			filterCalls++
			return []types.Log{{BlockNumber: 12}}, nil
		},
	}
	cached := &CachedClient{client: client, dataDir: t.TempDir()}
	query := Filter{}.rangeQuery(10, 20)

	for range 2 {
		logs, err := cached.FilterLogs(t.Context(), query)
		if err != nil {
			t.Fatalf("logs range: %v", err)
		}
		if len(logs) != 1 || logs[0].BlockNumber != 12 {
			t.Fatalf("unexpected logs: %+v", logs)
		}
	}

	if filterCalls != 1 {
		t.Fatalf("expected one underlying log query, got %d", filterCalls)
	}
}

func TestCachedClient_FilterLogsDelegatesBlockQueries(t *testing.T) {
	filterCalls := 0
	client := &mockClient{
		filterLogsFunc: func(context.Context, ethereum.FilterQuery) ([]types.Log, error) {
			filterCalls++
			return nil, nil
		},
	}
	cached := &CachedClient{client: client, dataDir: t.TempDir()}
	query := Filter{}.blockQuery(types.EmptyRootHash)

	for range 2 {
		if _, err := cached.FilterLogs(t.Context(), query); err != nil {
			t.Fatalf("filter logs: %v", err)
		}
	}

	if filterCalls != 2 {
		t.Fatalf("expected block queries to bypass the cache, got %d underlying calls", filterCalls)
	}
}

func TestCachedClient_DelegatesHeaderReads(t *testing.T) {
	wantHeader := &types.Header{Number: big.NewInt(42)}
	client := &mockClient{
		headerByNumberFunc: func(context.Context, *big.Int) (*types.Header, error) {
			return wantHeader, nil
		},
	}
	cached := &CachedClient{client: client, dataDir: t.TempDir()}

	header, err := cached.HeaderByNumber(t.Context(), big.NewInt(42))
	if err != nil {
		t.Fatalf("header by number: %v", err)
	}
	if header != wantHeader {
		t.Fatalf("expected delegated header %p, got %p", wantHeader, header)
	}
}
