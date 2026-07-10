package ethindexer

import (
	"context"
	"errors"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
)

type mockClient struct {
	headerByNumberFunc func(ctx context.Context, number *big.Int) (*types.Header, error)
	filterLogsFunc     func(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
}

func (m *mockClient) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	if m.headerByNumberFunc != nil {
		return m.headerByNumberFunc(ctx, number)
	}
	return nil, nil
}

func (m *mockClient) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	if m.filterLogsFunc != nil {
		return m.filterLogsFunc(ctx, q)
	}
	return nil, nil
}

type mockHandler struct {
	filter      Filter
	mu          sync.Mutex
	processed   []types.Log
	state       []byte
	processErr  error
	snapshotErr error
	restoreErr  error
	initCalled  bool
	initErr     error
	initClient  ChainReader
}

func (m *mockHandler) Filter() Filter {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.filter
}

func (m *mockHandler) Snapshot(context.Context) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state, m.snapshotErr
}

func (m *mockHandler) Restore(_ context.Context, state []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state
	return m.restoreErr
}

func (m *mockHandler) Process(ctx context.Context, logs []types.Log) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.processErr != nil {
		return m.processErr
	}
	m.processed = append(m.processed, logs...)
	return nil
}

func (m *mockHandler) Init(ctx context.Context, client ChainReader) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initCalled = true
	m.initClient = client
	return m.initErr
}

type mockStore struct {
	mu    sync.Mutex
	store map[string][]byte
}

func newMockStore() *mockStore {
	return &mockStore{
		store: make(map[string][]byte),
	}
}

func (m *mockStore) Read(_ context.Context, name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	val, ok := m.store[name]
	if !ok {
		return nil, nil
	}
	return val, nil
}

func (m *mockStore) Write(_ context.Context, name string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store[name] = data
	return nil
}

func (m *mockStore) Move(_ context.Context, srcKey, dstKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	val, ok := m.store[srcKey]
	if !ok {
		return errors.New("source key not found")
	}
	m.store[dstKey] = val
	delete(m.store, srcKey)
	return nil
}
