package ethindex

import (
	"context"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
)

type mockSubscription struct {
	errCh   chan error
	unsubCh chan struct{}
}

func newMockSubscription() *mockSubscription {
	return &mockSubscription{
		errCh:   make(chan error),
		unsubCh: make(chan struct{}),
	}
}

func (s *mockSubscription) Unsubscribe() {
	select {
	case <-s.unsubCh:
	default:
		close(s.unsubCh)
	}
}

func (s *mockSubscription) Err() <-chan error {
	return s.errCh
}

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
	mu          sync.Mutex
	filter      Filter
	processed   []types.Log
	state       []byte
	processErr  error
	snapshotErr error
	restoreErr  error
}

func (m *mockHandler) Snapshot() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state, m.snapshotErr
}

func (m *mockHandler) Restore(state []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state
	return m.restoreErr
}

func (m *mockHandler) Filter() Filter {
	return m.filter
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

type mockStore struct {
	mu    sync.Mutex
	store map[string][]byte
}

func newMockStore() *mockStore {
	return &mockStore{
		store: make(map[string][]byte),
	}
}

func (m *mockStore) Load(name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	val, ok := m.store[name]
	if !ok {
		return nil, nil
	}
	return val, nil
}

func (m *mockStore) Save(name string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store[name] = data
	return nil
}

func (m *mockStore) Delete(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.store, name)
	return nil
}
