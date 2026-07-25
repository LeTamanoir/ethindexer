package ethindexer

import (
	"context"
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
	filter     Filter
	mu         sync.Mutex
	processed  []types.Log
	state      []byte
	processErr error
}

type plainState struct {
	Value uint64
}

func (s *plainState) Process(_ context.Context, logs []types.Log) error {
	s.Value += uint64(len(logs))
	return nil
}

func (m *mockHandler) Filter() Filter {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.filter
}

func (m *mockHandler) GobEncode() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.state...), nil
}

func (m *mockHandler) GobDecode(state []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = append(m.state[:0], state...)
	return nil
}

func (m *mockHandler) Process(_ context.Context, logs []types.Log) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.processErr != nil {
		return m.processErr
	}
	m.processed = append(m.processed, logs...)
	return nil
}

func indexerForHandler(client ChainReader, handler *mockHandler, dataDir string, fromBlock uint64) *Indexer {
	return &Indexer{
		Client:    client,
		DataDir:   dataDir,
		FromBlock: fromBlock,
		Filter:    handler.Filter(),
		State:     handler,
	}
}
