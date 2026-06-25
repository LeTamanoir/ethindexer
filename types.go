package ethindex

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// BlockRef is a (number, hash) pair identifying a block.
type BlockRef struct {
	Number uint64
	Hash   common.Hash
}

// Handler defines the application-specific indexing logic.
type Handler interface {
	// Snapshot returns the current handler state.
	Snapshot(context.Context) ([]byte, error)

	// Restore restores a previously captured state.
	Restore(context.Context, []byte) error

	// Process applies matching logs in block order.
	Process(context.Context, []types.Log) error
}

// Client provides access to Ethereum logs and block headers.
type Client interface {
	FilterLogs(context.Context, ethereum.FilterQuery) ([]types.Log, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
}

// Logger records operational messages.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// Store provides keyed byte storage.
type Store interface {
	// Read returns the data stored under key. A missing key returns (nil, nil).
	Read(ctx context.Context, key string) ([]byte, error)

	// Write stores data under key, replacing any existing value.
	Write(ctx context.Context, key string, blob []byte) error

	// Move atomically transfers data from srcKey to dstKey, replacing any
	// existing value under dstKey.
	Move(ctx context.Context, srcKey, dstKey string) error
}
