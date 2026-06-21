package ethindex

import (
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// ErrReorg is returned when the chain reorgs during indexing.
var ErrReorg = errors.New("chain reorged")

// Filter specifies the Ethereum logs to fetch during indexing.
type Filter struct {
	// FromBlock is the first block included in the initial backfill.
	FromBlock uint64

	// Addresses restricts log collection to the given contract addresses.
	// See [ethereum.FilterQuery.Addresses] for more details.
	Addresses []common.Address

	// Topics filters logs by their indexed event topics.
	// See [ethereum.FilterQuery.Topics] for more details.
	Topics [][]common.Hash
}

// Handler defines the application-specific indexing logic.
type Handler interface {
	// Snapshot returns the current handler state for checkpointing.
	Snapshot() ([]byte, error)

	// Restore restores the handler state from a checkpoint snapshot.
	Restore([]byte) error

	// Filter returns the log filter used during indexing.
	Filter() Filter

	// Process processes matching logs in block order.
	Process(context.Context, []types.Log) error
}

// Client defines the Ethereum RPC methods required by the indexer.
type Client interface {
	FilterLogs(context.Context, ethereum.FilterQuery) ([]types.Log, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
}

// Config configures the indexer.
type Config struct {
	// MaxBlockRange is the maximum block span per backfill RPC call.
	// The default is 10,000.
	MaxBlockRange uint64

	// FinalityDepth is the block depth considered finalized.
	// The default is 64.
	FinalityDepth uint64
}

// Store defines the persistence methods used by the indexer.
type Store interface {
	// Load returns the data stored under key.
	Load(key string) ([]byte, error)

	// Save stores data under key, replacing any existing value.
	Save(key string, data []byte) error

	// Delete removes the data stored under key.
	Delete(key string) error
}
