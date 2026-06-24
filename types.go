package ethindex

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

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
	Snapshot(context.Context) ([]byte, error)

	// Restore restores the handler state from a checkpoint snapshot.
	Restore(context.Context, []byte) error

	// Process processes matching logs in block order.
	Process(context.Context, []types.Log) error
}

// Client defines the Ethereum RPC methods required by the indexer.
type Client interface {
	FilterLogs(context.Context, ethereum.FilterQuery) ([]types.Log, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
}

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// Config configures the indexer.
type Config struct {
	// Client is the Ethereum RPC client used to fetch logs and headers.
	Client Client

	// Logger records operational messages.
	Logger Logger

	// Handler processes matching logs and manages checkpoint state.
	Handler Handler

	// Filter specifies which logs the indexer fetches from the client.
	Filter Filter

	// Store persists checkpoints and handler state.
	Store Store

	// MaxBlockRange is the maximum block span per backfill RPC call.
	// The default is 10,000.
	MaxBlockRange uint64

	// FinalityDepth is the block depth considered finalized.
	// The default is 64.
	FinalityDepth uint64

	// MaxConcurrency bounds the number of concurrent RPC calls when
	// fetching missing headers after a gap (e.g. on reconnect).
	// The default is 16.
	MaxConcurrency int
}

func (c *Config) Validate() error {
	if c.Client == nil {
		return fmt.Errorf("client is required")
	}
	if c.Store == nil {
		return fmt.Errorf("store is required")
	}
	if c.Handler == nil {
		return fmt.Errorf("handler is required")
	}

	// Apply defaults
	if c.FinalityDepth == 0 {
		c.FinalityDepth = 64
	}
	if c.MaxBlockRange == 0 {
		c.MaxBlockRange = 10_000
	}
	if c.MaxConcurrency == 0 {
		c.MaxConcurrency = 16
	}
	if c.Logger == nil {
		c.Logger = noopLogger{}
	}

	return nil
}

// Store defines the persistence methods used by the indexer.
type Store interface {
	// Load returns the data stored under key.
	Load(ctx context.Context, key string) ([]byte, error)

	// Save stores data under key, replacing any existing value.
	Save(ctx context.Context, key string, data []byte) error

	// Move atomically renames the data from srcKey to dstKey, replacing any
	// existing value under dstKey. Implementations should avoid
	// re-serializing the data; a filesystem rename is the canonical example.
	// It is used to promote a dangling checkpoint to finalized.
	Move(ctx context.Context, srcKey, dstKey string) error
}
