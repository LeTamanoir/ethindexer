package ethindex

import (
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// ErrReorg is returned when a chain reorganization is detected during indexing.
var ErrReorg = errors.New("chain reorg detected")

// Filter defines the criteria used to select Ethereum logs during indexing.
type Filter struct {
	// FromBlock specifies the starting block number for the backfill phase.
	FromBlock uint64

	// Addresses restricts log collection to the given contract addresses.
	Addresses []common.Address

	// Topics filters logs by their indexed event topics.
	Topics [][]common.Hash
}

// Handler is implemented by the caller to define indexing logic for a specific
// set of Ethereum events. The indexer calls its methods to determine which logs
// to collect, how to process them, and how to persist and restore state.
type Handler interface {
	// Snapshot serializes the current handler state into a byte slice so it
	// can be persisted as a checkpoint by the indexer.
	Snapshot(context.Context) ([]byte, error)

	// Restore deserializes a previously saved snapshot and restores the
	// handler state to the checkpointed point in time.
	Restore(context.Context, []byte) error

	// Filter returns the log filter criteria that the indexer uses to
	// select relevant events from the chain.
	Filter() Filter

	// Process is called for each matching log in block order. It receives the
	// log and a context that is cancelled when the indexer is shutting down.
	Process(context.Context, types.Log) error
}

// Cache manages persistence for the indexer. It is used to save and restore
// the indexer state across restarts so that backfilling can resume from the
// last known position, as well as to cache expensive RPC calls.
type Cache interface {
	// Load retrieves the value stored under name and unmarshals it into out.
	// It returns ok = false if no value exists for that name.
	Load(name string, out any) (ok bool, err error)

	// Save persists v under the given name, overwriting any existing value.
	Save(name string, v any) error

	// Delete removes the entry stored under name from the cache.
	Delete(name string) error
}

// RPCClient defines the methods the indexer requires from an Ethereum RPC client.
type RPCClient interface {
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
	SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error)
}
