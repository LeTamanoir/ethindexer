package ethindexer

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// checkpoint stores handler state at a specific chain head.
type checkpoint struct {
	head  blockRef
	state []byte
}

// blockRef is a (number, hash) pair identifying a block.
type blockRef struct {
	number uint64
	hash   common.Hash
}

// Handler defines the application-specific indexing logic.
type Handler interface {
	// Filter specifies which logs the indexer fetches.
	Filter() Filter

	// Snapshot returns the current handler state.
	Snapshot(context.Context) ([]byte, error)

	// Restore restores a previously captured state.
	Restore(context.Context, []byte) error

	// Process applies matching logs in block order.
	Process(context.Context, []types.Log) error
}

// ChainReader provides access to Ethereum logs and block headers.
type ChainReader interface {
	FilterLogs(context.Context, ethereum.FilterQuery) ([]types.Log, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
}

// BlobStore provides keyed byte storage.
type BlobStore interface {
	// Read returns the data stored under key. A missing key returns (nil, nil).
	Read(ctx context.Context, key string) ([]byte, error)

	// Write stores data under key, replacing any existing value.
	Write(ctx context.Context, key string, blob []byte) error

	// Move atomically transfers data from srcKey to dstKey, replacing any
	// existing value under dstKey.
	Move(ctx context.Context, srcKey, dstKey string) error
}

// Filter specifies which logs the indexer fetches.
type Filter struct {
	// FromBlock is the first block to index.
	FromBlock uint64

	// Addresses restrict logs to the given contract addresses.
	// See [ethereum.FilterQuery.Addresses].
	Addresses []common.Address

	// Topics restrict logs by indexed event topics.
	// See [ethereum.FilterQuery.Topics].
	Topics [][]common.Hash
}

// rangeQuery builds a block-range FilterQuery over [from, to].
func (f Filter) rangeQuery(from, to uint64) ethereum.FilterQuery {
	return ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(to),
		Addresses: f.Addresses,
		Topics:    f.Topics,
	}
}

// blockQuery builds a single-block FilterQuery anchored to hash.
func (f Filter) blockQuery(hash common.Hash) ethereum.FilterQuery {
	return ethereum.FilterQuery{
		BlockHash: &hash,
		Addresses: f.Addresses,
		Topics:    f.Topics,
	}
}

// Options configures an Indexer.
type Options struct {
	// Client provides access to Ethereum logs and block headers.
	Client ChainReader

	// Handler receives logs and owns the indexed state.
	Handler Handler

	// Store persists checkpoints and cached log batches.
	Store BlobStore

	// LogFunc receives indexer log events.
	LogFunc func(msg string, args ...any)

	Config Config
}

// Config holds the indexer's tunables.
type Config struct {
	// MaxBlockRange is the maximum block span per backfill request.
	MaxBlockRange uint64

	// FinalityDepth is the block depth considered finalized.
	FinalityDepth uint64

	// CheckpointInterval is the minimum number of blocks between staged checkpoints.
	CheckpointInterval uint64

	// MaxConcurrency bounds concurrent header fetches.
	MaxConcurrency int
}

func (c *Config) applyDefaults() {
	if c.MaxBlockRange == 0 {
		c.MaxBlockRange = 10_000
	}
	if c.FinalityDepth == 0 {
		c.FinalityDepth = 64
	}
	if c.CheckpointInterval == 0 {
		c.CheckpointInterval = 10_000
	}
	if c.MaxConcurrency == 0 {
		c.MaxConcurrency = 16
	}
}
