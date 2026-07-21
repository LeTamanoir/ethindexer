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

	// Store persists checkpoints and cached log batches.
	Store BlobStore

	// Filter specifies which logs the indexer fetches.
	Filter Filter

	// InitFunc optionally initializes application state on a fresh start.
	InitFunc func(context.Context, ChainReader) error

	// ProcessFunc applies matching logs in block order.
	ProcessFunc func(context.Context, []types.Log) error

	// SnapshotFunc returns the current application state.
	SnapshotFunc func(context.Context) ([]byte, error)

	// RestoreFunc restores previously captured application state.
	RestoreFunc func(context.Context, []byte) error

	// LogFunc receives indexer log events.
	LogFunc func(msg string, args ...any)

	// MaxBlockRange is the maximum block span per backfill request.
	MaxBlockRange uint64

	// FinalityDepth is the block depth considered finalized.
	FinalityDepth uint64

	// CheckpointInterval is the minimum number of blocks between staged checkpoints.
	CheckpointInterval uint64

	// MaxConcurrency bounds concurrent header fetches.
	MaxConcurrency int
}

func (o *Options) applyDefaults() {
	if o.LogFunc == nil {
		o.LogFunc = func(string, ...any) {}
	}
	if o.MaxBlockRange == 0 {
		o.MaxBlockRange = 10_000
	}
	if o.FinalityDepth == 0 {
		o.FinalityDepth = 64
	}
	if o.CheckpointInterval == 0 {
		o.CheckpointInterval = 10_000
	}
	if o.MaxConcurrency == 0 {
		o.MaxConcurrency = 16
	}
}
