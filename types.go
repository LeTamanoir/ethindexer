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

// LogsRangeFunc returns matching logs for the inclusive block range [from, to].
type LogsRangeFunc func(context.Context, Filter, uint64, uint64) ([]types.Log, error)

// Filter specifies which logs the indexer fetches.
type Filter struct {
	// Addresses restrict logs to the given contract addresses.
	// See [ethereum.FilterQuery.Addresses].
	Addresses []common.Address

	// Topics restrict logs by indexed event topics.
	// See [ethereum.FilterQuery.Topics].
	Topics [][]common.Hash
}

// rangeQuery builds a block-range FilterQuery over r.
func (f Filter) rangeQuery(r BlockRange) ethereum.FilterQuery {
	return ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(r.From),
		ToBlock:   new(big.Int).SetUint64(r.To),
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
