package ethindexer

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// checkpoint stores application state at a specific chain head.
type checkpoint[T any] struct {
	Head  blockRef
	State T
}

// blockRef is a (number, hash) pair identifying a block.
type blockRef struct {
	Number uint64
	Hash   common.Hash
}

// ChainReader provides access to Ethereum logs and block headers.
type ChainReader interface {
	FilterLogs(context.Context, ethereum.FilterQuery) ([]types.Log, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
}

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

// BlockRange is an inclusive block range.
type BlockRange struct {
	From uint64
	To   uint64
}

// ChunkBlockRange splits the inclusive block range [from, to] into ranges
// containing at most size blocks.
func ChunkBlockRange(from, to, size uint64) []BlockRange {
	if size == 0 {
		panic("invalid block range size: 0")
	}
	var chunks []BlockRange
	for start := from; start <= to; {
		end := to
		if size-1 < to-start {
			end = start + size - 1
		}
		chunks = append(chunks, BlockRange{From: start, To: end})
		if end == to {
			break
		}
		start = end + 1
	}
	return chunks
}
