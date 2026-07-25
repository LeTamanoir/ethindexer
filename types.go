package ethindexer

import (
	"bytes"
	"context"
	"encoding/gob"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// checkpoint stores application state at a specific chain head.
type checkpoint struct {
	Head  blockRef
	State any
}

func (c checkpoint) GobEncode() ([]byte, error) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	if err := enc.Encode(c.Head); err != nil {
		return nil, err
	}
	if err := enc.Encode(c.State); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (c *checkpoint) GobDecode(data []byte) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&c.Head); err != nil {
		return err
	}
	return dec.Decode(c.State)
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
