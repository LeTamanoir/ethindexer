package ethindex

import (
	"context"
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

var ErrReorg = errors.New("chain reorg detected")

type Filter struct {
	FromBlock uint64
	Addresses []common.Address
	Topics    [][]common.Hash
}

type Handler interface {
	Snapshot() ([]byte, error)
	Restore([]byte) error
	Filter() Filter
	Process(ctx context.Context, log *types.Log) error
}

type Cache interface {
	Load(name string, out any) (ok bool, err error)
	Save(name string, v any) error
	Delete(name string) error
}
