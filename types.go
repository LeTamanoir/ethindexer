package ethindex

import (
	"context"
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type Filter struct {
	FromBlock uint64
	Addresses []common.Address
	Topics    [][]common.Hash
}

type Handler interface {
	Snapshot(context.Context) ([]byte, error)
	Restore(context.Context, []byte) error
	Filter() Filter
	Process(context.Context, []types.Log) error
}

type Client interface {
	ethereum.LogFilterer
	ethereum.ChainReader
}

type Config struct {
	NewHeadsBuffer int
	MaxBlockRange  uint64
	FinalityDepth  uint64
	MaxBackoff     time.Duration
	RetryFunc      func(err error, attempt int) bool
	Logger         *slog.Logger
}

type Store interface {
	Load(key string) ([]byte, error)
	Save(key string, data []byte) error
	Delete(key string) error
}
