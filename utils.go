package ethindex

import (
	"context"
	"io"
	"math/big"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"golang.org/x/sync/errgroup"
)

func atomicWrite(filename string, write func(io.Writer) error) error {
	dir := filepath.Dir(filename)

	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}()

	if err := write(f); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return os.Rename(f.Name(), filename)
}

func newFilterQuery(f Filter, from, to uint64) ethereum.FilterQuery {
	return ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(to),
		Addresses: f.Addresses,
		Topics:    f.Topics,
	}
}

type blockRange struct {
	from uint64
	to   uint64
}

func chunkBlockRange(from, to, size uint64) []blockRange {
	var chunks []blockRange
	for start := from; start <= to; start += size {
		end := min(start+size-1, to)
		chunks = append(chunks, blockRange{start, end})
	}
	return chunks
}

func headersRange(ctx context.Context, c Client, from, to uint64, maxConcurrency int) ([]*types.Header, error) {
	total := to - from + 1

	heads := make([]*types.Header, total)
	eg, ctx := errgroup.WithContext(ctx)

	eg.SetLimit(maxConcurrency)

	for i := range total {
		eg.Go(func() error {
			h, e := c.HeaderByNumber(ctx, big.NewInt(int64(from+i)))
			heads[i] = h
			return e
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return heads, nil
}

type noopLogger struct{}

var _ Logger = (*noopLogger)(nil)

func (noopLogger) Debug(string, ...any) {}
func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}
