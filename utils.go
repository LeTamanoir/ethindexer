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

func chunkBlockRange(from, to, size uint64) []struct{ from, to uint64 } {
	var chunks []struct{ from, to uint64 }
	for start := from; start <= to; start += size {
		end := min(start+size-1, to)
		chunks = append(chunks, struct {
			from uint64
			to   uint64
		}{start, end})
	}
	return chunks
}

func headersRange(ctx context.Context, c Client, from, to uint64) ([]*types.Header, error) {
	total := to - from + 1

	heads := make([]*types.Header, total)
	eg, ctx := errgroup.WithContext(ctx)

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
