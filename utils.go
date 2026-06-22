package ethindex

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
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

func logsKey(q ethereum.FilterQuery) string {
	var parts [][]byte

	for _, a := range q.Addresses {
		parts = append(parts, a[:])
	}
	for _, tt := range q.Topics {
		for _, t := range tt {
			parts = append(parts, t[:])
		}
	}

	hash := crypto.Keccak256Hash(parts...)

	return fmt.Sprintf("logs-%d-%d-%s", q.FromBlock, q.ToBlock, hash)
}

func cachedFilterLogs(ctx context.Context, c Client, s Store, q ethereum.FilterQuery) ([]types.Log, error) {
	cached, err := loadLogs(ctx, s, q)
	if err != nil {
		return nil, fmt.Errorf("load logs: %w", err)
	}
	if cached != nil {
		return cached, nil
	}

	logs, err := c.FilterLogs(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("filter logs: %w", err)
	}

	if err := saveLogs(ctx, s, q, logs); err != nil {
		return nil, fmt.Errorf("save logs: %w", err)
	}

	return logs, nil
}

func loadLogs(ctx context.Context, s Store, q ethereum.FilterQuery) ([]types.Log, error) {
	b, err := s.Load(ctx, logsKey(q))
	if err != nil {
		return nil, fmt.Errorf("store load: %w", err)
	}
	if len(b) == 0 {
		return nil, nil
	}

	var logs Logs
	if err := logs.UnmarshalBinary(b); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return logs, nil
}

func saveLogs(ctx context.Context, s Store, q ethereum.FilterQuery, logs []types.Log) error {
	b, err := Logs(logs).MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := s.Save(ctx, logsKey(q), b); err != nil {
		return fmt.Errorf("store save: %w", err)
	}
	return nil
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
