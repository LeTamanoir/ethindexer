package ethindex

import (
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

func loadCachedLogs(s Store, q ethereum.FilterQuery) ([]types.Log, error) {
	b, err := s.Load(logsKey(q))
	if err != nil {
		return nil, fmt.Errorf("load logs: %w", err)
	}
	if len(b) == 0 {
		return nil, nil
	}

	var logs Logs
	if err := logs.UnmarshalBinary(b); err != nil {
		return nil, fmt.Errorf("unmarshal logs: %w", err)
	}
	return logs, nil
}

func saveCachedLogs(s Store, q ethereum.FilterQuery, logs []types.Log) error {
	b, err := Logs(logs).MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal logs: %w", err)
	}
	if err := s.Save(logsKey(q), b); err != nil {
		return fmt.Errorf("save logs: %w", err)
	}
	return nil
}
