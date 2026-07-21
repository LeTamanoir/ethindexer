package ethindexer

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
)

// CachedClient provides chain reads and directory-backed caching for log ranges.
type CachedClient struct {
	client  ChainReader
	dataDir string
}

var _ ChainReader = (*CachedClient)(nil)

// FilterLogs returns logs matching q, caching bounded block-range queries.
func (c *CachedClient) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	if q.BlockHash != nil || q.FromBlock == nil || q.ToBlock == nil {
		return c.client.FilterLogs(ctx, q)
	}

	key := logsCacheKey(q)

	bin, err := readBlob(c.dataDir, key)
	if err != nil {
		return nil, fmt.Errorf("read cache: %w", err)
	}
	if len(bin) > 0 {
		logs, err := unmarshalLogs(bin)
		if err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}
		return logs, nil
	}

	logs, err := c.client.FilterLogs(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("filter logs: %w", err)
	}

	bin, err = marshalLogs(logs)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	if err := writeBlob(c.dataDir, key, bin); err != nil {
		return nil, fmt.Errorf("write cache: %w", err)
	}

	return logs, nil
}

// HeaderByNumber delegates a header query to the underlying chain reader.
func (c *CachedClient) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	return c.client.HeaderByNumber(ctx, number)
}

func logsCacheKey(q ethereum.FilterQuery) string {
	if q.BlockHash != nil || q.ToBlock == nil || q.FromBlock == nil {
		panic("logs cache key requires a range query")
	}

	var b []byte

	b = binary.LittleEndian.AppendUint64(b, uint64(len(q.Addresses)))
	for _, a := range q.Addresses {
		b = append(b, a[:]...)
	}
	b = binary.LittleEndian.AppendUint64(b, uint64(len(q.Topics)))
	for _, tt := range q.Topics {
		b = binary.LittleEndian.AppendUint64(b, uint64(len(tt)))
		for _, t := range tt {
			b = append(b, t[:]...)
		}
	}

	hash := sha256.Sum256(b)

	return fmt.Sprintf("logs-%d-%d-%s", q.FromBlock, q.ToBlock, hex.EncodeToString(hash[:]))
}
