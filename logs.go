package ethindex

import (
	"context"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

type Logs []types.Log

var errInvalidLogs = errors.New("invalid logs")

var _ encoding.BinaryMarshaler = (*Logs)(nil)
var _ encoding.BinaryUnmarshaler = (*Logs)(nil)

func (ls Logs) MarshalBinary() ([]byte, error) {
	size := 0
	for _, l := range ls {
		size += logSize(l)
	}

	b := make([]byte, 0, 8+size)

	b = binary.LittleEndian.AppendUint64(b, uint64(len(ls)))
	for _, l := range ls {
		b = appendLog(b, l)
	}

	return b, nil
}

func (ls *Logs) UnmarshalBinary(b []byte) error {
	if len(b) < 8 {
		return errInvalidLogs
	}
	logsLen := int(binary.LittleEndian.Uint64(b[:8]))
	b = b[8:]

	*ls = make(Logs, logsLen)

	var err error
	for i := range *ls {
		b, err = unmarshalLog(b, &(*ls)[i])
		if err != nil {
			return err
		}
	}

	if len(b) != 0 {
		return errInvalidLogs
	}

	return nil
}

func logSize(l types.Log) int {
	return /* Address */ common.AddressLength +
		/* Topics */ 8 + len(l.Topics)*common.HashLength +
		/* Data */ 8 + len(l.Data) +
		/* BlockNumber */ 8 +
		/* TxHash */ common.HashLength +
		/* TxIndex */ 8 +
		/* BlockHash */ common.HashLength +
		/* BlockTimestamp */ 8 +
		/* Index */ 8
}

func appendLog(b []byte, l types.Log) []byte {
	b = append(b, l.Address[:]...)
	b = binary.LittleEndian.AppendUint64(b, uint64(len(l.Topics)))
	for i := range l.Topics {
		b = append(b, l.Topics[i][:]...)
	}
	b = binary.LittleEndian.AppendUint64(b, uint64(len(l.Data)))
	b = append(b, l.Data...)
	b = binary.LittleEndian.AppendUint64(b, uint64(l.BlockNumber))
	b = append(b, l.TxHash[:]...)
	b = binary.LittleEndian.AppendUint64(b, uint64(l.TxIndex))
	b = append(b, l.BlockHash[:]...)
	b = binary.LittleEndian.AppendUint64(b, uint64(l.BlockTimestamp))
	b = binary.LittleEndian.AppendUint64(b, uint64(l.Index))
	return b
}

func unmarshalLog(b []byte, l *types.Log) ([]byte, error) {
	if len(b) < common.AddressLength {
		return nil, errInvalidLogs
	}
	l.Address.SetBytes(b[:common.AddressLength])
	b = b[common.AddressLength:]

	if len(b) < 8 {
		return nil, errInvalidLogs
	}
	topicsLen := int(binary.LittleEndian.Uint64(b[:8]))
	b = b[8:]

	if len(b) < topicsLen*common.HashLength {
		return nil, errInvalidLogs
	}
	l.Topics = make([]common.Hash, topicsLen)
	for i := range l.Topics {
		l.Topics[i].SetBytes(b[:common.HashLength])
		b = b[common.HashLength:]
	}

	if len(b) < 8 {
		return nil, errInvalidLogs
	}
	dataLen := int(binary.LittleEndian.Uint64(b[:8]))
	b = b[8:]

	if len(b) < dataLen {
		return nil, errInvalidLogs
	}
	l.Data = append(l.Data[:0], b[:dataLen]...)
	b = b[dataLen:]

	if len(b) < 8 {
		return nil, errInvalidLogs
	}
	l.BlockNumber = binary.LittleEndian.Uint64(b[:8])
	b = b[8:]

	if len(b) < common.HashLength {
		return nil, errInvalidLogs
	}
	l.TxHash.SetBytes(b[:common.HashLength])
	b = b[common.HashLength:]

	if len(b) < 8 {
		return nil, errInvalidLogs
	}
	l.TxIndex = uint(binary.LittleEndian.Uint64(b[:8]))
	b = b[8:]

	if len(b) < common.HashLength {
		return nil, errInvalidLogs
	}
	l.BlockHash.SetBytes(b[:common.HashLength])
	b = b[common.HashLength:]

	if len(b) < 8 {
		return nil, errInvalidLogs
	}
	l.BlockTimestamp = uint64(binary.LittleEndian.Uint64(b[:8]))
	b = b[8:]

	if len(b) < 8 {
		return nil, errInvalidLogs
	}
	l.Index = uint(binary.LittleEndian.Uint64(b[:8]))
	b = b[8:]

	return b, nil
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
