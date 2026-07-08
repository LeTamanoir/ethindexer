package ethindexer

import (
	"encoding/binary"
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

var (
	errInvalidLogs       = errors.New("invalid logs")
	errInvalidCheckpoint = errors.New("invalid checkpoint")
	errInvalidVersion    = errors.New("invalid format version")
)

const (
	logsVersion       = 1
	checkpointVersion = 1
)

func marshalCheckpoint(c checkpoint) ([]byte, error) {
	b := make([]byte, 0, 1+8+common.HashLength+len(c.state))
	b = append(b, checkpointVersion)
	b = binary.LittleEndian.AppendUint64(b, c.head.number)
	b = append(b, c.head.hash[:]...)
	b = append(b, c.state...)
	return b, nil
}

func unmarshalCheckpoint(b []byte) (checkpoint, error) {
	if len(b) == 0 {
		return checkpoint{}, errInvalidCheckpoint
	}
	if b[0] != checkpointVersion {
		return checkpoint{}, errInvalidVersion
	}
	b = b[1:]

	if len(b) < 8+common.HashLength {
		return checkpoint{}, errInvalidCheckpoint
	}

	return checkpoint{
		head: blockRef{
			number: binary.LittleEndian.Uint64(b),
			hash:   common.Hash(b[8 : 8+common.HashLength]),
		},
		state: append([]byte(nil), b[8+common.HashLength:]...),
	}, nil
}

func marshalLogs(logs []types.Log) ([]byte, error) {
	size := 0
	for _, l := range logs {
		size += logSize(l)
	}

	b := make([]byte, 0, 1+8+size)
	b = append(b, logsVersion)
	b = binary.LittleEndian.AppendUint64(b, uint64(len(logs)))
	for _, l := range logs {
		b = appendLog(b, l)
	}

	return b, nil
}

func unmarshalLogs(b []byte) ([]types.Log, error) {
	if len(b) == 0 {
		return nil, errInvalidLogs
	}
	if b[0] != logsVersion {
		return nil, errInvalidVersion
	}
	b = b[1:]

	if len(b) < 8 {
		return nil, errInvalidLogs
	}
	logsLen := int(binary.LittleEndian.Uint64(b[:8]))
	b = b[8:]

	logs := make([]types.Log, logsLen)

	for i := range logsLen {
		var l types.Log
		l, b = unmarshalLog(b)
		if b == nil {
			return nil, errInvalidLogs
		}
		logs[i] = l
	}

	if len(b) != 0 {
		return nil, errInvalidLogs
	}

	return logs, nil
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

func unmarshalLog(b []byte) (l types.Log, out []byte) {
	if len(b) < common.AddressLength {
		return
	}
	l.Address.SetBytes(b[:common.AddressLength])
	b = b[common.AddressLength:]

	if len(b) < 8 {
		return
	}
	topicsLen := int(binary.LittleEndian.Uint64(b[:8]))
	b = b[8:]

	if len(b) < topicsLen*common.HashLength {
		return
	}
	l.Topics = make([]common.Hash, topicsLen)
	for i := range l.Topics {
		l.Topics[i].SetBytes(b[:common.HashLength])
		b = b[common.HashLength:]
	}

	if len(b) < 8 {
		return
	}
	dataLen := int(binary.LittleEndian.Uint64(b[:8]))
	b = b[8:]

	if len(b) < dataLen {
		return
	}
	l.Data = make([]byte, dataLen)
	copy(l.Data, b[:dataLen])
	b = b[dataLen:]

	if len(b) < 8 {
		return
	}
	l.BlockNumber = binary.LittleEndian.Uint64(b[:8])
	b = b[8:]

	if len(b) < common.HashLength {
		return
	}
	l.TxHash.SetBytes(b[:common.HashLength])
	b = b[common.HashLength:]

	if len(b) < 8 {
		return
	}
	l.TxIndex = uint(binary.LittleEndian.Uint64(b[:8]))
	b = b[8:]

	if len(b) < common.HashLength {
		return
	}
	l.BlockHash.SetBytes(b[:common.HashLength])
	b = b[common.HashLength:]

	if len(b) < 8 {
		return
	}
	l.BlockTimestamp = uint64(binary.LittleEndian.Uint64(b[:8]))
	b = b[8:]

	if len(b) < 8 {
		return
	}
	l.Index = uint(binary.LittleEndian.Uint64(b[:8]))
	b = b[8:]

	return l, b
}
