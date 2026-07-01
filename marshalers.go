package ethindexer

import (
	"encoding"
	"encoding/binary"
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// checkpoint stores handler state at a specific chain head.
type checkpoint struct {
	head  blockRef
	state []byte
}

// logs is a slice of Ethereum logs that supports binary marshaling.
type logs []types.Log

var (
	errInvalidLogs       = errors.New("invalid logs")
	errInvalidCheckpoint = errors.New("invalid checkpoint")
)

var (
	_ encoding.BinaryMarshaler = (*logs)(nil)
	_ encoding.BinaryMarshaler = (*checkpoint)(nil)

	_ encoding.BinaryUnmarshaler = (*logs)(nil)
	_ encoding.BinaryUnmarshaler = (*checkpoint)(nil)
)

func (c checkpoint) MarshalBinary() ([]byte, error) {
	b := make([]byte, 0, 8+common.HashLength+len(c.state))

	b = binary.LittleEndian.AppendUint64(b, c.head.Number)
	b = append(b, c.head.Hash[:]...)
	b = append(b, c.state...)

	return b, nil
}

func (c *checkpoint) UnmarshalBinary(b []byte) error {
	if len(b) < 8+common.HashLength {
		return errInvalidCheckpoint
	}

	c.head.Number = binary.LittleEndian.Uint64(b)
	c.head.Hash.SetBytes(b[8 : 8+common.HashLength])
	c.state = append(c.state, b[8+common.HashLength:]...)

	return nil
}

func (ls logs) MarshalBinary() ([]byte, error) {
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

func (ls *logs) UnmarshalBinary(b []byte) error {
	if len(b) < 8 {
		return errInvalidLogs
	}
	logsLen := int(binary.LittleEndian.Uint64(b[:8]))
	b = b[8:]

	*ls = make(logs, logsLen)

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
