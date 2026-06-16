package ethindex

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type Logs []types.Log

func (ls Logs) MarshalBinary() ([]byte, error) {
	b := make([]byte, 0, logsLen(ls))
	for _, l := range ls {
		b = appendLog(b, l)
	}
	return b, nil
}

func (ls *Logs) UnmarshalBinary(b []byte) (err error) {
	for len(b) > 0 {
		var l types.Log
		b, err = unmarshalLog(b, &l)
		if err != nil {
			return
		}
		*ls = append(*ls, l)
	}

	return nil
}

func logsLen(ls Logs) int {
	l := 0
	for i := range ls {
		l += (0 +
			/* Address */ common.AddressLength +
			/* Topics */ 8 + len(ls[i].Topics)*common.HashLength +
			/* Data */ 8 + len(ls[i].Data) +
			/* BlockNumber */ 8 +
			/* TxHash */ common.HashLength +
			/* TxIndex */ 8 +
			/* BlockHash */ common.HashLength +
			/* BlockTimestamp */ 8 +
			/* Index */ 8)
	}
	return l
}

func appendLog(b []byte, l types.Log) []byte {
	b = append(b, l.Address.Bytes()...)

	b = binary.LittleEndian.AppendUint64(b, uint64(len(l.Topics)))
	for i := range l.Topics {
		b = append(b, l.Topics[i].Bytes()...)
	}

	b = binary.LittleEndian.AppendUint64(b, uint64(len(l.Data)))
	b = append(b, l.Data...)

	b = binary.LittleEndian.AppendUint64(b, l.BlockNumber)

	b = append(b, l.TxHash.Bytes()...)

	b = binary.LittleEndian.AppendUint64(b, uint64(l.TxIndex))

	b = append(b, l.BlockHash.Bytes()...)

	b = binary.LittleEndian.AppendUint64(b, uint64(l.BlockTimestamp))
	b = binary.LittleEndian.AppendUint64(b, uint64(l.Index))

	return b
}

func unmarshalLog(b []byte, l *types.Log) (out []byte, err error) {
	b, err = decodeAddress(b, &l.Address)
	if err != nil {
		return
	}

	var topicsLen uint64
	b, err = decodeUint64(b, &topicsLen)
	if err != nil {
		return
	}

	l.Topics = make([]common.Hash, topicsLen)
	for i := range l.Topics {
		b, err = decodeHash(b, &l.Topics[i])
		if err != nil {
			return
		}
	}

	b, err = decodeBytes(b, &l.Data)
	if err != nil {
		return
	}

	b, err = decodeUint64(b, &l.BlockNumber)
	if err != nil {
		return
	}

	b, err = decodeHash(b, &l.TxHash)
	if err != nil {
		return
	}

	var txIndex uint64
	b, err = decodeUint64(b, &txIndex)
	if err != nil {
		return
	}
	l.TxIndex = uint(txIndex)

	b, err = decodeHash(b, &l.BlockHash)
	if err != nil {
		return
	}

	b, err = decodeUint64(b, &l.BlockTimestamp)
	if err != nil {
		return
	}

	var index uint64
	b, err = decodeUint64(b, &index)
	if err != nil {
		return
	}
	l.Index = uint(index)

	return b, nil
}
