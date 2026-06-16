package ethindex

import (
	"encoding/binary"
	"encoding/gob"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type logs []types.Log

var _ gob.GobDecoder = (*logs)(nil)
var _ gob.GobEncoder = (*logs)(nil)

func (ls logs) GobEncode() ([]byte, error) {
	b := make([]byte, 0, logsSize(ls))

	b = binary.LittleEndian.AppendUint64(b, uint64(len(ls)))

	for _, l := range ls {
		b = appendLog(b, l)
	}

	return b, nil
}

func (ls *logs) GobDecode(b []byte) (err error) {
	var n uint64
	b, err = decodeUint64(b, &n)
	if err != nil {
		return
	}

	*ls = make(logs, n)
	for i := range n {
		b, err = unmarshalLog(b, &(*ls)[i])
		if err != nil {
			return
		}
	}

	return nil
}

func logsSize(ls logs) int {
	s := 8 // Slice len
	for _, l := range ls {
		s += /* Address */ common.AddressLength +
			/* Topics */ 8 + len(l.Topics)*common.HashLength +
			/* Data */ 8 + len(l.Data) +
			/* BlockNumber */ 8 +
			/* TxHash */ common.HashLength +
			/* TxIndex */ 8 +
			/* BlockHash */ common.HashLength +
			/* BlockTimestamp */ 8 +
			/* Index */ 8
	}
	return s
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
