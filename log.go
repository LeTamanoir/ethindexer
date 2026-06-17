package ethindex

import (
	"encoding/binary"
	"encoding/gob"
	"encoding/json"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
)

//go:generate go run github.com/mailru/easyjson/... $GOFILE

//easyjson:json
type log struct {
	Address        common.Address `json:"address"`
	Topics         []common.Hash  `json:"topics"`
	Data           hexutil.Bytes  `json:"data"`
	BlockNumber    hexutil.Uint64 `json:"blockNumber"`
	TxHash         common.Hash    `json:"transactionHash"`
	TxIndex        hexutil.Uint   `json:"transactionIndex"`
	BlockHash      common.Hash    `json:"blockHash"`
	BlockTimestamp hexutil.Uint64 `json:"blockTimestamp"`
	Index          hexutil.Uint   `json:"logIndex"`
}

var _ json.Marshaler = (*log)(nil)
var _ json.Unmarshaler = (*log)(nil)
var _ gob.GobDecoder = (*log)(nil)
var _ gob.GobEncoder = (*log)(nil)

func (l log) toGethLog() types.Log {
	return types.Log{
		Address:        l.Address,
		Topics:         l.Topics,
		Data:           l.Data,
		BlockNumber:    uint64(l.BlockNumber),
		TxHash:         l.TxHash,
		TxIndex:        uint(l.TxIndex),
		BlockHash:      l.BlockHash,
		BlockTimestamp: uint64(l.BlockTimestamp),
		Index:          uint(l.Index),
	}
}

func (l log) GobEncode() ([]byte, error) {
	b := make([]byte, 0, 0+
		/* Address */ common.AddressLength+
		/* Topics */ 8+len(l.Topics)*common.HashLength+
		/* Data */ 8+len(l.Data)+
		/* BlockNumber */ 8+
		/* TxHash */ common.HashLength+
		/* TxIndex */ 8+
		/* BlockHash */ common.HashLength+
		/* BlockTimestamp */ 8+
		/* Index */ 8)

	b = append(b, l.Address.Bytes()...)

	b = binary.LittleEndian.AppendUint64(b, uint64(len(l.Topics)))
	for i := range l.Topics {
		b = append(b, l.Topics[i].Bytes()...)
	}

	b = binary.LittleEndian.AppendUint64(b, uint64(len(l.Data)))
	b = append(b, l.Data...)

	b = binary.LittleEndian.AppendUint64(b, uint64(l.BlockNumber))

	b = append(b, l.TxHash.Bytes()...)

	b = binary.LittleEndian.AppendUint64(b, uint64(l.TxIndex))

	b = append(b, l.BlockHash.Bytes()...)

	b = binary.LittleEndian.AppendUint64(b, uint64(l.BlockTimestamp))

	b = binary.LittleEndian.AppendUint64(b, uint64(l.Index))

	return b, nil
}

func (l *log) GobDecode(b []byte) (err error) {
	b, err = decodeAddress(b, &l.Address)
	if err != nil {
		return
	}

	var topicsLen hexutil.Uint64
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

	b, err = decodeUint(b, &l.TxIndex)
	if err != nil {
		return
	}

	b, err = decodeHash(b, &l.BlockHash)
	if err != nil {
		return
	}

	b, err = decodeUint64(b, &l.BlockTimestamp)
	if err != nil {
		return
	}

	b, err = decodeUint(b, &l.Index)
	if err != nil {
		return
	}

	return nil
}
