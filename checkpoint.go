package ethindex

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/common"
)

type blockHeader struct {
	Number uint64
	Hash   common.Hash
}

type checkpoint struct {
	Header blockHeader
	State  []byte
}

func (c checkpoint) MarshalBinary() ([]byte, error) {
	b := make([]byte, 0, 0+
		/* Header.Number */ 8+
		/* Header.Hash */ common.HashLength+
		/* State */ len(c.State))

	b = binary.LittleEndian.AppendUint64(b, c.Header.Number)
	b = append(b, c.Header.Hash.Bytes()...)
	b = append(b, c.State...)

	return b, nil
}

func (c *checkpoint) UnmarshalBinary(b []byte) (err error) {
	b, err = decodeUint64(b, &c.Header.Number)
	if err != nil {
		return
	}

	b, err = decodeHash(b, &c.Header.Hash)
	if err != nil {
		return
	}

	c.State = make([]byte, len(b))
	copy(c.State, b)

	return nil
}
