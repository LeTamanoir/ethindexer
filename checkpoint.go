package ethindex

import (
	"encoding/binary"
	"encoding/gob"

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

var _ gob.GobDecoder = (*checkpoint)(nil)
var _ gob.GobEncoder = (*checkpoint)(nil)

func (c checkpoint) GobEncode() ([]byte, error) {
	b := make([]byte, 0, 0+
		/* Header.Number */ 8+
		/* Header.Hash */ common.HashLength+
		/* State */ len(c.State))

	b = binary.LittleEndian.AppendUint64(b, c.Header.Number)
	b = append(b, c.Header.Hash.Bytes()...)
	b = append(b, c.State...)

	return b, nil
}

func (c *checkpoint) GobDecode(b []byte) (err error) {
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
