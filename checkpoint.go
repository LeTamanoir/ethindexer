package ethindex

import (
	"encoding"
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

type checkpoint struct {
	BlockNumber uint64
	BlockHash   common.Hash
	State       []byte
}

var errInvalidCheckpoint = fmt.Errorf("invalid checkpoint")

var _ encoding.BinaryUnmarshaler = (*checkpoint)(nil)
var _ encoding.BinaryMarshaler = (*checkpoint)(nil)

func (c checkpoint) MarshalBinary() ([]byte, error) {
	b := make([]byte, 0, 8+common.HashLength+len(c.State))

	b = binary.LittleEndian.AppendUint64(b, c.BlockNumber)
	b = append(b, c.BlockHash[:]...)
	b = append(b, c.State...)

	return b, nil
}

func (c *checkpoint) UnmarshalBinary(b []byte) error {
	if len(b) < 8+common.HashLength {
		return errInvalidCheckpoint
	}

	c.BlockNumber = binary.LittleEndian.Uint64(b)
	c.BlockHash.SetBytes(b[8 : 8+common.HashLength])
	c.State = append(c.State, b[8+common.HashLength:]...)

	return nil
}
