package ethindex

import (
	"encoding"
	"encoding/binary"
	"errors"

	"github.com/ethereum/go-ethereum/common"
)

type checkpoint struct {
	head  blockRef
	state []byte
}

var errInvalidCheckpoint = errors.New("invalid checkpoint")

var _ encoding.BinaryUnmarshaler = (*checkpoint)(nil)
var _ encoding.BinaryMarshaler = (*checkpoint)(nil)

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
