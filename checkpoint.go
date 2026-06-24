package ethindex

import (
	"context"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

type checkpointKind string

const (
	finalized checkpointKind = "checkpoint-finalized"
	dangling  checkpointKind = "checkpoint-dangling"
)

type checkpoint struct {
	Head  BlockRef
	State []byte
}

var errInvalidCheckpoint = errors.New("invalid checkpoint")

var _ encoding.BinaryUnmarshaler = (*checkpoint)(nil)
var _ encoding.BinaryMarshaler = (*checkpoint)(nil)

func (c checkpoint) MarshalBinary() ([]byte, error) {
	b := make([]byte, 0, 8+common.HashLength+len(c.State))

	b = binary.LittleEndian.AppendUint64(b, c.Head.Number)
	b = append(b, c.Head.Hash[:]...)
	b = append(b, c.State...)

	return b, nil
}

func (c *checkpoint) UnmarshalBinary(b []byte) error {
	if len(b) < 8+common.HashLength {
		return errInvalidCheckpoint
	}

	c.Head.Number = binary.LittleEndian.Uint64(b)
	c.Head.Hash.SetBytes(b[8 : 8+common.HashLength])
	c.State = append(c.State, b[8+common.HashLength:]...)

	return nil
}

func saveCheckpoint(ctx context.Context, s Store, k checkpointKind, cp checkpoint) error {
	cpb, err := cp.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	if err := s.Save(ctx, string(k), cpb); err != nil {
		return fmt.Errorf("store save: %w", err)
	}

	return nil
}

func loadCheckpoint(ctx context.Context, s Store, k checkpointKind) (*checkpoint, bool, error) {
	cpb, err := s.Load(ctx, string(k))
	if err != nil {
		return nil, false, fmt.Errorf("store load: %w", err)
	}
	if len(cpb) == 0 {
		return nil, false, nil
	}

	var cp checkpoint
	if err := cp.UnmarshalBinary(cpb); err != nil {
		return nil, false, fmt.Errorf("unmarshal: %w", err)
	}

	return &cp, true, nil
}
