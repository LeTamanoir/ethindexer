package ethindex

import (
	"context"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

const (
	finalizedCheckpoint = "checkpoint-finalized"
	danglingCheckpoint  = "checkpoint-dangling"
)

type checkpoint struct {
	Head  BlockRef
	State []byte
}

var errInvalidCheckpoint = fmt.Errorf("invalid checkpoint")

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

func promoteDangling(ctx context.Context, s Store) error {
	cpb, err := s.Load(ctx, danglingCheckpoint)
	if err != nil {
		return fmt.Errorf("store load: %w", err)
	}
	if len(cpb) == 0 {
		return errors.New("dangling checkpoint missing from store")
	}

	var cp checkpoint
	if err := cp.UnmarshalBinary(cpb); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	if err := s.Save(ctx, finalizedCheckpoint, cpb); err != nil {
		return fmt.Errorf("store save: %w", err)
	}

	return nil
}

func saveDangling(ctx context.Context, s Store, cp checkpoint) error {
	cpb, err := cp.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	if err := s.Save(ctx, danglingCheckpoint, cpb); err != nil {
		return fmt.Errorf("store save: %w", err)
	}

	return nil
}

func saveFinalized(ctx context.Context, s Store, cp checkpoint) error {
	cpb, err := cp.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	if err := s.Save(ctx, finalizedCheckpoint, cpb); err != nil {
		return fmt.Errorf("store save: %w", err)
	}

	return nil
}

func loadFinalized(ctx context.Context, s Store) (*checkpoint, bool, error) {
	cpb, err := s.Load(ctx, finalizedCheckpoint)
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
