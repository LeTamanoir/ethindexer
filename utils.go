package ethindex

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
)

func atomicWrite(filename string, fn func(io.Writer) error) error {
	dir := filepath.Dir(filename)

	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}()

	if err := fn(f); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return os.Rename(f.Name(), filename)
}

func checkpointKey(h blockHeader) string {
	return fmt.Sprintf("checkpoint:%d-%s", h.Number, h.Hash)
}

func finalizedCheckpointKey() string {
	return "checkpoint:finalized"
}

func filterQueryKey(q ethereum.FilterQuery) string {
	var sb strings.Builder

	fmt.Fprintf(&sb, "%d-%d", q.FromBlock, q.ToBlock)

	if len(q.Addresses) > 0 {
		sb.WriteString("|A:")
		for i, a := range q.Addresses {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(a.Hex())
		}
	}

	if len(q.Topics) > 0 {
		sb.WriteString("|T:")
		for i, tt := range q.Topics {
			if i > 0 {
				sb.WriteByte(';')
			}
			for j, t := range tt {
				if j > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(t.Hex())
			}
		}
	}

	return sb.String()
}

func decodeUint64(b []byte, out *uint64) ([]byte, error) {
	const uint64Size = 8
	if len(b) < uint64Size {
		return nil, fmt.Errorf("buffer too short: need %d, have %d", uint64Size, len(b))
	}
	*out = binary.LittleEndian.Uint64(b)
	return b[uint64Size:], nil
}

func decodeHash(b []byte, out *common.Hash) ([]byte, error) {
	if len(b) < common.HashLength {
		return nil, fmt.Errorf("buffer too short: need %d, have %d", common.HashLength, len(b))
	}
	out.SetBytes(b)
	return b[common.HashLength:], nil
}

func decodeBytes(b []byte, out *[]byte) ([]byte, error) {
	var l uint64
	b, err := decodeUint64(b, &l)
	if err != nil {
		return nil, err
	}
	if len(b) < int(l) {
		return nil, fmt.Errorf("buffer too short: need %d, have %d", l, len(b))
	}
	*out = make([]byte, l)
	copy(*out, b[:l])
	return b[l:], nil
}

func decodeAddress(b []byte, out *common.Address) ([]byte, error) {
	if len(b) < common.AddressLength {
		return nil, fmt.Errorf("buffer too short: need %d, have %d", common.AddressLength, len(b))
	}
	out.SetBytes(b)
	return b[common.AddressLength:], nil
}
