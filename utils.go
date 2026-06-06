package ethindex

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethereum/go-ethereum"
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
