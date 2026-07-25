package ethindexer

import (
	"compress/gzip"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// readBlob reads and decompresses the blob stored under name. It reports
// whether the blob exists.
func readBlob(dir, name string, out any) (bool, error) {
	f, err := os.Open(filepath.Join(dir, name))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	defer func() { _ = f.Close() }()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return false, err
	}
	defer func() { _ = gr.Close() }()

	if err := gob.NewDecoder(gr).Decode(out); err != nil {
		return false, err
	}

	return true, nil
}

// writeBlob compresses and atomically stores data under name.
func writeBlob(dir, name string, data any) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create data directory %q: %w", dir, err)
	}

	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := f.Name()
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmpName)
	}()

	gw := gzip.NewWriter(f)
	if gob.NewEncoder(gw).Encode(data); err != nil {
		return err
	}
	if err := gw.Close(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return os.Rename(tmpName, filepath.Join(dir, name))
}
