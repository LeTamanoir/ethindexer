package ethindexer

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func blobPath(dir, key string) string {
	return filepath.Join(dir, key+".gz")
}

// readBlob reads and decompresses the blob stored under key. A missing key
// returns (nil, nil).
func readBlob(dir, key string) ([]byte, error) {
	f, err := os.Open(blobPath(dir, key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer func() { _ = gr.Close() }()

	return io.ReadAll(gr)
}

// writeBlob compresses and atomically stores data under key.
func writeBlob(dir, key string, data []byte) error {
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
	if _, err := gw.Write(data); err != nil {
		return err
	}
	if err := gw.Close(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return os.Rename(tmpName, blobPath(dir, key))
}

// moveBlob atomically transfers a blob from srcKey to dstKey.
func moveBlob(dir, srcKey, dstKey string) error {
	return os.Rename(blobPath(dir, srcKey), blobPath(dir, dstKey))
}
