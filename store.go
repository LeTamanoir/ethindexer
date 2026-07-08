package ethindexer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
)

// FileStore implements BlobStore using zstd-compressed files in a directory.
type FileStore struct {
	dir string
	enc *zstd.Encoder
	dec *zstd.Decoder
}

var _ BlobStore = (*FileStore)(nil)

// NewFileStore creates a FileStore rooted at dir.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("creating directory %q: %w", dir, err)
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		return nil, fmt.Errorf("creating zstd encoder: %w", err)
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		enc.Close()
		return nil, fmt.Errorf("creating zstd decoder: %w", err)
	}

	return &FileStore{dir: dir, enc: enc, dec: dec}, nil
}

func (s *FileStore) path(key string) string {
	return filepath.Join(s.dir, key+".zst")
}

func (s *FileStore) Read(_ context.Context, key string) ([]byte, error) {
	compressed, err := os.ReadFile(s.path(key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return s.dec.DecodeAll(compressed, nil)
}

func (s *FileStore) Write(_ context.Context, key string, data []byte) error {
	f, err := os.CreateTemp(s.dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := f.Name()
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmpName)
	}()

	compressed := s.enc.EncodeAll(data, nil)
	if _, err := f.Write(compressed); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path(key))
}

func (s *FileStore) Move(_ context.Context, srcKey, dstKey string) error {
	return os.Rename(s.path(srcKey), s.path(dstKey))
}
