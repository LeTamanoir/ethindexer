package ethindex

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// FileStore implements Store using gzip-compressed files in a directory.
type FileStore struct {
	dir string
}

var _ Store = (*FileStore)(nil)

// NewFileStore creates a FileStore rooted at dir.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("creating directory %q: %w", dir, err)
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) path(key string) string {
	return filepath.Join(s.dir, key+".gz")
}

func (s *FileStore) Load(key string) ([]byte, error) {
	f, err := os.Open(s.path(key))
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

func (s *FileStore) Save(key string, data []byte) error {
	return atomicWrite(s.path(key), func(w io.Writer) error {
		gw := gzip.NewWriter(w)
		if _, err := gw.Write(data); err != nil {
			_ = gw.Close()
			return err
		}
		return gw.Close()
	})
}

func (s *FileStore) Delete(key string) error {
	err := os.Remove(s.path(key))
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
