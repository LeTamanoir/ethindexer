package ethindex

import (
	"compress/gzip"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"path/filepath"
)

type FileCache struct {
	dir string
}

func NewFileCache(dir string) *FileCache {
	return &FileCache{dir: dir}
}

func (fs *FileCache) filepath(name string) string {
	return filepath.Join(fs.dir, name+".gz")
}

func (fs *FileCache) Delete(name string) error {
	return os.Remove(fs.filepath(name))
}

func (fs *FileCache) Load(name string, out any) (ok bool, err error) {
	f, err := os.Open(fs.filepath(name))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	defer func() {
		_ = f.Close()
	}()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = gr.Close()
	}()

	if err := gob.NewDecoder(gr).Decode(out); err != nil {
		return false, err
	}

	return true, nil
}

func (fs *FileCache) Save(name string, v any) error {
	return atomicWrite(fs.filepath(name), func(w io.Writer) error {
		gw := gzip.NewWriter(w)
		defer func() {
			_ = gw.Close()
		}()

		return gob.NewEncoder(gw).Encode(v)
	})
}
