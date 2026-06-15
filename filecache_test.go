package ethindex

import (
	"os"
	"path/filepath"
	"testing"
)

type testData struct {
	Name  string
	Value int
}

func TestFileCache_SaveLoad(t *testing.T) {
	dir := t.TempDir()
	cache := NewFileCache(dir)

	data := testData{Name: "hello", Value: 42}
	err := cache.Save("testkey", data)
	if err != nil {
		t.Fatalf("failed to save: %v", err)
	}

	var loaded testData
	ok, err := cache.Load("testkey", &loaded)
	if err != nil {
		t.Fatalf("failed to load: %v", err)
	}
	if !ok {
		t.Fatalf("expected to find key")
	}

	if loaded.Name != data.Name || loaded.Value != data.Value {
		t.Errorf("loaded data mismatch: got %v, want %v", loaded, data)
	}
}

func TestFileCache_Delete(t *testing.T) {
	dir := t.TempDir()
	cache := NewFileCache(dir)

	err := cache.Save("testkey", testData{Name: "hello"})
	if err != nil {
		t.Fatalf("failed to save: %v", err)
	}

	err = cache.Delete("testkey")
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// verify it's gone
	var loaded testData
	ok, err := cache.Load("testkey", &loaded)
	if err != nil {
		t.Fatalf("expected no error when loading deleted file, got: %v", err)
	}
	if ok {
		t.Errorf("expected ok=false for deleted file")
	}

	// verify file is actually gone from filesystem
	if _, err := os.Stat(filepath.Join(dir, "testkey.gz")); !os.IsNotExist(err) {
		t.Errorf("expected file to be removed from disk, got error: %v", err)
	}
}

func TestFileCache_NotFound(t *testing.T) {
	dir := t.TempDir()
	cache := NewFileCache(dir)

	var loaded testData
	ok, err := cache.Load("missingkey", &loaded)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if ok {
		t.Errorf("expected ok=false for missing file")
	}
}
