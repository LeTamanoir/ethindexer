package ethindexer

import (
	"path/filepath"
	"testing"
)

func TestBlob_WriteRead(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	data := []byte("hello world")
	if err := writeBlob(dir, "testkey", data); err != nil {
		t.Fatalf("failed to save: %v", err)
	}

	loaded, err := readBlob(dir, "testkey")
	if err != nil {
		t.Fatalf("failed to load: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected data, got nil")
	}
	if string(loaded) != string(data) {
		t.Errorf("data mismatch: got %q, want %q", loaded, data)
	}
}

func TestBlob_ReadNotFound(t *testing.T) {
	dir := t.TempDir()

	loaded, err := readBlob(dir, "missingkey")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if loaded != nil {
		t.Errorf("expected nil for missing key, got %v", loaded)
	}
}
