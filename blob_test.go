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

	var loaded []byte
	found, err := readBlob(dir, "testkey", &loaded)
	if err != nil {
		t.Fatalf("failed to load: %v", err)
	}
	if !found {
		t.Fatal("expected blob")
	}
	if string(loaded) != string(data) {
		t.Errorf("data mismatch: got %q, want %q", loaded, data)
	}
}

func TestBlob_ReadNotFound(t *testing.T) {
	dir := t.TempDir()

	var loaded []byte
	found, err := readBlob(dir, "missingkey", &loaded)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if found {
		t.Fatal("expected missing blob")
	}
}
