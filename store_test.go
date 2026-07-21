package ethindexer

import (
	"os"
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

func TestBlob_Move(t *testing.T) {
	dir := t.TempDir()

	if err := writeBlob(dir, "src", []byte("hello")); err != nil {
		t.Fatalf("failed to save: %v", err)
	}

	if err := moveBlob(dir, "src", "dst"); err != nil {
		t.Fatalf("failed to move: %v", err)
	}

	loaded, err := readBlob(dir, "dst")
	if err != nil {
		t.Fatalf("failed to load moved data: %v", err)
	}
	if string(loaded) != "hello" {
		t.Errorf("expected %q, got %q", "hello", loaded)
	}

	if _, err := os.Stat(filepath.Join(dir, "src.gz")); !os.IsNotExist(err) {
		t.Errorf("expected source file to be removed, got error: %v", err)
	}
}

func TestBlob_MoveMissingSource(t *testing.T) {
	dir := t.TempDir()

	if err := moveBlob(dir, "missing", "dst"); err == nil {
		t.Fatal("expected error moving missing source, got nil")
	}
}
