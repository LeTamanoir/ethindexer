package ethindex

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileStore_SaveLoad(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("hello world")
	if err := store.Save("testkey", data); err != nil {
		t.Fatalf("failed to save: %v", err)
	}

	loaded, err := store.Load("testkey")
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

func TestFileStore_Delete(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Save("testkey", []byte("hello")); err != nil {
		t.Fatalf("failed to save: %v", err)
	}

	if err := store.Delete("testkey"); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	loaded, err := store.Load("testkey")
	if err != nil {
		t.Fatalf("unexpected error loading deleted key: %v", err)
	}
	if loaded != nil {
		t.Errorf("expected nil for deleted key, got %v", loaded)
	}

	if _, err := os.Stat(filepath.Join(dir, "testkey.gz")); !os.IsNotExist(err) {
		t.Errorf("expected file to be removed, got error: %v", err)
	}
}

func TestFileStore_LoadNotFound(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Load("missingkey")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if loaded != nil {
		t.Errorf("expected nil for missing key, got %v", loaded)
	}
}

func TestFileStore_DeleteMissing(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Delete("missingkey"); err != nil {
		t.Errorf("expected no error deleting missing key, got: %v", err)
	}
}
