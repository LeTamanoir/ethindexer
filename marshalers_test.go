package ethindexer

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestMarshalLogsRoundTrip(t *testing.T) {
	logs := []types.Log{
		{
			Address:        common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Topics:         []common.Hash{common.HexToHash("0xaaa"), common.HexToHash("0xbbb")},
			Data:           []byte{1, 2, 3},
			BlockNumber:    100,
			TxHash:         common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001"),
			TxIndex:        5,
			BlockHash:      common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000b1"),
			BlockTimestamp: 1234567890,
			Index:          2,
		},
		{
			Address:        common.HexToAddress("0x2222222222222222222222222222222222222222"),
			Topics:         []common.Hash{common.HexToHash("0xccc")},
			Data:           []byte{},
			BlockNumber:    101,
			TxHash:         common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002"),
			TxIndex:        0,
			BlockHash:      common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000b2"),
			BlockTimestamp: 1234567902,
			Index:          0,
		},
		{
			Address:        common.HexToAddress("0x2222222222222222222222222222222222222222"),
			Topics:         []common.Hash{},
			Data:           []byte{9, 9},
			BlockNumber:    101,
			TxHash:         common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000003"),
			TxIndex:        1,
			BlockHash:      common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000b2"),
			BlockTimestamp: 1234567902,
			Index:          1,
		},
	}

	b, err := marshalLogs(logs)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got, err := unmarshalLogs(b)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !reflect.DeepEqual(got, logs) {
		t.Fatalf("round-trip mismatch\n got: %+v\nwant: %+v", got, logs)
	}
}

func TestMarshalLogsEmpty(t *testing.T) {
	b, err := marshalLogs(nil)
	if err != nil {
		t.Fatalf("marshal nil: %v", err)
	}
	logs, err := unmarshalLogs(b)
	if err != nil {
		t.Fatalf("unmarshal nil: %v", err)
	}
	if len(logs) != 0 {
		t.Fatalf("expected 0 logs, got %d", len(logs))
	}

	b, err = marshalLogs([]types.Log{})
	if err != nil {
		t.Fatalf("marshal empty: %v", err)
	}
	logs, err = unmarshalLogs(b)
	if err != nil {
		t.Fatalf("unmarshal empty: %v", err)
	}
	if len(logs) != 0 {
		t.Fatalf("expected 0 logs, got %d", len(logs))
	}
}

func TestUnmarshalLogsTruncated(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"version_only", []byte{logsVersion}},
		{"bad_version", []byte{99}},
		{"incomplete_count", []byte{logsVersion, 0x01}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := unmarshalLogs(c.data)
			if err == nil {
				t.Fatal("expected error for truncated data")
			}
		})
	}
}

func TestUnmarshalLogsBadVersion(t *testing.T) {
	b := []byte{99} // wrong version
	_, err := unmarshalLogs(b)
	if !errors.Is(err, errInvalidVersion) {
		t.Fatalf("expected errInvalidVersion, got %v", err)
	}
}

func TestUnmarshalLogsExtraData(t *testing.T) {
	b, _ := marshalLogs([]types.Log{})
	b = append(b, 99) // extra trailing byte
	if _, err := unmarshalLogs(b); err == nil {
		t.Fatal("expected error for extra trailing data")
	}
}

func TestMarshalCheckpointRoundTrip(t *testing.T) {
	cp := checkpoint{
		head:  blockRef{number: 42, hash: common.HexToHash("0xabc")},
		state: []byte("handler state"),
	}

	b, err := marshalCheckpoint(cp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got, err := unmarshalCheckpoint(b)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.head.number != cp.head.number || got.head.hash != cp.head.hash {
		t.Fatalf("head mismatch: got %+v, want %+v", got.head, cp.head)
	}
	if !bytes.Equal(got.state, cp.state) {
		t.Fatalf("state mismatch: got %q, want %q", got.state, cp.state)
	}
}

func TestUnmarshalCheckpointBadVersion(t *testing.T) {
	b := []byte{99} // wrong version
	_, err := unmarshalCheckpoint(b)
	if !errors.Is(err, errInvalidVersion) {
		t.Fatalf("expected errInvalidVersion, got %v", err)
	}
}

func TestUnmarshalCheckpointTruncated(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"version_only", []byte{checkpointVersion}},
		{"bad_version", []byte{99}},
		{"incomplete_head", append([]byte{checkpointVersion}, make([]byte, 4)...)},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := unmarshalCheckpoint(c.data)
			if err == nil {
				t.Fatal("expected error for truncated data")
			}
		})
	}
}

func TestFileStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	data := []byte("hello zstd world")
	if err := store.Write(t.Context(), "key1", data); err != nil {
		t.Fatalf("write: %v", err)
	}

	read, err := store.Read(t.Context(), "key1")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(read, data) {
		t.Fatalf("round-trip mismatch: %q vs %q", read, data)
	}

	// Missing key returns nil, nil.
	read, err = store.Read(t.Context(), "missing")
	if err != nil {
		t.Fatalf("read missing: %v", err)
	}
	if read != nil {
		t.Fatalf("expected nil for missing key, got %q", read)
	}
}

func TestFileStoreMove(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	data := []byte("movable")
	if err := store.Write(t.Context(), "src", data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := store.Move(t.Context(), "src", "dst"); err != nil {
		t.Fatalf("move: %v", err)
	}

	read, err := store.Read(t.Context(), "dst")
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if !bytes.Equal(read, data) {
		t.Fatalf("move round-trip mismatch")
	}

	read, err = store.Read(t.Context(), "src")
	if err != nil {
		t.Fatalf("read src after move: %v", err)
	}
	if read != nil {
		t.Fatalf("expected src to be gone after move")
	}
}
