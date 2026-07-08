package ethindexer

import (
	"bytes"
	"encoding/binary"
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
			BlockHash:      common.HexToHash("0xb1"),
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
			BlockHash:      common.HexToHash("0xb2"),
			BlockTimestamp: 1234567902,
			Index:          0,
		},
		// Same block as previous, same address — exercises dedup tables.
		{
			Address:        common.HexToAddress("0x2222222222222222222222222222222222222222"),
			Topics:         []common.Hash{},
			Data:           []byte{9, 9},
			BlockNumber:    101,
			TxHash:         common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000003"),
			TxIndex:        1,
			BlockHash:      common.HexToHash("0xb2"),
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
		{"bad_version", []byte{99}}, // triggers errInvalidVersion
		{"incomplete_addr_count", append([]byte{logsVersion}, binary.AppendUvarint(nil, 1)...)},
		{"incomplete_block_count", func() []byte {
			b := []byte{logsVersion}
			b = binary.AppendUvarint(b, 0) // 0 addrs
			b = binary.AppendUvarint(b, 1) // 1 block but no block data
			return b
		}()},
		{"valid_header_no_logs", func() []byte {
			b := []byte{logsVersion}
			b = binary.AppendUvarint(b, 0) // 0 addrs
			b = binary.AppendUvarint(b, 0) // 0 blocks
			b = binary.AppendUvarint(b, 1) // 1 log but no log data
			return b
		}()},
		{"truncated_mid_log", func() []byte {
			logs := []types.Log{{
				Address:     common.HexToAddress("0x1"),
				BlockHash:   common.HexToHash("0xb"),
				BlockNumber: 1,
			}}
			b, _ := marshalLogs(logs)
			return b[:len(b)-2]
		}()},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := unmarshalLogs(c.data); err == nil {
				t.Fatal("expected error for truncated data")
			}
		})
	}
}

func TestUnmarshalLogsExtraData(t *testing.T) {
	b, _ := marshalLogs([]types.Log{})
	b = append(b, 99) // extra trailing byte
	if _, err := unmarshalLogs(b); err == nil {
		t.Fatal("expected error for extra trailing data")
	}
}

func TestUnmarshalLogsBadVersion(t *testing.T) {
	b := []byte{99} // wrong version
	_, err := unmarshalLogs(b)
	if !errors.Is(err, errInvalidVersion) {
		t.Fatalf("expected errInvalidVersion, got %v", err)
	}
}

func TestUnmarshalLogsInvalidAddrIdx(t *testing.T) {
	// Build a minimal batch with addrIdx pointing past the address table.
	b := []byte{logsVersion}
	b = binary.AppendUvarint(b, 1) // 1 address
	b = append(b, common.HexToAddress("0x1").Bytes()...)
	b = binary.AppendUvarint(b, 1) // 1 block
	b = binary.AppendUvarint(b, 1) // block number
	b = append(b, common.HexToHash("0xb").Bytes()...)
	b = binary.AppendUvarint(b, 0) // block timestamp
	b = binary.AppendUvarint(b, 1) // 1 log
	b = binary.AppendUvarint(b, 5) // addrIdx = 5, but only 1 address in table
	if _, err := unmarshalLogs(b); err == nil {
		t.Fatal("expected error for invalid addr index")
	}
}

func TestUnmarshalLogsInvalidBlockIdx(t *testing.T) {
	// Build a minimal batch with blockIdx pointing past the block table.
	b := []byte{logsVersion}
	b = binary.AppendUvarint(b, 1) // 1 address
	b = append(b, common.HexToAddress("0x1").Bytes()...)
	b = binary.AppendUvarint(b, 1) // 1 block
	b = binary.AppendUvarint(b, 1) // block number
	b = append(b, common.HexToHash("0xb").Bytes()...)
	b = binary.AppendUvarint(b, 0) // block timestamp
	b = binary.AppendUvarint(b, 1) // 1 log
	b = binary.AppendUvarint(b, 0) // addrIdx = 0 (valid)
	b = binary.AppendUvarint(b, 0) // topicsLen = 0
	b = binary.AppendUvarint(b, 0) // dataLen = 0
	b = binary.AppendUvarint(b, 5) // blockIdx = 5, but only 1 block in table
	if _, err := unmarshalLogs(b); err == nil {
		t.Fatal("expected error for invalid block index")
	}
}

func TestUvarintSize(t *testing.T) {
	cases := []struct {
		v    uint64
		want int
	}{
		{0, 1},
		{1, 1},
		{127, 1},
		{128, 2},
		{16383, 2},
		{16384, 3},
		{1<<63 - 1, 9},
		{1 << 63, 10},
		{^uint64(0), 10},
	}
	for _, c := range cases {
		if got := uvarintSize(c.v); got != c.want {
			t.Errorf("uvarintSize(%d) = %d, want %d", c.v, got, c.want)
		}
	}
	// Verify against the actual encoder.
	for _, c := range cases {
		b := binary.AppendUvarint(nil, c.v)
		if len(b) != c.want {
			t.Errorf("AppendUvarint(%d) length %d != uvarintSize %d", c.v, len(b), c.want)
		}
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
