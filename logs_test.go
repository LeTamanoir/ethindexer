package ethindexer

import (
	"bytes"
	"encoding/gob"
	"math/big"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestLogBatchGobRoundTrip(t *testing.T) {
	want := logBatch{
		{
			Address:        common.HexToAddress("0x1234"),
			Topics:         []common.Hash{common.HexToHash("0x5678"), common.HexToHash("0x9abc")},
			Data:           []byte{1, 2, 3, 4},
			BlockNumber:    123456,
			TxHash:         common.HexToHash("0xdef0"),
			TxIndex:        42,
			BlockHash:      common.HexToHash("0x1357"),
			BlockTimestamp: 987654,
			Index:          84,
			Removed:        true,
		},
		{},
	}

	var encoded bytes.Buffer
	if err := gob.NewEncoder(&encoded).Encode(want); err != nil {
		t.Fatalf("encode logs: %v", err)
	}

	var got logBatch
	if err := gob.NewDecoder(&encoded).Decode(&got); err != nil {
		t.Fatalf("decode logs: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded logs differ:\ngot:  %+v\nwant: %+v", got, want)
	}
}

func TestLogBatchGobRejectsInvalidData(t *testing.T) {
	encoded, err := (logBatch{}).GobEncode()
	if err != nil {
		t.Fatalf("encode logs: %v", err)
	}

	tests := map[string][]byte{
		"empty":            nil,
		"unknown version":  {logBatchEncodingVersion + 1},
		"truncated":        encoded[:len(encoded)-1],
		"trailing content": append(encoded, 0),
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			var logs logBatch
			if err := logs.GobDecode(data); err == nil {
				t.Fatal("expected decoding error")
			}
		})
	}
}

type defaultGobLogBatch []types.Log

func BenchmarkLogBatchGob(b *testing.B) {
	logs := make(logBatch, 1000)
	for i := range logs {
		logs[i] = types.Log{
			Address:        common.BigToAddress(big.NewInt(int64(i + 1))),
			Topics:         []common.Hash{{1}, {2}, {3}},
			Data:           bytes.Repeat([]byte{byte(i)}, 128),
			BlockNumber:    uint64(i + 1),
			TxHash:         common.BigToHash(big.NewInt(int64(i + 2))),
			TxIndex:        uint(i),
			BlockHash:      common.BigToHash(big.NewInt(int64(i + 3))),
			BlockTimestamp: uint64(i + 4),
			Index:          uint(i + 5),
			Removed:        i%2 == 0,
		}
	}

	b.Run("encode/default", func(b *testing.B) {
		var encoded bytes.Buffer
		for b.Loop() {
			encoded.Reset()
			if err := gob.NewEncoder(&encoded).Encode(defaultGobLogBatch(logs)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("encode/custom", func(b *testing.B) {
		var encoded bytes.Buffer
		for b.Loop() {
			encoded.Reset()
			if err := gob.NewEncoder(&encoded).Encode(logs); err != nil {
				b.Fatal(err)
			}
		}
	})

	var defaultEncoded bytes.Buffer
	if err := gob.NewEncoder(&defaultEncoded).Encode(defaultGobLogBatch(logs)); err != nil {
		b.Fatal(err)
	}
	var customEncoded bytes.Buffer
	if err := gob.NewEncoder(&customEncoded).Encode(logs); err != nil {
		b.Fatal(err)
	}

	b.Run("decode/default", func(b *testing.B) {
		reader := bytes.NewReader(nil)
		for b.Loop() {
			reader.Reset(defaultEncoded.Bytes())
			var decoded defaultGobLogBatch
			if err := gob.NewDecoder(reader).Decode(&decoded); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("decode/custom", func(b *testing.B) {
		reader := bytes.NewReader(nil)
		for b.Loop() {
			reader.Reset(customEncoded.Bytes())
			var decoded logBatch
			if err := gob.NewDecoder(reader).Decode(&decoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}
