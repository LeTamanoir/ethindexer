package ethindexer_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/LeTamanoir/ethindexer"
)

func TestChunkBlockRange(t *testing.T) {
	tests := []struct {
		name       string
		from, to   uint64
		size       uint64
		wantChunks []ethindexer.BlockRange
	}{
		{
			name: "reversed range",
			from: 10,
			to:   9,
			size: 3,
		},
		{
			name: "single block",
			from: 7,
			to:   7,
			size: 10,
			wantChunks: []ethindexer.BlockRange{
				{From: 7, To: 7},
			},
		},
		{
			name: "exact chunk",
			from: 10,
			to:   19,
			size: 10,
			wantChunks: []ethindexer.BlockRange{
				{From: 10, To: 19},
			},
		},
		{
			name: "partial final chunk",
			from: 10,
			to:   25,
			size: 10,
			wantChunks: []ethindexer.BlockRange{
				{From: 10, To: 19},
				{From: 20, To: 25},
			},
		},
		{
			name: "one block chunks",
			from: 3,
			to:   5,
			size: 1,
			wantChunks: []ethindexer.BlockRange{
				{From: 3, To: 3},
				{From: 4, To: 4},
				{From: 5, To: 5},
			},
		},
		{
			name: "maximum block number",
			from: math.MaxUint64 - 4,
			to:   math.MaxUint64,
			size: 3,
			wantChunks: []ethindexer.BlockRange{
				{From: math.MaxUint64 - 4, To: math.MaxUint64 - 2},
				{From: math.MaxUint64 - 1, To: math.MaxUint64},
			},
		},
		{
			name: "full uint64 range",
			from: 0,
			to:   math.MaxUint64,
			size: math.MaxUint64,
			wantChunks: []ethindexer.BlockRange{
				{From: 0, To: math.MaxUint64 - 1},
				{From: math.MaxUint64, To: math.MaxUint64},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ethindexer.ChunkBlockRange(tt.from, tt.to, tt.size)
			if !reflect.DeepEqual(got, tt.wantChunks) {
				t.Fatalf("ChunkBlockRange(%d, %d, %d) = %v, want %v",
					tt.from, tt.to, tt.size, got, tt.wantChunks)
			}
		})
	}
}

func TestChunkBlockRange_ZeroSizePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("ChunkBlockRange did not panic for a zero chunk size")
		}
	}()

	ethindexer.ChunkBlockRange(0, 10, 0)
}
