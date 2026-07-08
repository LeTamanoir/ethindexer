package ethindexer

import (
	"encoding/binary"
	"errors"
	"math/bits"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

var (
	errInvalidLogs       = errors.New("invalid logs")
	errInvalidCheckpoint = errors.New("invalid checkpoint")
	errInvalidVersion    = errors.New("invalid log format version; clear your indexer cache and restart")
)

const logsVersion = 1

func marshalCheckpoint(c checkpoint) ([]byte, error) {
	b := make([]byte, 0, 8+common.HashLength+len(c.state))
	b = binary.LittleEndian.AppendUint64(b, c.head.number)
	b = append(b, c.head.hash[:]...)
	b = append(b, c.state...)
	return b, nil
}

func unmarshalCheckpoint(b []byte) (checkpoint, error) {
	if len(b) < 8+common.HashLength {
		return checkpoint{}, errInvalidCheckpoint
	}
	return checkpoint{
		head: blockRef{
			number: binary.LittleEndian.Uint64(b),
			hash:   common.Hash(b[8 : 8+common.HashLength]),
		},
		state: append([]byte(nil), b[8+common.HashLength:]...),
	}, nil
}

// readUvarint reads a uvarint from b, returning the value, the remaining
// slice, and true on success. If b is too short or the value overflows,
// it returns (_, nil, false).
func readUvarint(b []byte) (uint64, []byte, bool) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, nil, false
	}
	return v, b[n:], true
}

func marshalLogs(logs []types.Log) ([]byte, error) {
	// Build deduplication tables.
	addrToIdx := make(map[common.Address]uint64, 1)
	var addrs []common.Address

	type blockMeta struct {
		number    uint64
		hash      common.Hash
		timestamp uint64
	}
	hashToBlockIdx := make(map[common.Hash]uint64, len(logs))
	var blocks []blockMeta

	for _, l := range logs {
		if _, ok := addrToIdx[l.Address]; !ok {
			addrToIdx[l.Address] = uint64(len(addrs))
			addrs = append(addrs, l.Address)
		}
		if _, ok := hashToBlockIdx[l.BlockHash]; !ok {
			hashToBlockIdx[l.BlockHash] = uint64(len(blocks))
			blocks = append(blocks, blockMeta{
				number:    l.BlockNumber,
				hash:      l.BlockHash,
				timestamp: l.BlockTimestamp,
			})
		}
	}

	// Compute upper-bound capacity.
	size := 1 // version
	size += uvarintSize(uint64(len(addrs))) + len(addrs)*common.AddressLength
	size += uvarintSize(uint64(len(blocks)))
	for _, blk := range blocks {
		size += uvarintSize(blk.number) + common.HashLength + uvarintSize(blk.timestamp)
	}
	size += uvarintSize(uint64(len(logs)))
	for _, l := range logs {
		size += uvarintSize(addrToIdx[l.Address])
		size += uvarintSize(uint64(len(l.Topics))) + len(l.Topics)*common.HashLength
		size += uvarintSize(uint64(len(l.Data))) + len(l.Data)
		size += uvarintSize(hashToBlockIdx[l.BlockHash])
		size += common.HashLength // TxHash
		size += uvarintSize(uint64(l.TxIndex))
		size += uvarintSize(uint64(l.Index))
	}

	b := make([]byte, 0, size)
	b = append(b, logsVersion)

	// Address table.
	b = binary.AppendUvarint(b, uint64(len(addrs)))
	for _, a := range addrs {
		b = append(b, a[:]...)
	}

	// Block metadata table.
	b = binary.AppendUvarint(b, uint64(len(blocks)))
	for _, blk := range blocks {
		b = binary.AppendUvarint(b, blk.number)
		b = append(b, blk.hash[:]...)
		b = binary.AppendUvarint(b, blk.timestamp)
	}

	// Logs.
	b = binary.AppendUvarint(b, uint64(len(logs)))
	for _, l := range logs {
		b = binary.AppendUvarint(b, addrToIdx[l.Address])
		b = binary.AppendUvarint(b, uint64(len(l.Topics)))
		for _, t := range l.Topics {
			b = append(b, t[:]...)
		}
		b = binary.AppendUvarint(b, uint64(len(l.Data)))
		b = append(b, l.Data...)
		b = binary.AppendUvarint(b, hashToBlockIdx[l.BlockHash])
		b = append(b, l.TxHash[:]...)
		b = binary.AppendUvarint(b, uint64(l.TxIndex))
		b = binary.AppendUvarint(b, uint64(l.Index))
	}

	return b, nil
}

func unmarshalLogs(b []byte) ([]types.Log, error) {
	if len(b) == 0 {
		return nil, errInvalidLogs
	}
	if b[0] != logsVersion {
		return nil, errInvalidVersion
	}
	b = b[1:]

	// Address table.
	addrCount, b, ok := readUvarint(b)
	if !ok {
		return nil, errInvalidLogs
	}
	addrs := make([]common.Address, addrCount)
	for i := range addrs {
		if len(b) < common.AddressLength {
			return nil, errInvalidLogs
		}
		addrs[i].SetBytes(b[:common.AddressLength])
		b = b[common.AddressLength:]
	}

	// Block metadata table.
	blockCount, b, ok := readUvarint(b)
	if !ok {
		return nil, errInvalidLogs
	}
	type blockMeta struct {
		number    uint64
		hash      common.Hash
		timestamp uint64
	}
	blocks := make([]blockMeta, blockCount)
	for i := range blocks {
		blocks[i].number, b, ok = readUvarint(b)
		if !ok || len(b) < common.HashLength {
			return nil, errInvalidLogs
		}
		blocks[i].hash.SetBytes(b[:common.HashLength])
		b = b[common.HashLength:]
		blocks[i].timestamp, b, ok = readUvarint(b)
		if !ok {
			return nil, errInvalidLogs
		}
	}

	// Logs.
	logCount, b, ok := readUvarint(b)
	if !ok {
		return nil, errInvalidLogs
	}
	logs := make([]types.Log, logCount)

	for i := range logs {
		var l types.Log
		var addrIdx, topicsLen, dataLen, blockIdx, txIndex, index uint64
		var ok bool

		addrIdx, b, ok = readUvarint(b)
		if !ok || addrIdx >= uint64(len(addrs)) {
			return nil, errInvalidLogs
		}
		l.Address = addrs[addrIdx]

		topicsLen, b, ok = readUvarint(b)
		if !ok {
			return nil, errInvalidLogs
		}
		if len(b) < int(topicsLen)*common.HashLength {
			return nil, errInvalidLogs
		}
		l.Topics = make([]common.Hash, topicsLen)
		for j := range l.Topics {
			l.Topics[j].SetBytes(b[:common.HashLength])
			b = b[common.HashLength:]
		}

		dataLen, b, ok = readUvarint(b)
		if !ok {
			return nil, errInvalidLogs
		}
		if len(b) < int(dataLen) {
			return nil, errInvalidLogs
		}
		l.Data = make([]byte, dataLen)
		copy(l.Data, b[:dataLen])
		b = b[dataLen:]

		blockIdx, b, ok = readUvarint(b)
		if !ok || blockIdx >= uint64(len(blocks)) {
			return nil, errInvalidLogs
		}
		blk := blocks[blockIdx]
		l.BlockNumber = blk.number
		l.BlockHash = blk.hash
		l.BlockTimestamp = blk.timestamp

		if len(b) < common.HashLength {
			return nil, errInvalidLogs
		}
		l.TxHash.SetBytes(b[:common.HashLength])
		b = b[common.HashLength:]

		txIndex, b, ok = readUvarint(b)
		if !ok {
			return nil, errInvalidLogs
		}
		l.TxIndex = uint(txIndex)

		index, b, ok = readUvarint(b)
		if !ok {
			return nil, errInvalidLogs
		}
		l.Index = uint(index)

		logs[i] = l
	}

	if len(b) != 0 {
		return nil, errInvalidLogs
	}

	return logs, nil
}

func uvarintSize(x uint64) int {
	if x == 0 {
		return 1
	}
	b := bits.Len64(x)
	return (b + 6) / 7
}
