package ethindexer

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

const (
	logBatchEncodingVersion = 1
	encodedLogFixedSize     = common.AddressLength + 2*common.HashLength + 6*8 + 1
)

// logBatch provides a fast gob representation for cached Ethereum logs.
type logBatch []types.Log

func (logs logBatch) GobEncode() ([]byte, error) {
	size := uint64(1 + 8)
	for i := range logs {
		size += encodedLogFixedSize +
			uint64(len(logs[i].Topics))*common.HashLength +
			uint64(len(logs[i].Data))
	}
	if size > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("encoded log batch is too large: %d bytes", size)
	}

	buf := make([]byte, 0, int(size))
	buf = append(buf, logBatchEncodingVersion)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(len(logs)))

	for i := range logs {
		log := &logs[i]
		buf = append(buf, log.Address[:]...)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(len(log.Topics)))
		for _, topic := range log.Topics {
			buf = append(buf, topic[:]...)
		}
		buf = binary.LittleEndian.AppendUint64(buf, uint64(len(log.Data)))
		buf = append(buf, log.Data...)
		buf = binary.LittleEndian.AppendUint64(buf, log.BlockNumber)
		buf = append(buf, log.TxHash[:]...)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(log.TxIndex))
		buf = append(buf, log.BlockHash[:]...)
		buf = binary.LittleEndian.AppendUint64(buf, log.BlockTimestamp)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(log.Index))
		if log.Removed {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	}

	return buf, nil
}

func (logs *logBatch) GobDecode(data []byte) error {
	if len(data) == 0 {
		return io.ErrUnexpectedEOF
	}
	if data[0] != logBatchEncodingVersion {
		return fmt.Errorf("unsupported log batch encoding version %d", data[0])
	}

	decoder := logBatchDecoder(data[1:])
	count, err := decoder.uint64()
	if err != nil {
		return err
	}
	if count > uint64(len(decoder))/encodedLogFixedSize {
		return fmt.Errorf("invalid log count %d", count)
	}

	var decoded logBatch
	if count != 0 {
		decoded = make(logBatch, int(count))
	}
	for i := range decoded {
		log := &decoded[i]

		address, err := decoder.bytes(common.AddressLength)
		if err != nil {
			return fmt.Errorf("decode log %d address: %w", i, err)
		}
		copy(log.Address[:], address)

		topicCount, err := decoder.uint64()
		if err != nil {
			return fmt.Errorf("decode log %d topic count: %w", i, err)
		}
		if topicCount > uint64(len(decoder))/common.HashLength {
			return fmt.Errorf("invalid topic count %d for log %d", topicCount, i)
		}
		if topicCount != 0 {
			log.Topics = make([]common.Hash, int(topicCount))
		}
		for j := range log.Topics {
			topic, err := decoder.bytes(common.HashLength)
			if err != nil {
				return fmt.Errorf("decode log %d topic %d: %w", i, j, err)
			}
			copy(log.Topics[j][:], topic)
		}

		dataLength, err := decoder.uint64()
		if err != nil {
			return fmt.Errorf("decode log %d data length: %w", i, err)
		}
		logData, err := decoder.bytes(dataLength)
		if err != nil {
			return fmt.Errorf("decode log %d data: %w", i, err)
		}
		if dataLength != 0 {
			log.Data = append([]byte(nil), logData...)
		}

		if log.BlockNumber, err = decoder.uint64(); err != nil {
			return fmt.Errorf("decode log %d block number: %w", i, err)
		}
		txHash, err := decoder.bytes(common.HashLength)
		if err != nil {
			return fmt.Errorf("decode log %d transaction hash: %w", i, err)
		}
		copy(log.TxHash[:], txHash)

		txIndex, err := decoder.uint64()
		if err != nil {
			return fmt.Errorf("decode log %d transaction index: %w", i, err)
		}
		log.TxIndex = uint(txIndex)
		if uint64(log.TxIndex) != txIndex {
			return fmt.Errorf("transaction index %d overflows uint", txIndex)
		}

		blockHash, err := decoder.bytes(common.HashLength)
		if err != nil {
			return fmt.Errorf("decode log %d block hash: %w", i, err)
		}
		copy(log.BlockHash[:], blockHash)

		if log.BlockTimestamp, err = decoder.uint64(); err != nil {
			return fmt.Errorf("decode log %d block timestamp: %w", i, err)
		}
		index, err := decoder.uint64()
		if err != nil {
			return fmt.Errorf("decode log %d index: %w", i, err)
		}
		log.Index = uint(index)
		if uint64(log.Index) != index {
			return fmt.Errorf("log index %d overflows uint", index)
		}

		removed, err := decoder.bytes(1)
		if err != nil {
			return fmt.Errorf("decode log %d removed flag: %w", i, err)
		}
		switch removed[0] {
		case 0:
		case 1:
			log.Removed = true
		default:
			return fmt.Errorf("invalid removed flag %d for log %d", removed[0], i)
		}
	}

	if len(decoder) != 0 {
		return fmt.Errorf("unexpected trailing log data: %d bytes", len(decoder))
	}
	*logs = decoded
	return nil
}

type logBatchDecoder []byte

func (d *logBatchDecoder) bytes(size uint64) ([]byte, error) {
	if size > uint64(len(*d)) {
		return nil, io.ErrUnexpectedEOF
	}
	value := (*d)[:int(size)]
	*d = (*d)[int(size):]
	return value, nil
}

func (d *logBatchDecoder) uint64() (uint64, error) {
	value, err := d.bytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(value), nil
}
