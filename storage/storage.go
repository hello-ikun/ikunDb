package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"time"
)

var (
	ErrInsufficientDataLength = errors.New("insufficient data length")
	ErrTimestampFailed        = errors.New("timestamp failed")
	ErrCRCFailed              = errors.New("CRC check failed")
	ErrEncodingTimestamp      = errors.New("error encoding timestamp")
	ErrEncodingKeyLength      = errors.New("error encoding key length")
	ErrEncodingValueLength    = errors.New("error encoding value length")
	ErrDecodingTimestamp      = errors.New("error decoding timestamp")
	ErrDecodingKeyLength      = errors.New("error decoding key length")
	ErrDecodingValueLength    = errors.New("error decoding value length")
	ErrLength                 = errors.New("error body length")
	ErrPosLength              = errors.New("error pos length")
	ErrPosDecodeFailed        = errors.New("error pos decode failed")
	ErrKeyNotFound            = errors.New("key not found")
)

const MaxStorageLength = crc32.Size + 1 + 8 + binary.MaxVarintLen64*2

// Encode encodes key and value into a byte slice.
// crc+[timestamp]+type+len(key)+len(value)
func Encode(data *EncodeStorage) ([]byte, error) {
	keyLen, valLen := len(data.Key), len(data.Value)
	var timeStamp int64
	if data.TimeDuration <= 0 {
		timeStamp = -1
	} else {
		timeStamp = time.Now().Add(data.TimeDuration).Unix()
	}
	maxLen := MaxStorageLength
	body := make([]byte, maxLen)

	index := crc32.Size
	body[index] = data.Flag
	index++

	if data.Flag != byte(1) {
		binary.LittleEndian.PutUint64(body[index:], uint64(timeStamp))
	}
	index += 8

	// Encode keyLen
	n := binary.PutUvarint(body[index:], uint64(keyLen))
	if n <= 0 {
		return nil, ErrEncodingKeyLength
	}
	index += n

	// Encode valueLen
	n = binary.PutUvarint(body[index:], uint64(valLen))
	if n <= 0 {
		return nil, ErrEncodingValueLength
	}
	index += n

	encBody := make([]byte, index+len(data.Key)+len(data.Value))
	//copy key and value

	copy(encBody[:index], body[:index])
	// Copy key and value
	copy(encBody[index:], data.Key)
	index += keyLen
	copy(encBody[index:], data.Value)
	index += valLen

	// Calculate CRC
	crc := crc32.ChecksumIEEE(encBody[crc32.Size:])
	binary.LittleEndian.PutUint32(encBody[:crc32.Size], crc)
	return encBody[:index], nil
}

type EncodeStorage struct {
	Key, Value   []byte
	Flag         byte
	TimeDuration time.Duration
	Length       int
}
type DecodeStorage struct {
	Key, Value []byte
	Flag       byte
	TimeStamp  int64
	Length     int
}
type DecodeStruct struct {
	KeyLen, ValueLen int
	TimeStamp        int64
	Flag             byte
	Length           int
}

func DecodeDetail(data []byte) (*DecodeStruct, error) {
	if len(data) < crc32.Size {
		return nil, ErrInsufficientDataLength
	}

	index := crc32.Size
	flag := data[index]
	index++

	var timeStamp uint64
	if flag != byte(1) {
		timeStamp = binary.LittleEndian.Uint64(data[index:])
	}
	index += 8

	// Decode keyLen
	keyLen, n := binary.Uvarint(data[index:])
	if n <= 0 {
		return nil, ErrDecodingKeyLength
	}
	index += n

	// Decode valueLen
	valueLen, n := binary.Uvarint(data[index:])
	if n <= 0 {
		return nil, ErrDecodingValueLength
	}
	index += n
	return &DecodeStruct{
		KeyLen:    int(keyLen),
		ValueLen:  int(valueLen),
		TimeStamp: int64(timeStamp),
		Flag:      flag,
		Length:    index,
	}, nil
}

// DecodeStoage decodes the key and value from the given data.
// return value and length
func DecodeStoage(data []byte) (*DecodeStorage, error) {
	length := len(data)
	if length < crc32.Size {
		return nil, ErrInsufficientDataLength
	}

	index := crc32.Size
	flag := data[index]
	index++

	var timeStamp uint64
	if flag != byte(1) {
		timeStamp = binary.LittleEndian.Uint64(data[index:])
	}
	index += 8

	// Decode keyLen
	keyLen, n := binary.Uvarint(data[index:])
	if n <= 0 {
		return nil, ErrDecodingKeyLength
	}
	index += n

	// Decode valueLen
	valueLen, n := binary.Uvarint(data[index:])
	if n <= 0 {
		return nil, ErrDecodingValueLength
	}
	index += n
	if index+int(keyLen+valueLen) < length {
		return nil, ErrLength
	}
	// Extract key and value
	key := data[index : index+int(keyLen)]
	index += int(keyLen)

	value := data[index : index+int(valueLen)]
	index += int(valueLen)

	// Verify CRC
	crc := crc32.ChecksumIEEE(data[crc32.Size:index])
	if binary.LittleEndian.Uint32(data[:crc32.Size]) != crc {
		return nil, ErrCRCFailed
	}

	return &DecodeStorage{
		Key:       key,
		Value:     value,
		Flag:      flag,
		TimeStamp: int64(timeStamp),
		Length:    index,
	}, nil
}

// Pos ***************Pos**********************************************************
type Pos struct {
	Fid    uint64
	Offset uint64
	Length uint64
}

// Byte 对于pos存储时候进行优化 要么 直接存储pos要么存储byte
func (p *Pos) Byte() []byte {
	return []byte(fmt.Sprintf("%08x%08x%08x", p.Fid, p.Offset, p.Length))
}

func DecodeToPos(data []byte) (*Pos, error) {
	if len(data) != 24 {
		return nil, ErrPosLength
	}
	var fid, offset, length uint64
	_, err := fmt.Sscanf(string(data), "%08x%08x%08x", &fid, &offset, &length)
	if err != nil {
		return nil, ErrPosDecodeFailed
	}
	pos := &Pos{
		Fid:    fid,
		Offset: offset,
		Length: length,
	}
	return pos, nil
}
