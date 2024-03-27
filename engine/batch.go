package engine

import (
	"errors"
	"fmt"
	"ikunDb/index"
	"ikunDb/options"
	"ikunDb/storage"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type IkunDbBatch struct {
	option  options.BatchOptions
	m       *sync.RWMutex
	db      *IkunDB
	pending map[string]*storage.EncodeStorage
}

var (
	TxnFinishTag  = "txn-finish"
	NonSeqNum     = 0
	ErrBatchCount = errors.New("error batch count")
)

func (db *IkunDB) DefaultBathch() *IkunDbBatch {
	return db.NewIkunDbBatch(options.DefaultBatchOptions)
}
func (db *IkunDB) NewIkunDbBatch(batchOptions options.BatchOptions) *IkunDbBatch {
	return &IkunDbBatch{
		batchOptions, &sync.RWMutex{}, db, make(map[string]*storage.EncodeStorage),
	}
}
func (i *IkunDbBatch) Set(key, value []byte, duration time.Duration) error {
	i.m.Lock()
	defer i.m.Unlock()

	data := storage.EncodeStorage{Key: key, Value: value, Flag: index.Normal, TimeDuration: duration}
	i.pending[string(key)] = &data
	return nil
}
func (i *IkunDbBatch) Del(key []byte) error {
	i.m.Lock()
	defer i.m.Unlock()

	pos, err := i.db.memoryIndex.Get(key)
	if err != nil {
		return err
	}
	if pos != nil {
		data := &storage.EncodeStorage{Key: key, Flag: index.Delete}
		i.pending[string(key)] = data
	} else {
		if i.pending[string(key)] != nil {
			delete(i.pending, string(key))
		}
		// del not exist key
		return nil
	}
	return nil
}

// Commit batch commit
func (i *IkunDbBatch) Commit() error {
	i.m.Lock()
	defer i.m.Unlock()

	if len(i.pending) == 0 {
		return nil
	}

	if len(i.pending) > int(i.option.MaxBatchNum) {
		return ErrBatchCount
	}

	seqNum := atomic.AddUint64(&i.db.seqNum, 1)

	for _, data := range i.pending {
		encodeKey := EncodeSeqNum(data.Key, seqNum)
		pos, err := i.db.appendDataWithLock(&storage.EncodeStorage{
			Key:          encodeKey,
			Value:        data.Value,
			Flag:         data.Flag,
			TimeDuration: data.TimeDuration,
		})

		if i.option.SyncWrites {
			if err := i.db.activeFiles.Sync(); err != nil {
				return err
			}
		}

		if err != nil {
			return err
		}
		// update memory index and index key is not encodeKey
		if data.Flag == index.Normal {
			old, ok := i.db.memoryIndex.Set(data.Key, pos)
			if !ok {
				return ErrSet
			}
			if old != nil {
				i.db.needMergeSize += old.Length
			}
		} else {
			old, ok := i.db.memoryIndex.Del(data.Key)
			if !ok {
				return ErrDel
			}
			if old != nil {
				i.db.needMergeSize += old.Length
			}
			i.db.needMergeSize += pos.Length
		}
	}
	// append tx finish tag
	encodeTxn := EncodeSeqNum([]byte(TxnFinishTag), seqNum)
	//pos, err := i.db.appendData(encodeTxn, encodeTxn, index.TxFinish)
	pos, err := i.db.appendData(&storage.EncodeStorage{
		Key:          encodeTxn,
		Value:        encodeTxn,
		Flag:         index.TxFinish,
		TimeDuration: 0,
	})
	fmt.Println("pos:", pos.Length)
	i.db.needMergeSize += pos.Length //txn create need merge data

	if i.option.SyncWrites {
		if err := i.db.activeFiles.Sync(); err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	i.pending = map[string]*storage.EncodeStorage{}
	return nil
}

// EncodeSeqNum achieve seqNum + key encode to data
func EncodeSeqNum(key []byte, seqNum uint64) []byte {
	return []byte(fmt.Sprintf("%s%03x", key, seqNum&0xfff))
}

func DecodeSeqNum(buf []byte) (uint64, []byte) {
	// Extract the hexadecimal representation of the sequence number from the end of the buffer
	hexSeq := string(buf[len(buf)-3:])
	// Decode the hexadecimal string to uint64
	seqNum, err := strconv.ParseUint(hexSeq, 16, 64)
	if err != nil {
		// Handle error, returning 0 and original buffer
		return 0, buf
	}

	// Return the decoded sequence number
	return seqNum, buf[:len(buf)-3]
}
