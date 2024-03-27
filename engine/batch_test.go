package engine

import (
	"fmt"
	"ikunDb/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatch(t *testing.T) {
	keys := utils.GenerateTestKey(12)
	buf := EncodeSeqNum(keys, 12)
	seqNum, data := DecodeSeqNum(buf)
	assert.Equal(t, int(seqNum), 12)
	assert.Equal(t, data, keys)
}
func TestBatch1(t *testing.T) {
	db := NewIkunDb()
	for i := 0; i < 100; i++ {
		key := utils.GenerateTestKey(i)
		val, err := db.Get(key)
		assert.Equal(t, err, ErrKeyNotFound)
		t.Log(string(key), string(val))
	}
	t.Log(db.GetSeqNumber())
	db.Fold(func(key, val []byte) bool {
		fmt.Printf("%s:%s\n", key, val)
		return true
	})
	t.Log(db.GetSeqNumber())
	assert.Equal(t, 0, len(db.ListKey()))
}
func TestBatch2(t *testing.T) {
	db := NewIkunDb()
	t.Log(db.Stat())
	batch := db.DefaultBathch()
	for i := 0; i < 100; i++ {
		key := utils.GenerateTestKey(i)
		val := utils.GenerateRandomByte(12)
		err := batch.Set(key, val, 0)
		assert.Nil(t, err)
	}
	// t.Log(db.GetSeqNumber())
	batch.Commit()
	db.Fold(func(key, val []byte) bool {
		fmt.Printf("%s:%s\n", key, val)
		return true
	})
	t.Log(db.GetSeqNumber())
	t.Log(db.Stat())
	db.Merge()
	t.Log(db.Stat())
	assert.Equal(t, 100, len(db.ListKey()))
	db.Close()
}
func TestBatchEncode(t *testing.T) {
	key := utils.GenerateTestKey(12)
	buf := EncodeSeqNum(key, 42342425252512)
	seq, info := DecodeSeqNum(buf)
	t.Log(key, seq, info, buf)
}
func TestBatchEncode1(t *testing.T) {
	// fd, err := os.Open("./temp/000000002.ikun")
	// assert.Nil(t, err)
	// buf := make([]byte, storage.MaxStorageLength)
	// fd.ReadAt(buf, 649)
	// fmt.Println(buf, string(buf))
	// info, err := storage.DecodeDetail(buf)
	// assert.Nil(t, err)
	// fmt.Println(info)
	//load("./temp/000000001.ikun")
}
