package index

import (
	"github.com/hello-ikun/ikunDb/storage"
	"github.com/hello-ikun/ikunDb/utils"
	"testing"
)

func TestIndex(t *testing.T) {
	// var indexType IndexerType
	for i := 0; i < 4; i++ {
		fn := Indexers[uint8(i)]
		for i := 0; i < 100; i++ {
			key := utils.GetTestKey(i)
			fn.Set(key, &storage.Pos{Fid: 1, Offset: 2, Length: 3})
		}
		iter := fn.Iterator()
		for iter.Head(); iter.Valid(); iter.Next() {
			t.Log(string(iter.Key()), iter.Value())
		}
		t.Log(fn.Size())
	}

}
