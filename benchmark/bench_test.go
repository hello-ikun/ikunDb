package benchmark

import (
	"math/rand"
	"testing"
	"time"

	"github.com/hello-ikun/ikunDb/engine"
	"github.com/hello-ikun/ikunDb/utils"

	"github.com/stretchr/testify/assert"
)

var db *engine.IkunDB

func init() {
	db = engine.NewBenchMarkIkunDb()

}
func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Set(utils.GetTestKey(i), utils.RandomValue(1024), 0)
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		_ = db.Set(utils.GetTestKey(i), utils.RandomValue(1024), 0)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = db.Get(utils.GetTestKey(rand.Int()))
	}
}

func Benchmark_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		_ = db.Del(utils.GetTestKey(rand.Int()))
	}
}
