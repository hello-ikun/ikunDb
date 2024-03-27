package storage

import (
	"github.com/hello-ikun/ikunDb/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestStorage(t *testing.T) {
	f, err := os.OpenFile("test.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)
	assert.Nil(t, err)
	for i := 0; i < 10; i++ {
		key := utils.GenerateTestKey(i)
		val := utils.GenerateRandomByte(13)
		body, err := Encode(&EncodeStorage{
			Key:          key,
			Value:        val,
			Flag:         byte(1),
			TimeDuration: 1,
		})
		assert.Nil(t, err)
		f.Write(body)
	}
	f.Close()
}
