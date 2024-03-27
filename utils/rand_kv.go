package utils

import (
	r1 "crypto/rand"
	"fmt"
	"math/big"
	"math/rand"
	"time"
)

const (
	charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

// GenerateRandomString generates a random string of specified length
func GenerateRandomString(length int) string {
	return string(GenerateRandomByte(length))
}

// GenerateRandomByte generates a random string of specified length
func GenerateRandomByte(length int) []byte {
	return secureRandStr(length)
}
func GenerateTestKey(i int) []byte {
	return []byte(fmt.Sprintf("ikunDb-test-key:%09d", i))
}

func secureRandStr(n int) []byte {
	charsetLen := big.NewInt(int64(len(charset)))
	b := make([]byte, n)
	for i := range b {
		randomIndex, err := r1.Int(r1.Reader, charsetLen)
		if err != nil {
			panic(err)
		}
		b[i] = charset[randomIndex.Int64()]
	}
	return b
}
func SecureRandomByte(n int) []byte {
	return secureRandStr(n)
}

var (
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// GetTestKey get random key
func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("ikunDb-go-key-%09d", i))
}

// RandomValue get random value
func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("ikunDb-go-value-" + string(b))
}
