package redis

import (
	"errors"
	"ikunDb/engine"
	"ikunDb/options"
	"time"
)

type redisDataType = byte

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type RedisDataStruct struct {
	db *engine.IkunDB
}

func NewRedisData(option options.Options) (*RedisDataStruct, error) {
	db, err := engine.OpenIkunDb(option)
	if err != nil {
		return nil, err
	}
	return &RedisDataStruct{db: db}, nil
}
func DefaultRedis() *RedisDataStruct {
	db := engine.NewIkunDb()
	return &RedisDataStruct{db: db}
}
func (r *RedisDataStruct) Close() error {
	return r.db.Close()
}

func (r *RedisDataStruct) Set(key, val []byte, ttl time.Duration) error {
	if val == nil {
		return nil
	}

	// 编码 value : type + expire + payload
	val = append(val, String)
	return r.db.Set(key, val, ttl)
}
func (r *RedisDataStruct) Get(key []byte) ([]byte, error) {
	encValue, err := r.db.Get(key)
	if err != nil {
		return nil, err
	}
	length := len(encValue)
	valType := encValue[length-1]
	if valType != String {
		return nil, ErrWrongTypeOperation
	}
	return encValue[:length-1], nil
}
