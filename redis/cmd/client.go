package main

import (
	"fmt"
	"github.com/hello-ikun/ikunDb/redis"

	"github.com/tidwall/redcon"
)

type IkunDbClient struct {
	server *IkunDbServer
	db     *redis.RedisDataStruct
}
type cmdHandler func(cli *IkunDbClient, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	"set": set,
	"get": get,
}

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}
func set(cli *IkunDbClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("set")
	}

	key, value := args[0], args[1]
	if err := cli.db.Set(key, value, 0); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func get(cli *IkunDbClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}

	value, err := cli.db.Get(args[0])
	if err != nil {
		return nil, err
	}
	return value, nil
}
