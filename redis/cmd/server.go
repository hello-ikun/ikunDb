package main

import (
	"log"
	"strings"
	"sync"

	"github.com/hello-ikun/ikunDb/redis"
	"github.com/tidwall/redcon"
)

var addr = "127.0.0.1:6380"

type IkunDbServer struct {
	dbs    map[int]*redis.RedisDataStruct
	server *redcon.Server
	mu     sync.RWMutex
}

func (i *IkunDbServer) listen() {
	log.Println("IkunDb server running, ready to accept connections.")
	_ = i.server.ListenAndServe()
}
func (i *IkunDbServer) accept(conn redcon.Conn) bool {
	cli := new(IkunDbClient)
	i.mu.Lock()
	defer i.mu.Unlock()

	cli.server = i
	cli.db = i.dbs[0]
	conn.SetContext(cli)
	return true
}
func (i *IkunDbServer) close(conn redcon.Conn, err error) {
	for _, db := range i.dbs {
		_ = db.Close()
	}
	_ = i.server.Close()
}
func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))

	client, _ := conn.Context().(*IkunDbClient)
	switch command {
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		cmdFunc, ok := supportedCommands[command]
		if !ok {
			conn.WriteError("Err unsupported command: '" + command + "'")
			return
		}
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteAny(res)
	}
}

func main() {
	redisData := redis.DefaultRedis()
	ikunServer := &IkunDbServer{dbs: make(map[int]*redis.RedisDataStruct)}
	ikunServer.dbs[0] = redisData
	ikunServer.server = redcon.NewServer(addr, execClientCommand, ikunServer.accept, ikunServer.close)
	ikunServer.listen()
}
