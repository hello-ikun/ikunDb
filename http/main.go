package main

import (
	"encoding/json"
	"fmt"
	"github.com/hello-ikun/ikunDb/engine"
	"github.com/hello-ikun/ikunDb/options"
	"github.com/hello-ikun/ikunDb/storage"
	"log"
	"net/http"
	"os"
)

var db *engine.IkunDB

func init() {
	var err error
	options := options.DefaultOptions
	dir, _ := os.MkdirTemp("", "ikunDb-go-http")
	options.DirPath = dir
	db, err = engine.OpenIkunDb(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
}

func handleSet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var kv map[string]string

	if err := json.NewDecoder(request.Body).Decode(&kv); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range kv {
		if err := db.Set([]byte(key), []byte(value), 0); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put kv in db: %v\n", err)
			return
		}
	}
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")

	value, err := db.Get([]byte(key))
	if err != nil && err != storage.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get kv in db: %v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

func handleDel(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")

	err := db.Del([]byte(key))
	if err != nil && err != storage.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get kv in db: %v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}

func handleListKey(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	keys := db.ListKey()
	writer.Header().Set("Content-Type", "application/json")
	var result []string
	for _, k := range keys {
		result = append(result, string(k))
	}
	_ = json.NewEncoder(writer).Encode(result)
}

func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stat, err := db.Stat()
	if err != nil {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(err.Error())
	} else {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(stat)
	}
}

func main() {
	// 注册处理方法
	http.HandleFunc("/ikunDb/set", handleSet)
	http.HandleFunc("/ikunDb/get", handleGet)
	http.HandleFunc("/ikunDb/delete", handleDel)
	http.HandleFunc("/ikunDb/listkey", handleListKey)
	http.HandleFunc("/ikunDb/stat", handleStat)

	// 启动 HTTP 服务
	_ = http.ListenAndServe("localhost:8080", nil)
}
