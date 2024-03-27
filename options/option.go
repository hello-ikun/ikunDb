package options

import (
	"github.com/hello-ikun/ikunDb/index"
)

// Options set options
type Options struct {
	DirPath         string
	MemoryIndexType index.IndexerType
	MaxDataSize     int64
	MergePath       string
	SyncWrites      bool
}

var DefaultOptions = Options{
	DirPath:         "./temp",
	MergePath:       "./temp-merge",
	MemoryIndexType: index.Base,
	MaxDataSize:     1024,
	SyncWrites:      true,
}
var BenchMarkOptions = Options{
	DirPath:         "./temp",
	MergePath:       "./temp-merge",
	MemoryIndexType: index.Art,
	MaxDataSize:     256 * 1024 * 1024, // 256MB
	SyncWrites:      false,
}

type BatchOptions struct {
	MaxBatchNum uint64
	SyncWrites  bool
}

var DefaultBatchOptions = BatchOptions{
	MaxBatchNum: 1000,
	SyncWrites:  true,
}

const ReDiskSize = 1024 * 1024 * 1024
