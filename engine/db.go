package engine

import (
	"errors"
	"ikunDb/index"
	"ikunDb/options"
	"ikunDb/storage"
	"ikunDb/utils"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

type IkunDB struct {
	options       options.Options
	mu            *sync.RWMutex
	memoryIndex   index.IndexInterface
	activeFiles   *index.Data
	olderFiles    map[uint64]*index.Data
	initialFlag   bool
	fileIds       []int
	seqNum        uint64
	nonMergeFlag  bool
	mergeEnd      uint64
	hasMaxNumber  uint64
	needMergeSize uint64
	fileLock      *flock.Flock
}

var (
	ErrDirPath         = errors.New("error dir path")
	ErrFileSize        = errors.New("error file size")
	ErrPosNil          = errors.New("error pos")
	ErrKeyNotFound     = errors.New("error key not found")
	ErrDataNil         = errors.New("error data")
	ErrSet             = errors.New("error set key")
	ErrDel             = errors.New("error delete key")
	ErrTimeOut         = errors.New("error time out")
	ErrDatabaseIsUsing = errors.New("error database used by other")
	fileLockName       = "fileLock"
)

func (db *IkunDB) setFileLock() error {
	// 判断当前数据目录是否正在使用
	fileLock := flock.New(filepath.Join(db.options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return err
	}
	if !hold {
		return ErrDatabaseIsUsing
	}
	db.fileLock = fileLock
	return nil
}
func OpenIkunDb(option options.Options) (*IkunDB, error) {
	if err := checkOptions(option); err != nil {
		return nil, err
	}

	ikunDb := &IkunDB{
		options:     option,
		mu:          &sync.RWMutex{},
		memoryIndex: index.Indexers[option.MemoryIndexType],
		olderFiles:  make(map[uint64]*index.Data),
	}
	if err := ikunDb.setFileLock(); err != nil {
		return nil, err
	}
	// set initial flag
	if err := ikunDb.setInitialFlag(); err != nil {
		return nil, err
	}
	if err := ikunDb.setMergeFlag(); err != nil {
		return nil, err
	}
	if err := ikunDb.loadMerge(); err != nil {
		return nil, err
	}
	if err := ikunDb.loadDataFiles(); err != nil {
		return nil, err
	}
	if err := ikunDb.loadMemoryIndex(); err != nil {
		return nil, err
	}
	return ikunDb, nil
}
func NewIkunDb() *IkunDB {
	defaultOptions := options.DefaultOptions
	ikun, err := OpenIkunDb(defaultOptions)
	if err != nil {
		panic(err)
	}
	return ikun
}
func NewBenchMarkIkunDb() *IkunDB {
	defaultOptions := options.BenchMarkOptions
	defaultOptions.MemoryIndexType = index.None
	ikun, err := OpenIkunDb(defaultOptions)
	if err != nil {
		panic(err)
	}
	return ikun
}
func (db *IkunDB) GetSeqNumber() uint64 {
	return db.seqNum
}

// BackUp backup data
func (db *IkunDB) BackUp(path string) error {
	if err := utils.CopyDir(db.options.DirPath, path); err != nil {
		return err
	}
	if err := utils.CopyDir(db.options.MergePath, path); err != nil {
		return err
	}
	return nil
}

// ListKey get all keys[包含可能超时的]
func (db *IkunDB) ListKey() [][]byte {
	var ans [][]byte
	iter := db.memoryIndex.Iterator()
	for iter.Head(); iter.Valid(); iter.Next() {
		ans = append(ans, iter.Key())
	}
	return ans
}

// Fold use function
func (db *IkunDB) Fold(fn func(key, val []byte) bool) error {
	db.mu.RLock()
	iter := db.memoryIndex.Iterator()
	db.mu.RUnlock()

	for iter.Head(); iter.Valid(); iter.Next() {
		val, err := db.getValByPos(iter.Value())
		if errors.Is(err, ErrTimeOut) {
			continue
		}
		if err != nil {
			return err
		}
		if !fn(iter.Key(), val) {
			break
		}
	}
	return nil
}

// Set set key and value
func (db *IkunDB) Set(key, value []byte, duration time.Duration) error {
	encKey := EncodeSeqNum(key, uint64(NonSeqNum))
	pos, err := db.appendDataWithLock(&storage.EncodeStorage{
		Key:          encKey,
		Value:        value,
		Flag:         index.Normal,
		TimeDuration: duration,
	})
	if err != nil {
		return err
	}
	// set
	old, ok := db.memoryIndex.Set(key, pos)
	if !ok {
		return ErrSet
	}
	// add need resize
	if old != nil {
		db.needMergeSize += old.Length
	}
	return nil
}

// Get value by key
func (db *IkunDB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	pos, err := db.memoryIndex.Get(key)
	if err != nil {
		return nil, err
	}
	if pos == nil {
		return nil, ErrKeyNotFound
	}
	val, err := db.getValByPos(pos)
	if errors.Is(err, ErrTimeOut) {
		db.needMergeSize += pos.Length
		return nil, nil
	}
	return val, err
}

func (db *IkunDB) getValByPos(pos *storage.Pos) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if pos == nil {
		return nil, ErrPosNil
	}
	var data *index.Data
	if pos.Fid == db.activeFiles.Fid {
		data = db.activeFiles
	} else {
		data = db.olderFiles[pos.Fid]
	}
	if data == nil {
		return nil, ErrDataNil
	}
	buf := make([]byte, pos.Length)
	if _, err := data.Fio.ReadAt(buf, int64(pos.Offset)); err != nil {
		return nil, err
	}
	// just get value
	info, err := storage.DecodeStoage(buf)
	if err != nil {
		return nil, err
	}
	if info.Flag == index.Delete {
		return nil, ErrDataNil
	}
	if info.TimeStamp == -1 || info.TimeStamp > time.Now().Unix() {
		return info.Value, nil
	}
	return nil, ErrTimeOut
}

// Del del
func (db *IkunDB) Del(key []byte) error {
	db.mu.Lock()

	// check key exist
	_, ok := db.memoryIndex.Del(key)
	if !ok {
		db.mu.Unlock()
		return storage.ErrKeyNotFound
	}

	db.mu.Unlock()

	// append record
	pos, err := db.appendDataWithLock(&storage.EncodeStorage{
		Key:          key,
		Value:        nil,
		Flag:         index.Delete,
		TimeDuration: 0,
	})
	if err != nil {
		return err
	}
	if pos != nil {
		db.needMergeSize += pos.Length
	}
	// del
	old, ok := db.memoryIndex.Del(key)
	if !ok {
		return ErrDel
	}
	if old != nil {
		db.needMergeSize += old.Length
	}

	return nil
}

// Sync sync
func (db *IkunDB) Sync() error {
	if db.activeFiles != nil {
		if err := db.activeFiles.Sync(); err != nil {
			return err
		}
		return nil
	}
	return nil
}

// Close close file
func (db *IkunDB) Close() error {
	db.mu = nil

	// close files
	_ = db.activeFiles.Close()
	for _, olderFile := range db.olderFiles {
		_ = olderFile.Close()
	}
	// close memory index
	_ = db.memoryIndex.Close()

	// close and delete fileLock file
	_ = db.fileLock.Close()
	_ = os.RemoveAll(filepath.Join(db.options.DirPath, fileLockName))
	db.fileIds = nil
	return nil
}

// append data with lock
func (db *IkunDB) appendDataWithLock(encodeStorage *storage.EncodeStorage) (*storage.Pos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendData(encodeStorage)
}

// append data todo：add time
func (db *IkunDB) appendData(encodeStorage *storage.EncodeStorage) (*storage.Pos, error) {
	if db.activeFiles == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}
	// encode key +value + timeDuration to data
	data, err := storage.Encode(encodeStorage)
	if err != nil {
		return nil, err
	}
	length := len(data)
	if db.activeFiles.Offset+uint64(length) > uint64(db.options.MaxDataSize) {
		if err := db.activeFiles.Sync(); err != nil {
			return nil, err
		}
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}
	length, err = db.activeFiles.Write(data)
	if err != nil {
		return nil, err
	}
	if db.options.SyncWrites {
		if err := db.activeFiles.Sync(); err != nil {
			return nil, err
		}
	}
	pos := &storage.Pos{Fid: db.activeFiles.Fid, Offset: db.activeFiles.Offset, Length: uint64(length)}
	db.activeFiles.Offset += pos.Length
	return pos, nil
}

// load file
func (db *IkunDB) loadDataFiles() error {
	if db.initialFlag {
		if err := db.setActiveFile(); err != nil {
			return err
		}
		return nil
	}
	dirs, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	for _, dir := range dirs {
		if strings.HasSuffix(dir.Name(), index.DataFileNameSuffix) {
			fileNameId := strings.Split(dir.Name(), index.DataFileNameSuffix)[0]
			fileId, err := strconv.Atoi(fileNameId)
			if err != nil {
				return err
			}
			fileIds = append(fileIds, fileId)
		}
	}
	sort.Ints(fileIds)
	db.fileIds = fileIds

	for k, v := range db.fileIds {
		if v <= int(db.hasMaxNumber) {
			continue
		}
		data, err := index.OpenDataFile(db.options.DirPath, uint64(v))
		if err != nil {
			return err
		}
		if k == len(db.fileIds)-1 {
			db.activeFiles = data
		} else {
			db.olderFiles[uint64(v)] = data
		}
	}
	return nil
}

type txn struct {
	key      []byte
	pos      *storage.Pos
	codeFlag index.CodeFlag
}

// load memory index
func (db *IkunDB) loadMemoryIndex() error {
	// if db first use not to load memory index
	// because data is none
	if db.initialFlag {
		return nil
	}
	txnInfo := map[uint64][]*txn{}
	update := func(txn *txn) error {
		seqNum, key := DecodeSeqNum(txn.key)

		db.seqNum = max(seqNum, db.seqNum)

		if txn.codeFlag == index.TxFinish {
			if txn.codeFlag == index.TxFinish {
				for _, tx := range txnInfo[seqNum] {
					if tx.codeFlag == index.Normal {
						old, ok := db.memoryIndex.Set(tx.key, tx.pos)
						if !ok {
							return ErrSet
						}
						if old != nil {
							db.needMergeSize += old.Length
						}
					}
					if tx.codeFlag == index.Delete {
						old, ok := db.memoryIndex.Del(tx.key)
						if !ok {
							return ErrSet
						}
						if old != nil {
							db.needMergeSize += old.Length
						}
						db.needMergeSize += tx.pos.Length
					}
				}
				txnInfo[seqNum] = nil
			}
		} else {
			if seqNum == uint64(NonSeqNum) {
				if txn.codeFlag == index.Normal {
					old, ok := db.memoryIndex.Set(key, txn.pos)
					if !ok {
						return ErrSet
					}
					if old != nil {
						db.needMergeSize += old.Length
					}
				}
				if txn.codeFlag == index.Delete {
					old, ok := db.memoryIndex.Del(key)
					if !ok {
						return ErrSet
					}
					if old != nil {
						db.needMergeSize += old.Length
					}
					db.needMergeSize += txn.pos.Length
				}
			} else {
				txn.key = key
				txnInfo[seqNum] = append(txnInfo[seqNum], txn)
			}
		}
		return nil
	}
	// load file index
	for k, v := range db.fileIds {
		var data *index.Data
		if k == len(db.fileIds)-1 {
			data = db.activeFiles
		} else {
			data = db.olderFiles[uint64(v)]
		}
		if data == nil {
			continue
		}

		dataSize, err := data.Fio.Size()
		if err != nil {
			return err
		}
		if dataSize == 0 {
			continue
		}
		// use file to get data
		offset := 0
		endFlag := false
		for {
			maxLen := storage.MaxStorageLength
			if int(dataSize) < offset+maxLen {
				maxLen = int(dataSize) - offset
			}
			if maxLen == 0 {
				break
			}
			//read to data
			buf := make([]byte, maxLen)
			if _, err := data.Fio.ReadAt(buf, int64(offset)); err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			//fmt.Println(buf, string(buf), maxLen)
			info, err := storage.DecodeDetail(buf)
			if err != nil {
				return err
			}
			offset += info.Length
			timeStamp := info.TimeStamp
			if timeStamp <= 0 || timeStamp > time.Now().Unix() {
				//read key and value
				kvBuf := make([]byte, info.KeyLen+info.ValueLen)
				if _, err := data.Fio.ReadAt(kvBuf, int64(offset)); err != nil {
					if err == io.EOF {
						break
					}
					return err
				}

				// check flag if normal insert data else delete
				key := kvBuf[:info.KeyLen]
				err = update(&txn{key, &storage.Pos{Offset: uint64(offset - info.Length), Length: uint64(info.Length + info.KeyLen + info.ValueLen), Fid: uint64(v)}, info.Flag})
				if err != nil {
					return err
				}
				if endFlag {
					// set offset
					if data == db.activeFiles {
						db.activeFiles.Offset = uint64(offset)
					}
					break
				}
			}
			offset += info.KeyLen + info.ValueLen
		}
	}
	return nil
}

// set initial flag
func (db *IkunDB) setInitialFlag() error {
	flag, err := db.checkFlag(db.options.DirPath)
	if err != nil {
		return err
	}
	db.initialFlag = flag
	return nil
}

// load index from file
func (db *IkunDB) setActiveFile() error {
	var fileId uint64
	if db.activeFiles != nil {
		fileId = db.activeFiles.Fid + 1
		db.olderFiles[db.activeFiles.Fid] = db.activeFiles
	}

	data, err := index.OpenDataFile(db.options.DirPath, fileId)
	if err != nil {
		return err
	}
	db.activeFiles = data
	return nil
}

// check option
func checkOptions(option options.Options) error {
	if option.DirPath == "" {
		return ErrDirPath
	}
	if _, err := os.Stat(option.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(option.DirPath, os.ModePerm); err != nil {
			return err
		}
	}
	if option.MaxDataSize <= 0 {
		return ErrFileSize
	}
	return nil
}

type IkunDbStat struct {
	KeyNum        int64
	DbSize        int64
	NeedMergeData int64
	UseFiles      int
}

func (db *IkunDB) Stat() (*IkunDbStat, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	fileCount := len(db.olderFiles)
	if db.activeFiles != nil {
		fileCount += 1
	}
	dbSize, err := db.getDbSize()
	if err != nil {
		return nil, err
	}
	return &IkunDbStat{
			KeyNum:        db.memoryIndex.Size(),
			DbSize:        dbSize,
			NeedMergeData: int64(db.needMergeSize),
			UseFiles:      fileCount},
		nil
}
