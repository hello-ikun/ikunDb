package engine

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hello-ikun/ikunDb/index"
	"github.com/hello-ikun/ikunDb/options"
	"github.com/hello-ikun/ikunDb/storage"
	"github.com/hello-ikun/ikunDb/utils"
)

const (
	mergeList = "mergeList.kunkun"
)

var (
	ErrMergeSize = errors.New("error Too little disk space")
)

// MergeInfo merge frame
type MergeInfo struct {
	option     options.Options
	mu         *sync.RWMutex
	db         *IkunDB
	activeFile *index.Data
}

func (db *IkunDB) merge() *MergeInfo {
	return &MergeInfo{
		option: db.options,
		mu:     &sync.RWMutex{},
		db:     db,
	}
}
func (db *IkunDB) Merge() error {
	mergeInfo := db.merge()
	return mergeInfo.merge()
}

// merge
func (m *MergeInfo) merge() error {
	if err := m.checkMergeInfo(); err != nil {
		return err
	}

	if err := m.db.activeFiles.Sync(); err != nil {
		return err
	}
	if err := m.db.setActiveFile(); err != nil {
		return err
	}

	if err := m.setActiveFile(); err != nil {
		return err
	}
	startMergeId := m.activeFile.Fid
	iter := m.db.memoryIndex.Iterator()
	for iter.Head(); iter.Valid(); iter.Next() {
		key := iter.Key()
		pos := iter.Value()
		val, err := m.db.getValByPos(pos)
		if errors.Is(err, ErrTimeOut) {
			continue
		}
		if err != nil {
			return err
		}
		err = m.appendMergeWithLock(key, val)
		if err != nil {
			return err
		}
	}

	endMergeId := m.activeFile.Fid
	_ = m.activeFile.Close()

	dbMergeFile := m.db.activeFiles.Fid - 1
	// write to xxx.kunkun
	buf := m.EncodeNumbers(startMergeId, endMergeId, dbMergeFile)
	if err := os.WriteFile(filepath.Join(m.db.options.MergePath, mergeList), buf, os.ModePerm); err != nil {
		return err
	}
	// update db info
	m.db.hasMaxNumber = m.db.activeFiles.Fid - 1
	m.db.mergeEnd = endMergeId
	m.db.needMergeSize = 0

	return nil
}

// EncodeNumbers encode number
func (m *MergeInfo) EncodeNumbers(start, end, maxNum uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64*3)
	idx := 0
	idx += binary.PutUvarint(buf[idx:], start)
	idx += binary.PutUvarint(buf[idx:], end)
	idx += binary.PutUvarint(buf[idx:], maxNum)
	return buf[:idx]
}

// DecodeNumber decode number
func (db *IkunDB) DecodeNumber() (uint64, uint64, uint64, error) {
	fileName := filepath.Join(db.options.MergePath, mergeList)
	buf, err := os.ReadFile(fileName)
	if err != nil {
		return 0, 0, 0, err
	}
	idx := 0
	startId, n := binary.Uvarint(buf[idx:])
	idx += n
	endId, n := binary.Uvarint(buf[idx:])
	idx += n
	maxId, n := binary.Uvarint(buf[idx:])
	idx += n
	return startId, endId, maxId, nil
}

func (m *MergeInfo) appendMergeWithLock(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.appendMerge(key, value)
	return err
}

func (m *MergeInfo) appendMerge(key, value []byte) (*storage.Pos, error) {
	timeDuration := time.Second * 5
	data, err := storage.Encode(&storage.EncodeStorage{
		Key:          EncodeSeqNum(key, uint64(NonSeqNum)),
		Value:        value,
		Flag:         index.Normal,
		TimeDuration: timeDuration,
	})
	if err != nil {
		return nil, err
	}
	length := len(data)
	if m.activeFile.Offset+uint64(length) > uint64(m.option.MaxDataSize) {
		if err := m.activeFile.Sync(); err != nil {
			return nil, err
		}

		if err := m.setActiveFile(); err != nil {
			return nil, err
		}
	}
	length, err = m.activeFile.Write(data)
	if err != nil {
		return nil, err
	}
	pos := &storage.Pos{Fid: m.activeFile.Fid, Offset: m.activeFile.Offset, Length: uint64(length)}
	m.activeFile.Offset += pos.Length
	return pos, nil
}
func (db *IkunDB) checkDisk() error {
	info, err := utils.GetDiskInfo(db.options.DirPath)
	if err != nil {
		return err
	}
	free := info.FreeSpace
	allDbSize, err := db.getDbSize()
	if err != nil {
		return err
	}
	allDbSize -= int64(db.needMergeSize) // merge after data size
	if int64(free) <= allDbSize+int64(options.ReDiskSize) {
		return ErrMergeSize
	}
	return nil
}
func (db *IkunDB) getDbSize() (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var ansSize int64
	size, err := utils.GetDiskSize(db.options.DirPath)
	if err != nil {
		return 0, err
	}
	ansSize += size

	size, err = utils.GetDiskSize(db.options.MergePath)
	if err != nil {
		return 0, err
	}
	ansSize += size
	return ansSize, nil
}
func (m *MergeInfo) checkMergeInfo() error {
	if err := m.db.checkDisk(); err != nil {
		return err
	}
	mergePath := m.db.options.MergePath
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (m *MergeInfo) setActiveFile() error {
	var fileId uint64
	if m.activeFile != nil {
		fileId = m.activeFile.Fid + 1
		_ = m.activeFile.Close()
	} else {
		if !m.db.nonMergeFlag {
			fileId = m.db.mergeEnd + 1
		} else {
			fileId = 1
		}
	}

	data, err := index.OpenMergeFile(m.db.options.MergePath, fileId)
	if err != nil {
		return err
	}
	m.activeFile = data
	return nil
}

func (db *IkunDB) loadMerge() error {
	if db.nonMergeFlag {
		return nil
	}
	start, end, maxNumber, err := db.DecodeNumber()
	db.mergeEnd = end
	db.hasMaxNumber = maxNumber
	if err != nil {
		return err
	}
	var st uint64
	for i := st; i <= end; i++ {
		// lazy delete
		if i < start {
			// delete old merge file
			fileName := index.GetMergeName(db.options.MergePath, i)
			if err := os.RemoveAll(fileName); err != nil {
				return err
			}
			continue
		}
		data, err := index.OpenDataFile(db.options.MergePath, i)
		if err != nil {
			return err
		}
		db.olderFiles[i] = data
	}
	return nil
}

// merge flag
func (db *IkunDB) setMergeFlag() error {
	if db.initialFlag {
		db.nonMergeFlag = true
	} else {
		var flag bool
		if _, err := os.Stat(filepath.Join(db.options.MergePath, mergeList)); os.IsNotExist(err) {
			flag = true
		}
		db.nonMergeFlag = flag
	}
	return nil
}

func (db *IkunDB) checkFlag(path string) (bool, error) {
	var flag = false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		flag = true
	}
	dirs, err := os.ReadDir(path)
	if err != nil {
		return false, err
	}
	if len(dirs) == 0 {
		flag = true
	}
	return flag, nil
}
