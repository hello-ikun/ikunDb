package index

import (
	"fmt"
	"path/filepath"
)

// 实现文件的抽象
type Data struct {
	Fid    uint64
	Offset uint64
	Fio    FileIoInterface
}

const (
	DataFileNameSuffix = ".ikun"
)

type CodeFlag = byte

const (
	Normal CodeFlag = iota
	Delete
	TxFinish
)

func newFileName(dirPath string, fileID uint64) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+DataFileNameSuffix, fileID))
}

func GetMergeName(dirPath string, fileID uint64) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+DataFileNameSuffix, fileID))
}
func OpenMergeFile(dirPath string, fileID uint64) (*Data, error) {
	return OpenDataFile(dirPath, fileID)
}
func OpenDataFile(dirPath string, fileID uint64) (*Data, error) {
	fileName := newFileName(dirPath, fileID)
	return NewData(fileName, fileID, BaseFileIoType)
}

func NewData(fileName string, fid uint64, fileIoType FileioType) (*Data, error) {
	return &Data{
		fid, 0, FileIos(fileName, fileIoType),
	}, nil
}
func (d *Data) Write(data []byte) (n int, err error) {
	return d.Fio.Write(data)
}
func (d *Data) Sync() error {
	return d.Fio.Sync()
}
func (d *Data) Close() error {
	return d.Fio.Close()
}
