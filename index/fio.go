package index

import (
	"os"
)

type FileIoInterface interface {
	Write([]byte) (n int, err error)
	Read(b []byte) (n int, err error)
	ReadAt(b []byte, off int64) (n int, err error)
	Close() error
	Sync() error
	Size() (int64, error)
}

type FileioType = uint8

const (
	BaseFileIoType FileioType = iota
	MMap
)

func FileIos(fileName string, filoType FileioType) FileIoInterface {
	return NewBaseFileIo(fileName)
}

type BaseFileIo struct {
	fio *os.File
}

func NewBaseFileIo(fileName string) FileIoInterface {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		panic(err)
	}
	return &BaseFileIo{
		fio: f,
	}
}
func (f *BaseFileIo) Write(data []byte) (n int, err error) {
	return f.fio.Write(data)
}
func (f *BaseFileIo) Read(b []byte) (n int, err error) {
	return f.fio.Read(b)
}
func (f *BaseFileIo) ReadAt(b []byte, off int64) (n int, err error) {
	return f.fio.ReadAt(b, off)
}
func (f *BaseFileIo) Close() error {
	return f.fio.Close()
}
func (f *BaseFileIo) Sync() error {
	return f.fio.Sync()
}
func (f *BaseFileIo) Size() (int64, error) {
	stat, err := f.fio.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
