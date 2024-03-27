package utils

import (
	"github.com/shirou/gopsutil/disk"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

type DiskInfo struct {
	TotalSpace uint64
	FreeSpace  uint64
}

// GetDiskInfo 获取所有磁盘的分区信息
func GetDiskInfo(path string) (*DiskInfo, error) {
	// 获取指定路径分区的磁盘使用情况
	usageStat, err := disk.Usage(path)
	if err != nil {
		return nil, err
	}
	info := &DiskInfo{
		TotalSpace: usageStat.Total,
		FreeSpace:  usageStat.Free,
	}
	return info, nil
}

func GetDiskSize(pathSize string) (int64, error) {
	return getDirSize(pathSize)
}
func getDirSize(pathSize string) (int64, error) {
	if _, err := os.Stat(pathSize); os.IsNotExist(err) {
		return 0, nil
	}
	dirs, err := os.ReadDir(pathSize)
	if err != nil {
		return 0, err
	}
	var size int64
	for _, dir := range dirs {
		info, err := dir.Info()
		if err != nil {
			return 0, err
		}
		size += info.Size()
	}
	return size, nil
}
func CopyDir(src, dst string) error {
	if _, err := os.Stat(src); os.IsNotExist(err) {
		return nil
	}
	if _, err := os.Stat(dst); os.IsExist(err) {
		if err := os.RemoveAll(dst); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(dst, os.ModePerm); err != nil {
		return err
	}
	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dst, filepath.Clean(path))
		if info.IsDir() {
			if err := os.MkdirAll(dstPath, info.Mode()); err != nil {
				return err
			}
		} else {
			srcFile, err := os.Open(path)
			if err != nil {
				return err
			}
			defer srcFile.Close()

			dstFile, err := os.Create(dstPath)
			if err != nil {
				return err
			}
			defer dstFile.Close()

			if _, err := io.Copy(dstFile, srcFile); err != nil {
				return err
			}
		}
		return nil
	})
}
