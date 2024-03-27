package utils

import (
	"testing"
)

func TestDisk(t *testing.T) {
	info, err := GetDiskInfo("./")
	t.Log(info, err)
	t.Log(info.TotalSpace / 1024 / 1024 / 1024)
	// assert.Nil(t, err)

	//copy("../O")
}
