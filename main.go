package main

import (
	"fmt"
	"ikunDb/engine"
)

func main() {
	db := engine.NewIkunDb()
	fmt.Println(db.GetSeqNumber())
	//batch := db.DefaultBathch()
	//for i := 0; i < 150; i++ {
	//	key := utils.GenerateTestKey(i)
	//	val := utils.SecureRandomByte(12)
	//	//if err := db.Set(key, val, 1); err != nil {
	//	//	panic(err)
	//	//}
	//	batch.Set(key, val, 0)
	//}
	//batch.Commit()=
	//err := db.Merge()
	//if err != nil {
	//	fmt.Println(err)
	//}
	//fmt.Println(db.GetSeqNumber())
	//for _, v := range db.ListKey() {
	//	fmt.Println(string(v))
	//}
	//fmt.Println(len(db.ListKey()))
	//db.Fold(func(key, val []byte) bool {
	//	fmt.Println(string(key), string(val))
	//	return true
	//})
	//db.BackUp("./test")

	// batch := db.NewIkunDbBatch()
	// for i := 100; i < 200; i++ {
	// 	key := utils.GenerateTestKey(i)
	// 	val, err := db.Get(key)
	// 	fmt.Printf("%s:%s:%v\n", key, val, err)
	// 	// val := utils.SecureRandomByte(12)
	// 	// fmt.Printf("%s:%s\n", key, val)
	// 	// err := db.Set(key, val)
	// 	// if err != nil {
	// 	// 	fmt.Println(err)
	// 	// }
	// }
	// for i := 0; i < 150; i++ {
	// 	key := utils.GenerateTestKey(i)
	// 	// val, err := db.Get(key)
	// 	val := utils.SecureRandomByte(12)
	// 	batch.Set(key, val)
	// }
	// batch.Commit()
}
