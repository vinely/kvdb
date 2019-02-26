package db

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

var (
	testredisdb *RedisDB
)

func initRedisDB() error {
	// testredisdb = &RedisDB{
	// 	Address:  "localhost:6379",
	// 	Password: "",
	// 	HashKey:  "testredisdb",
	// 	DB:       0,
	// 	Count:    20,
	// }
	// return testredisdb.Setup()
	db, err := NewKVDataBase("redis://localhost:6379/serv?count=20")
	if err != nil {
		return err
	}
	testredisdb = db.(*RedisDB)
	return nil

}

func TestRedisDB_Set(t *testing.T) {
	err := initRedisDB()
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < 50; i++ {
		err = testredisdb.Set(&KVData{"key" + strconv.Itoa(i), []byte("value" + strconv.Itoa(i))})
		if err != nil {
			fmt.Println(err)
		}
	}
}

func TestRedisDB_Get(t *testing.T) {
	err := initRedisDB()
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < 50; i++ {
		kvi := testredisdb.Get("key" + strconv.Itoa(i))
		if err != nil {
			fmt.Println(err)
		}
		if kvi.Result && kvi.Len() >= 1 {
			fmt.Println(string(kvi.Data[0].Value))
		}
	}
}

func TestRedisDB_KeyCount(t *testing.T) {
	err := initRedisDB()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(testredisdb.KeyCount())
}

func TestRedisDB_ListKeys(t *testing.T) {
	err := initRedisDB()
	if err != nil {
		fmt.Println(err)
	}
	res := testredisdb.ListKeys(1)
	fmt.Println(res)
}

func TestRedisDB_FindOne(t *testing.T) {
	err := initRedisDB()
	if err != nil {
		fmt.Println(err)
	}
	v := testredisdb.FindOne(func(k, v []byte) *KVInfo {
		if strings.Compare(string(k), "key11") == 0 {
			return &KVInfo{
				Data: []KVData{
					{string(k), v},
				},
				Result: true,
				Info:   "",
			}
		}
		return &KVInfo{
			Result: false,
		}
	})
	fmt.Println(string(v.Data[0].Value))
}

func TestRedisDB_List(t *testing.T) {
	err := initRedisDB()
	if err != nil {
		fmt.Println(err)
	}
	v := testredisdb.List(0, func(k, v []byte) *KVInfo {
		if strings.Contains(string(k), "key1") {
			return &KVInfo{
				Data: []KVData{
					{string(k), v},
				},
				Result: true,
				Info:   "",
			}
		}
		return &KVInfo{
			Result: false,
		}
	})
	fmt.Printf("%v\n", v.Data)
}
