package db

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

var (
	TestBoltDB *BoltDB
)

func initBoltDB() error {
	db, err := NewKVDataBase("bolt://service.db/service?count=20")
	if err != nil {
		return err
	}
	TestBoltDB = db.(*BoltDB)
	return nil
}

func TestBoltDB_Set(t *testing.T) {
	err := initBoltDB()
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < 50; i++ {
		err = TestBoltDB.Set(&KVData{"key" + strconv.Itoa(i), []byte("value" + strconv.Itoa(i))})
		if err != nil {
			fmt.Println(err)
		}
	}
}

func TestBoltDB_Get(t *testing.T) {
	err := initBoltDB()
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < 50; i++ {
		kvi := TestBoltDB.Get("key" + strconv.Itoa(i))
		if err != nil {
			fmt.Println(err)
		}
		if kvi.Result && kvi.Len() >= 1 {
			fmt.Println(string(kvi.Data[0].Value))
		}

	}
}

func TestBoltDB_KeyCount(t *testing.T) {
	err := initBoltDB()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(TestBoltDB.KeyCount())
}

func TestBoltDB_ListKeys(t *testing.T) {
	err := initBoltDB()
	if err != nil {
		fmt.Println(err)
	}
	res := TestBoltDB.ListKeys(1)
	fmt.Println(res)
}

func TestBoltDB_FindOne(t *testing.T) {
	err := initBoltDB()
	if err != nil {
		fmt.Println(err)
	}
	v := TestBoltDB.FindOne(func(k, v []byte) *KVInfo {
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

func TestBoltDB_List(t *testing.T) {
	err := initBoltDB()
	if err != nil {
		fmt.Println(err)
	}
	v := TestBoltDB.List(0, func(k, v []byte) *KVInfo {
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
