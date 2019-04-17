package db

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

var (
	testMemDB *MemBucket
)

func initMemDB() error {
	db, err := NewKVDataBase("mem://abs/serv?count=20")
	if err != nil {
		return err
	}
	testMemDB = db.(*MemBucket)
	return nil

}

func TestMemDB_Set(t *testing.T) {
	err := initMemDB()
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < 50; i++ {
		ret := testMemDB.Set(&KVData{"key" + strconv.Itoa(i), []byte("value" + strconv.Itoa(i))})
		if ret.Result {
			d := ret.Data.(*KVData)
			fmt.Println(string(d.Value))
		}
	}
}

func TestMemDB_Get(t *testing.T) {
	err := initMemDB()
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < 50; i++ {
		kvr := testMemDB.Get("key" + strconv.Itoa(i))
		if err != nil {
			fmt.Println(err)
		}
		if kvr.Result {
			fmt.Println(string(kvr.Data.([]byte)))
		}
	}
}

func TestMemDB_KeyCount(t *testing.T) {
	err := initMemDB()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(testMemDB.KeyCount())
}

func TestMemDB_ListKeys(t *testing.T) {
	err := initMemDB()
	if err != nil {
		fmt.Println(err)
	}
	res := testMemDB.ListKeys(1)
	fmt.Println(res)
}

func TestMemDB_FindOne(t *testing.T) {
	err := initMemDB()
	if err != nil {
		fmt.Println(err)
	}
	v := testMemDB.FindOne(func(k, v []byte) *KVResult {
		if strings.Compare(string(k), "key11") == 0 {
			return &KVResult{
				Data:   v,
				Result: true,
				Info:   "",
			}
		}
		return &KVResult{
			Result: false,
		}
	})
	d := v.Data.([]byte)
	fmt.Println(string(d))
}

func TestMemDB_List(t *testing.T) {
	err := initMemDB()
	if err != nil {
		fmt.Println(err)
	}
	v := testMemDB.List(0, func(k, v []byte) *KVResult {
		if strings.Contains(string(k), "key1") {
			return &KVResult{
				Data:   v,
				Result: true,
				Info:   "",
			}
		}
		return &KVResult{
			Result: false,
		}
	})
	var str []string
	d := v.Data.([]interface{})
	for _, b := range d {
		str = append(str, string(b.([]byte)))
	}
	fmt.Println(str)
}
