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
	db, err := NewKVDataBase("bolt://service.db/service?count=20&path=./base")
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
		ret := TestBoltDB.Set(&KVData{"key" + strconv.Itoa(i), []byte("value" + strconv.Itoa(i))})
		if ret.Result {
			d := ret.Data.(*KVData)
			fmt.Println(string(d.Value))
		}
	}
}

func TestBoltDB_Get(t *testing.T) {
	err := initBoltDB()
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < 50; i++ {
		kvr := TestBoltDB.Get("key" + strconv.Itoa(i))
		if err != nil {
			fmt.Println(err)
		}
		if kvr.Result {
			v := kvr.Data.([]byte)
			fmt.Println(string(v))
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
	v := TestBoltDB.FindOne(func(k, v []byte) *KVResult {
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

func TestBoltDB_List(t *testing.T) {
	err := initBoltDB()
	if err != nil {
		fmt.Println(err)
	}
	v := TestBoltDB.List(0, func(k, v []byte) *KVResult {
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
