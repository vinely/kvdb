package db

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"

	"github.com/boltdb/bolt"
	jsoniter "github.com/json-iterator/go"
)

// BoltDB - BoltDB struct
type BoltDB struct {
	KVMethods
	Type   *KVDBType
	DBFile string
	Bucket string
	Count  uint
	DB     *bolt.DB
}

// DefaultBoltDB - get Default Bolt DB
func DefaultBoltDB() *BoltDB {
	return &BoltDB{
		DBFile: "service.db",
		Bucket: "service",
		Count:  50,
	}
}

func init() {
	NewKVDatabaseType("bolt", NewBoltDB)
}

// NewBoltDB - new bolt db using uri format description
// format : bolt://<db file>/<bucket>?[count=]&[path=]
// example bolt://service.db/service?count=20&path=./base
func NewBoltDB(uri string) (KVMethods, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	t := GetKVDatabaseType(u.Scheme)
	if t == nil {
		return nil, errors.New("This type of database [" + t.Scheme + "] didn't exist")
	}
	para := u.Query()
	path := para.Get("path")
	bolt := &BoltDB{
		Type:   t,
		DBFile: filepath.Join(path, u.Host),
		Bucket: filepath.Base(u.Path),
	}

	if para.Get("count") != "" {
		i, _ := strconv.Atoi(para.Get("count"))
		if i <= 0 {
			return nil, errors.New("wrong count parameter")
		}
		bolt.Count = uint(i)
	}

	err = bolt.setup()
	if err != nil {
		return nil, err
	}
	return bolt, nil
}

func (db *BoltDB) setup() error {
	var err error
	err = os.MkdirAll(filepath.Dir(db.DBFile), 0775)
	if err != nil {
		return fmt.Errorf("could create dir, %v", err)
	}
	db.DB, err = bolt.Open(db.DBFile, 0600, nil)
	if err != nil {
		return fmt.Errorf("could not open db, %v", err)
	}
	err = db.DB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(db.Bucket))
		if err != nil {
			return fmt.Errorf("could not create default bucket: %v", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("could not set up default buckets, %v", err)
	}
	// fmt.Println("DB Setup Done")
	return nil
}

// Name - tag  different databases
func (db *BoltDB) Name() string {
	return "Bolt_" + db.Bucket
}

// DBType - DataBase Type
func (db *BoltDB) DBType() *KVDBType {
	return db.Type
}

// Exists - if key existed
func (db *BoltDB) Exists(key string) bool {
	if err := db.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.Bucket))
		if b == nil {
			return fmt.Errorf("could not open bucket, %s", db.Bucket)
		}
		if v := b.Get([]byte(key)); v != nil {
			return nil
		}
		return errors.New("Not Existed")
	}); err != nil {
		return false
	}
	return true
}

// Get - get value from key
func (db *BoltDB) Get(key string) *KVResult {
	var data []byte
	if err := db.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.Bucket))
		if b == nil {
			return fmt.Errorf("could not open bucket, %s", db.Bucket)
		}
		data = b.Get([]byte(key))
		if data == nil {
			return errors.New("no suck key in DB")
		}
		return nil
	}); err != nil {
		return &KVResult{
			Result: false,
			Info:   err.Error(),
		}
	}
	return &KVResult{
		Data:   data,
		Result: true,
		Info:   "",
	}
}

// FindOne - find first matched content that hander returned
func (db *BoltDB) FindOne(handler func(k, v []byte) *KVResult) *KVResult {
	kv := &KVResult{}
	err := db.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.Bucket))
		if b == nil {
			return fmt.Errorf("could not open bucket, %s", db.Bucket)
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if i := handler(k, v); i.Result {
				kv = i
				return nil
			}
		}
		return errors.New("didn't found kvs")
	})
	if err == nil {
		return kv
	}
	return &KVResult{
		Result: false,
		Info:   err.Error(),
	}
}

// Set - set key value
func (db *BoltDB) Set(kv *KVData) *KVResult {
	err := db.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.Bucket))
		err := b.Put([]byte(kv.Key), kv.Value)
		return err
	})
	if err != nil {
		return &KVResult{
			Result: false,
			Info:   err.Error(),
		}
	}
	return &KVResult{
		Data:   kv,
		Result: true,
		Info:   "",
	}
}

// Delete - delete key
func (db *BoltDB) Delete(key string) *KVResult {
	kv := &KVData{Key: key}
	kvr := &KVResult{
		Data: kv,
	}
	err := db.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.Bucket))
		kv.Value = b.Get([]byte(key))
		err := b.Delete([]byte(key))
		return err
	})
	if err != nil {
		kvr.Info = err.Error()
		kvr.Result = false
		return kvr
	}
	kvr.Result = true
	return kvr
}

// KeyCount - Key Number
// count of keys
func (db *BoltDB) KeyCount() int {
	var number = 0
	if err := db.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.Bucket))
		stats := b.Stats()
		number = stats.KeyN
		return nil
	}); err == nil {
		return number
	}
	return 0
}

// ListKeys - list keys
// page - the number of page
// boltdb.Count define the records in one page
func (db *BoltDB) ListKeys(page uint) []string {
	var list []string
	if err := db.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.Bucket))
		if b == nil {
			return fmt.Errorf("could not open bucket, %s", db.Bucket)
		}
		c := b.Cursor()
		index := uint(0)
		for k, _ := c.First(); k != nil && index < (page+1)*db.Count; k, _ = c.Next() {
			if index >= page*db.Count {
				list = append(list, string(k))
			}
			index++
		}
		return nil
	}); err == nil {
		return list
	}
	return []string{}
}

// List - list content that hander returned
// page - page number
// boltdb.Count define the records in one page
func (db *BoltDB) List(page uint, handler func(k, v []byte) *KVResult) *KVResult {
	data := make([]interface{}, 0)
	kv := &KVResult{
		Info:   "",
		Result: true,
	}
	err := db.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.Bucket))
		if b == nil {
			return fmt.Errorf("could not open bucket, %s", db.Bucket)
		}
		c := b.Cursor()
		index := uint(0)
		for k, v := c.First(); k != nil && index < (page+1)*db.Count; k, v = c.Next() {
			if index >= page*db.Count {
				if i := handler(k, v); i.Result {
					data = append(data, i.Data)
					index++
				}
			}
		}
		return nil
	})
	if err == nil {
		kv.Data = data
		return kv
	}
	return &KVResult{
		Info:   err.Error(),
		Result: false,
	}
}

// SetData - set data in object json format
func (db *BoltDB) SetData(key string, data interface{}) *KVResult {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	value, err := json.Marshal(data)
	if err != nil {
		return &KVResult{
			Result: false,
			Info:   err.Error(),
		}
	}
	return db.Set(&KVData{key, value})
}
