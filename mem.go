package db

import (
	"errors"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"
)

var (
	// MemDBDefaultPassword - default no password
	MemDBDefaultPassword = ""
	// MemDBList - map of memdbs
	MemDBList = make(map[string]*MemDB)
)

// MemBucket - mem bucket
type MemBucket struct {
	Label   string
	Buckets map[string]*MemBucket
	Data    map[string]interface{}
	DB      *MemDB
}

// MemDB - using Memory as a key-value database
type MemDB struct {
	KVMethods
	Type     *KVDBType
	Label    string
	Password string
	Buckets  map[string]*MemBucket
	Count    uint
}

func init() {
	NewKVDatabaseType("mem", NewMemDB)
}

// NewMemDB - new redis db using uri format description
// format : mem://<name>/<hashkey>?[count=]&[password=]
// example mem://temp/serv?count=20&password=123
func NewMemDB(uri string) (KVMethods, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	t := GetKVDatabaseType(u.Scheme)
	if t == nil {
		return nil, errors.New("This type of database [" + t.Scheme + "] didn't exist")
	}
	para := u.Query()
	password := para.Get("password")

	db, ok := MemDBList[u.Host]
	var bucket *MemBucket
	if ok {
		if db.Password != "" && strings.Compare(db.Password, password) != 0 {
			return nil, errors.New("Password not match")
		}
		label := filepath.Base(u.Path)
		bucket, ok = db.Buckets[label]
		if !ok {
			bucket = &MemBucket{
				DB:    db,
				Label: filepath.Base(u.Path),
				Data:  make(map[string]interface{}),
			}
		}
		db.Buckets[bucket.Label] = bucket
	} else {
		db := &MemDB{
			Type:    t,
			Label:   u.Host,
			Buckets: make(map[string]*MemBucket),
		}
		bucket = &MemBucket{
			DB:    db,
			Label: filepath.Base(u.Path),
			Data:  make(map[string]interface{}),
		}
		db.Buckets[bucket.Label] = bucket
		if para.Get("count") != "" {
			i, _ := strconv.Atoi(para.Get("count"))
			if i <= 0 {
				return nil, errors.New("wrong count parameter")
			}
			db.Count = uint(i)
		}
		if para.Get("password") != "" {
			db.Password = para.Get("password")
		}
	}
	return bucket, nil
}

// Name - tag  different databases
func (db *MemBucket) Name() string {
	return "Memdb_" + db.Label
}

// DBType - DataBase Type
func (db *MemBucket) DBType() *KVDBType {
	return db.DB.Type
}

// Exists - if key existed
func (db *MemBucket) Exists(key string) bool {
	_, ok := db.Data[key]
	return ok
}

// Get - get value from key
func (db *MemBucket) Get(key string) *KVResult {
	data, ok := db.Data[key]
	if !ok {
		return &KVResult{
			Result: false,
			Info:   "data didn't existed",
		}
	}
	return &KVResult{
		Data:   data,
		Result: true,
		Info:   "",
	}
}

// Set - set key value
func (db *MemBucket) Set(kv *KVData) *KVResult {
	db.Data[kv.Key] = kv.Value
	return &KVResult{
		Data:   kv,
		Result: true,
		Info:   "",
	}
}

// Del - del a key
func (db *MemBucket) Del(key string) error {
	delete(db.Data, key)
	return nil
}

// Delete - delete key
func (db *MemBucket) Delete(key string) *KVResult {
	var (
		err error
		ok  bool
	)
	kvr := &KVResult{}
	kvr.Data, ok = db.Data[key]
	if !ok {
		kvr.Info = "data didn't existed"
		kvr.Result = false
		return kvr
	}
	err = db.Del(key)
	if err != nil {
		kvr.Info = err.Error()
		kvr.Result = false
	}
	kvr.Result = true
	return kvr
}

// KeyCount - Key Number
// count of keys
func (db *MemBucket) KeyCount() int {
	return len(db.Data)
}

// FindOne - find first matched content that hander returned
func (db *MemBucket) FindOne(handler func(k, v []byte) *KVResult) *KVResult {
	for k, data := range db.Data {
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		v, err := json.Marshal(data)
		if err != nil {
			return &KVResult{
				Result: false,
				Info:   err.Error(),
			}
		}
		if i := handler([]byte(k), v); i.Result {
			return i
		}
	}
	return &KVResult{
		Result: false,
		Info:   "didn't found kvs",
	}
}

// ListKeys - list keys
// page - the number of page
func (db *MemBucket) ListKeys(page uint) []string {
	var list []string
	index := uint(0)
	for k := range db.Data {
		if index >= page*db.DB.Count {
			list = append(list, k)
		}
		index++
		if index >= (page+1)*db.DB.Count {
			return list
		}
	}
	return list
}

// List - list content that hander returned
// page - page number
func (db *MemBucket) List(page uint, handler func(k, v []byte) *KVResult) *KVResult {
	data := make([]interface{}, 0)
	kv := &KVResult{
		Info:   "",
		Result: true,
	}
	index := uint(0)

	for k, vdata := range db.Data {
		if index >= page*db.DB.Count {
			var json = jsoniter.ConfigCompatibleWithStandardLibrary
			v, err := json.Marshal(vdata)
			if err != nil {
				return &KVResult{
					Result: false,
					Info:   err.Error(),
				}
			}
			if i := handler([]byte(k), v); i.Result {
				data = append(data, i.Data)
				index++
			}
		}
		if index >= (page+1)*db.DB.Count {
			kv.Data = data
			return kv
		}
	}
	kv.Data = data
	return kv
}

// SetData - set data in object json format
func (db *MemBucket) SetData(key string, data interface{}) *KVResult {
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
