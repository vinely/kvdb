package db

import (
	"errors"
	"net/url"
)

// KVData - KV data for record
type KVData struct {
	Key   string
	Value []byte
}

// KVResult - KVData Result
type KVResult struct {
	Data   interface{} `json:",omitempty"`
	Result bool
	Info   string
}

func (kv *KVResult) Error() string {
	if kv.Result {
		return ""
	}
	return kv.Info
}

// KVInfo - KVData Result
type KVInfo struct {
	KVResult
	KVData []KVData `json:",omitempty"`
}

// Len - get length of kv array
func (kv *KVInfo) Len() int {
	return len(kv.KVData)
}

// KVMethods - interface for KV DB
type KVMethods interface {
	KVBase
	KVList
	KVUtil
}

// KVBase - base interface for KV DB
type KVBase interface {
	Name() string
	DBType() *KVDBType
	Exists(key string) bool
	Get(key string) *KVResult
	FindOne(handler func(k, v []byte) *KVResult) *KVResult
	Set(kv *KVData) *KVResult
	Delete(key string) *KVResult
	KeyCount() int
}

// KVUtil - extended interface in use
type KVUtil interface {
	SetData(key string, data interface{}) *KVResult
}

// KVList - interface of list operations
// interface for list method
type KVList interface {
	// List keys on page no
	ListKeys(page uint) []string
	// List Values that match hander selection on page no
	List(page uint, handler func(k, v []byte) *KVResult) *KVResult
}

// KVDBType -kv database types
type KVDBType struct {
	Scheme      string
	DataBases   map[string]KVMethods
	Constructor KVDBConstructor
}

// KVDBConstructor - kv database constructor delegate function
type KVDBConstructor func(uri string) (KVMethods, error)

var (
	// KVDBs - constructor functions for databases
	KVDBs = make(map[string]KVDBType)
)

// NewKVDataBase - construct a new database
// uri describe the database location
//    "redis://localhost:6379/service?count=50&dbno=1"
// 	  "bolt://service.db/service?count=50"
func NewKVDataBase(uri string) (KVMethods, error) {
	res, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	db := KVDBs[res.Scheme]
	kvdb, err := db.Constructor(uri)
	if err != nil {
		return nil, err
	}
	_, ok := db.DataBases[kvdb.Name()]
	if ok {
		return nil, errors.New(kvdb.Name() + " already existed")
	}
	db.DataBases[kvdb.Name()] = kvdb
	return kvdb, nil
}

// NewKVDatabaseType - database register to maps
func NewKVDatabaseType(scheme string, con KVDBConstructor) error {
	_, ok := KVDBs[scheme]
	if ok {
		return errors.New("already existed")
	}
	k := &KVDBType{
		Scheme:      scheme,
		DataBases:   make(map[string]KVMethods),
		Constructor: con,
	}
	KVDBs[k.Scheme] = *k
	return nil
}

// GetKVDatabaseType  - get database type from scheme
func GetKVDatabaseType(scheme string) *KVDBType {
	t, ok := KVDBs[scheme]
	if ok {
		return &t
	}
	return nil
}

// Count - return count of databases in this type
func (kvdt *KVDBType) Count() int {
	return len(kvdt.DataBases)
}
