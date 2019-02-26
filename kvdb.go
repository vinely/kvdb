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

// KVInfo - KVData Result
type KVInfo struct {
	Data   []KVData
	Result bool
	Info   string
}

// Len - get length of kv array
func (kv *KVInfo) Len() int {
	return len(kv.Data)
}

func (kv *KVInfo) Error() string {
	if kv.Result {
		return ""
	}
	return kv.Info
}

// KVMethods - interface for KV DB
type KVMethods interface {
	KVBase
	KVList
}

// KVBase - base interface for KV DB
type KVBase interface {
	Name() string
	DBType() *KVDBType
	Exists(key string) bool
	Get(key string) *KVInfo
	FindOne(handler func(k, v []byte) *KVInfo) *KVInfo
	Set(kv *KVData) *KVInfo
	Delete(key string) *KVInfo
	KeyCount() int
}

// KVList - interface of list operations
// interface for list method
type KVList interface {
	// List keys on page no
	ListKeys(page uint) []string
	// List Values that match hander selection on page no
	List(page uint, handler func(k, v []byte) *KVInfo) *KVInfo
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
