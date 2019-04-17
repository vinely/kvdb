package db

import (
	"errors"
	"net/url"
	"path/filepath"
	"strconv"

	"github.com/go-redis/redis"
	jsoniter "github.com/json-iterator/go"
)

var (
	// RedisDBDefaultAddress - default local redis db
	RedisDBDefaultAddress = "localhost:6379"
	// RedisDBDefaultPassword - default no password
	RedisDBDefaultPassword = ""
	// RedisDBDefaultDB - default db is 0
	RedisDBDefaultDB = 0
)

// RedisDB - using Redis Hashkey mode as a key-value database
type RedisDB struct {
	KVMethods
	Type     *KVDBType
	Address  string
	Password string
	HashKey  string
	DB       int
	Count    uint
	Client   *redis.Client
}

func init() {
	NewKVDatabaseType("redis", NewRedisDB)
}

// NewRedisDB - new redis db using uri format description
// format : redis://<redis host address>/<hashkey>?[count=]&[password=]&[dbno=]
// example redis://localhost:6379/serv?count=20&password=123&dbno=1
func NewRedisDB(uri string) (KVMethods, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	t := GetKVDatabaseType(u.Scheme)
	if t == nil {
		return nil, errors.New("This type of database [" + t.Scheme + "] didn't exist")
	}

	redis := &RedisDB{
		Type:    t,
		Address: u.Host,
		HashKey: filepath.Base(u.Path),
	}
	para := u.Query()
	if para.Get("count") != "" {
		i, _ := strconv.Atoi(para.Get("count"))
		if i <= 0 {
			return nil, errors.New("wrong count parameter")
		}
		redis.Count = uint(i)
	}
	if para.Get("password") != "" {
		redis.Password = para.Get("password")
	}
	if para.Get("dbno") != "" {
		redis.DB, _ = strconv.Atoi(para.Get("dbno"))
	}

	err = redis.setup()
	if err != nil {
		return nil, err
	}
	return redis, nil
}

func (db *RedisDB) setup() error {
	db.Client = redis.NewClient(&redis.Options{
		Addr:     db.Address,
		Password: db.Password,
		DB:       db.DB,
		// Addr:     "localhost:6379",
		// Password: "", // no password set
		// DB:       0,  // use default DB
	})

	_, err := db.Client.Ping().Result()
	return err
}

// Name - tag  different databases
func (db *RedisDB) Name() string {
	return "Redis_" + db.HashKey
}

// DBType - DataBase Type
func (db *RedisDB) DBType() *KVDBType {
	return db.Type
}

// Exists - if key existed
func (db *RedisDB) Exists(key string) bool {
	return db.Client.HExists(db.HashKey, key).Val()
}

// Get - get value from key
func (db *RedisDB) Get(key string) *KVResult {
	data, err := db.Client.HGet(db.HashKey, key).Bytes()
	if err != nil {
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

// Set - set key value
func (db *RedisDB) Set(kv *KVData) *KVResult {
	//	return db.Client.Set(key, data, time.Duration(t)).Err()
	err := db.Client.HSet(db.HashKey, kv.Key, kv.Value).Err()
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

// Del - del a key
func (db *RedisDB) Del(key string) error {
	return db.Client.HDel(db.HashKey, key).Err()
}

// Delete - delete key
func (db *RedisDB) Delete(key string) *KVResult {
	var err error
	kvr := &KVResult{}
	kvr.Data, err = db.Client.HGet(db.HashKey, key).Bytes()
	if err != nil {
		kvr.Info = err.Error()
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
func (db *RedisDB) KeyCount() int {
	return int(db.Client.HLen(db.HashKey).Val())
}

// FindOne - find first matched content that hander returned
func (db *RedisDB) FindOne(handler func(k, v []byte) *KVResult) *KVResult {
	iter := db.Client.HScan(db.HashKey, 0, "", 10).Iterator()
	for iter.Next() {
		k := iter.Val()
		v, err := db.Client.HGet(db.HashKey, k).Bytes()
		if err != nil {
			continue
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
func (db *RedisDB) ListKeys(page uint) []string {
	var list []string
	index := uint(0)
	iter := db.Client.HScan(db.HashKey, 0, "", 10).Iterator()
	for iter.Next() {
		if index >= page*db.Count {
			list = append(list, iter.Val())
		}
		index++
		if index >= (page+1)*db.Count {
			return list
		}
	}
	return list
}

// List - list content that hander returned
// page - page number
func (db *RedisDB) List(page uint, handler func(k, v []byte) *KVResult) *KVResult {
	data := make([]interface{}, 0)
	kv := &KVResult{
		Info:   "",
		Result: true,
	}
	index := uint(0)
	iter := db.Client.HScan(db.HashKey, 0, "", 10).Iterator()
	for iter.Next() {
		if index >= page*db.Count {
			k := iter.Val()
			v, err := db.Client.HGet(db.HashKey, k).Bytes()
			if err != nil {
				continue
			}
			if i := handler([]byte(k), v); i.Result {
				data = append(data, i.Data)
				index++
			}
		}
		if index >= (page+1)*db.Count {
			kv.Data = data
			return kv
		}
	}
	kv.Data = data
	return kv
}

// SetData - set data in object json format
func (db *RedisDB) SetData(key string, data interface{}) *KVResult {
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
