package cete

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/badger/badger"
	"gopkg.in/vmihailenco/msgpack.v2"
)

// Common errors that can be returned
var (
	ErrAlreadyExists  = errors.New("cete: already exists")
	ErrNotFound       = errors.New("cete: not found")
	ErrBadIdentifier  = errors.New("cete: bad identifier")
	ErrEndOfRange     = errors.New("cete: end of range")
	ErrCounterChanged = errors.New("cete: counter changed")
	ErrIndexError     = errors.New("cete: index error")
)

type Name string

func (n Name) Hex() string {
	return hex.EncodeToString([]byte(n))
}

type Index struct {
	index *badger.KV
	table *Table
}

type Table struct {
	indexes map[Name]*Index
	data    *badger.KV
	db      *DB
}

type DB struct {
	path        string
	tables      map[Name]*Table
	config      dbConfig
	configMutex *sync.Mutex
}

func (d *DB) Table(tableName string) *Table {
	return d.tables[Name(tableName)]
}

func exists(path string) (bool, error) {
	stat, err := os.Stat(path)
	if err == nil {
		if !stat.IsDir() {
			return false, nil
		}
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func integerToBytes(integer interface{}) []byte {
	var num uint64
	switch i := integer.(type) {
	case int:
		num = uint64(i)
	case int16:
		num = uint64(i)
	case int32:
		num = uint64(i)
	case int64:
		num = uint64(i)
	case uint16:
		num = uint64(i)
	case uint32:
		num = uint64(i)
	case uint64:
		num = i
	default:
		log.Fatal("integerToBytes called on a non integer: ", i)
	}

	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, num)
	return result
}

func valueToBytes(value interface{}) []byte {
	switch v := value.(type) {
	case int, int16, int32, int64, uint16, uint32, uint64:
		return integerToBytes(v)
	case float32:
		return integerToBytes(math.Float32bits(v))
	case float64:
		return integerToBytes(math.Float64bits(v))
	case string:
		return []byte(v)
	case []byte:
		return v
	case []interface{}:
		var result []byte
		for _, vv := range v {
			result = append(result, valueToBytes(vv)...)
		}
		return result
	case time.Time:
		return append(valueToBytes(v.Unix()), valueToBytes(v.Nanosecond())...)
	case Bounds:
		return nil
	}

	panic(fmt.Sprintf("cete: unsupported value: %v", value))
}

type Document []byte

func (v Document) QueryInt(query string) int {
	return int(v.QueryOne(query).(uint64))
}

func (v Document) QueryInt64(query string) int64 {
	return int64(v.QueryOne(query).(uint64))
}

func (v Document) QueryFloat64(query string) float64 {
	return v.QueryOne(query).(float64)
}

func (v Document) QueryString(query string) string {
	return v.QueryOne(query).(string)
}

func (v Document) QueryBytes(query string) []byte {
	return v.QueryOne(query).([]byte)
}

func (v Document) QueryTime(query string) time.Time {
	t := v.QueryOne(query).([]interface{})
	return time.Unix(int64(t[0].(uint64)), int64(t[1].(uint64)))
}

func (v Document) QueryOne(query string) interface{} {
	results, err := msgpack.NewDecoder(bytes.NewReader([]byte(v))).Query(query)
	if err != nil || len(results) == 0 {
		return nil
	}

	return results[0]
}

func (v Document) Decode(dst interface{}) error {
	return msgpack.Unmarshal([]byte(v), dst)
}
