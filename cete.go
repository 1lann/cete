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
	"strings"
	"sync"
	"time"

	"github.com/1lann/badger"
	"github.com/1lann/msgpack"
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

// Name represents a table or index identifier.
type Name string

// Hex returns the hexadecimal representation of the name.
func (n Name) Hex() string {
	return hex.EncodeToString([]byte(n))
}

// Index represents an index of a table.
type Index struct {
	index *badger.KV
	table *Table
}

// Table represents a table in the database.
type Table struct {
	indexes map[Name]*Index
	data    *badger.KV
	db      *DB

	compressionLock *sync.RWMutex
	keyToCompressed map[string]string
	compressedToKey map[string]string
	nextKey         string
}

// DB represents the database.
type DB struct {
	path        string
	tables      map[Name]*Table
	config      dbConfig
	configMutex *sync.Mutex
	openOptions badger.Options
	closed      int32
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
		num = uint64(i) + (1 << 63)
	case int16:
		num = uint64(i) + (1 << 63)
	case int32:
		num = uint64(i) + (1 << 63)
	case int64:
		num = uint64(i) + (1 << 63)
	case uint16:
		num = uint64(i) + (1 << 63)
	case uint32:
		num = uint64(i) + (1 << 63)
	case uint64:
		num = i + (1 << 63)
	default:
		log.Fatal("integerToBytes called on a non integer: ", i)
	}

	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, num)
	return result
}

func valueToBytes(value interface{}) (b []byte) {
	switch v := value.(type) {
	case int, int16, int32, int64, uint16, uint32, uint64:
		return integerToBytes(v)
	case float32:
		return integerToBytes(math.Float32bits(v))
	case float64:
		return integerToBytes(math.Float64bits(v))
	case string:
		return append([]byte(strings.ToLower(v)), 0)
	case []byte:
		return append(v, 0)
	case []interface{}:
		var result []byte
		for _, vv := range v {
			result = append(result, valueToBytes(vv)...)
		}
		return result
	case time.Time:
		return append(valueToBytes(v.Unix()), valueToBytes(v.Nanosecond())...)
	case Bounds:
		return integerToBytes(int64(v))
	}

	panic(fmt.Sprintf("cete: unsupported value: %v", value))
}

func getItemValue(item *badger.KVItem) []byte {
	result := make(chan []byte, 1)
	err := item.Value(func(value []byte) error {
		result <- value
		return nil
	})
	if err != nil {
		return nil
	}
	return <-result
}

// Document represents the value of a document.
type Document struct {
	data  []byte
	table *Table
}

// QueryInt returns the int value of a QueryOne assumed to contain an int.
func (v Document) QueryInt(query string) int {
	r, ok := v.QueryOne(query).(uint64)
	if !ok {
		return 0
	}

	return int(r)
}

// QueryInt64 returns the int64 value of a QueryOne assumed to contain an int64.
func (v Document) QueryInt64(query string) int64 {
	r, ok := v.QueryOne(query).(uint64)
	if !ok {
		return 0
	}

	return int64(r)
}

// QueryFloat64 returns the float64 value of a QueryOne assumed to contain a float64.
func (v Document) QueryFloat64(query string) float64 {
	r, ok := v.QueryOne(query).(float64)
	if !ok {
		return 0
	}

	return r
}

// QueryString returns the string value of a QueryOne assumed to contain a string.
func (v Document) QueryString(query string) string {
	r, ok := v.QueryOne(query).(string)
	if !ok {
		return ""
	}

	return r
}

// QueryBytes returns the []byte value of a QueryOne assumed to contain a []byte.
func (v Document) QueryBytes(query string) []byte {
	r, ok := v.QueryOne(query).([]byte)
	if !ok {
		return nil
	}

	return r
}

// QueryTime returns the time.Time value of a QueryOne assumed to contain a time.Time.
func (v Document) QueryTime(query string) time.Time {
	t, ok := v.QueryOne(query).([]interface{})
	if !ok {
		return time.Time{}
	}
	return time.Unix(int64(t[0].(uint64)), int64(t[1].(uint64)))
}

// QueryOne returns the first matching value of a msgpack query.
func (v Document) QueryOne(query string) interface{} {
	results := v.QueryAll(query)
	if len(results) == 0 {
		return nil
	}

	return results[0]
}

// QueryAll returns the first matching value of a msgpack query.
func (v Document) QueryAll(query string) []interface{} {
	var results []interface{}
	var err error
	if v.table != nil && v.table.keyToCompressed != nil {
		results, err = msgpack.NewDecoder(bytes.NewReader(v.data)).
			QueryCompressed(v.table.keyToC, query)
	} else {
		results, err = msgpack.NewDecoder(bytes.NewReader(v.data)).Query(query)
	}

	if err != nil || len(results) == 0 {
		return nil
	}
	return results
}

// Decode attempts to decodes the document to an interface using reflection.
func (v Document) Decode(dst interface{}) error {
	if v.table != nil && v.table.keyToCompressed != nil {
		return msgpack.UnmarshalCompressed(v.table.cToKey, v.data, dst)
	}

	return msgpack.Unmarshal(v.data, dst)
}
