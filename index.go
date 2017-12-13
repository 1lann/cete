package cete

import (
	"bytes"
	"log"
	"os"
	"strings"
	"sync/atomic"

	"github.com/1lann/badger"
	"github.com/1lann/msgpack"
)

// Bounds is the type for variables which represent a bound for Between.
type Bounds int

// Minimum and maximum values.
var (
	MinValue Bounds = (-1 << 63)
	MaxValue Bounds = (1 << 63) - 1
)

const prefetchSize = 2

// NewIndex creates a new index on the table, using the name as the Query.
// The index name must not be empty, and must be no more than 125 bytes
// long. ErrAlreadyExists will be returned if the index already exists.
//
// NewIndex may take a while if there are already values in the
// table, as it needs to index all the existing values in the table.
func (t *Table) NewIndex(name string) error {
	if name == "" || len(name) > 125 {
		return ErrBadIdentifier
	}

	t.db.configMutex.Lock()

	tableName := t.name()
	tableConfigKey := -1

	for key, table := range t.db.config.Tables {
		if table.TableName == tableName {
			tableConfigKey = key
			for _, index := range table.Indexes {
				if index.IndexName == name {
					t.db.configMutex.Unlock()
					return ErrAlreadyExists
				}
			}
		}
	}

	if tableConfigKey < 0 {
		t.db.configMutex.Unlock()
		return ErrNotFound
	}

	kv, err := t.db.newKV(Name(tableName), Name(name))
	if err != nil {
		t.db.configMutex.Unlock()
		return err
	}

	indexes := t.db.config.Tables[tableConfigKey].Indexes
	indexes = append(indexes, indexConfig{IndexName: name})
	t.db.config.Tables[tableConfigKey].Indexes = indexes
	if err = t.db.writeConfig(); err != nil {
		t.db.configMutex.Unlock()
		return err
	}

	t.db.configMutex.Unlock()

	idx := &Index{
		index: kv,
		table: t,
	}

	t.indexes[Name(name)] = idx

	if err = idx.indexValues(name); err != nil {
		log.Println("cete: error while indexing \""+
			idx.name()+"\", index likely corrupt:", err)
		return nil
	}

	return nil
}

func (i *Index) indexValues(name string) error {
	var total int64

	i.table.Between(MinValue, MaxValue).Do(func(key string, counter uint64, doc Document) error {
		last := atomic.AddInt64(&total, 1)
		if last%100000 == 0 {
			log.Println(last)
		}

		results, err := i.indexQuery(doc.data, name)
		if err != nil {
			return nil
		}

		for _, result := range results {
			err = i.addToIndex(valueToBytes(result), key)
			if err != nil {
				log.Println("cete: index error for index \""+name+"\":", err)
			}
		}

		return nil
	}, 20)

	return nil
}

func (i *Index) indexQuery(data []byte, query string) ([]interface{}, error) {
	rd := bytes.NewReader(data)
	dec := msgpack.NewDecoder(rd)

	compressed := i.table.keyToCompressed != nil

	queries := strings.Split(query, ",")
	if len(queries) > 1 {
		results := make([]interface{}, len(queries))

		var res []interface{}
		var err error
		for it, q := range queries {
			if compressed {
				res, err = dec.QueryCompressed(i.table.keyToC, q)
			} else {
				res, err = dec.Query(q)
			}
			if err != nil {
				return nil, err
			}

			rd.Reset(data)
			dec.Reset(rd)

			results[it] = res[0]
		}

		return []interface{}{results}, nil
	}

	if compressed {
		return dec.QueryCompressed(i.table.keyToC, query)
	}

	return dec.Query(query)
}

// One puts the first matching value with the index's key into dst. dst
// must either be a pointer or nil if you would like to only get the key/counter
// and check for existence. Note that indexes are non-unique, a single index key
// can map to multiple values. Use GetAll to get all such matching values.
func (i *Index) One(key interface{}, dst interface{}) (string, uint64, error) {
	r := i.GetAll(key)
	defer r.Close()

	if !r.Next() {
		if r.Error() == ErrEndOfRange {
			return "", 0, ErrNotFound
		}

		return "", 0, r.Error()
	}

	if dst == nil {
		return r.Key(), r.Counter(), nil
	}

	return r.Key(), r.Counter(), r.Decode(dst)
}

// GetAll returns all the matching values as a range for the provided index key.
func (i *Index) GetAll(key interface{}) *Range {
	var item badger.KVItem
	err := i.index.Get(valueToBytes(key), &item)
	if err != nil {
		return newRange(func() (string, []byte, uint64, error) {
			return "", nil, 0, err
		}, func() {}, nil)
	}

	itemValue := getItemValue(&item)
	if itemValue == nil {
		return newRange(func() (string, []byte, uint64, error) {
			return "", nil, 0, ErrEndOfRange
		}, func() {}, nil)
	}

	r, err := i.getAllValues(itemValue)
	if err != nil {
		return newRange(func() (string, []byte, uint64, error) {
			return "", nil, 0, err
		}, func() {}, nil)
	}
	return r
}

func (i *Index) getAllValues(indexValue []byte) (*Range, error) {
	var keys []string
	err := msgpack.Unmarshal(indexValue, &keys)
	if err != nil {
		log.Println("cete: corrupt index \""+i.name()+"\":", err)
		return nil, ErrIndexError
	}

	if len(keys) == 0 {
		log.Println("cete: corrupt index \""+i.name()+"\":", err)
		return nil, ErrIndexError
	}

	c := 0
	var value []byte
	var item badger.KVItem

	return newRange(func() (string, []byte, uint64, error) {
		for {
			if c >= len(keys) {
				return "", nil, 0, ErrEndOfRange
			}

			err = i.table.data.Get([]byte(keys[c]), &item)
			if err != nil {
				return "", nil, 0, err
			}

			itemValue := getItemValue(&item)
			if itemValue == nil {
				c++
				continue
			}

			value = make([]byte, len(itemValue))
			copy(value, itemValue)

			c++
			return keys[c-1], value, item.Counter(), nil
		}
	}, func() {}, i.table), nil
}

// Between returns a Range of documents between the lower and upper index values
// provided. The range will be sorted in ascending order by index value. You can
// reverse the sorting by specifying true to the optional reverse parameter.
// The bounds are inclusive on both ends. It is possible to have
// duplicate documents if the same document has multiple unique index values.
// To remove filter duplicate documents, use `Unique()` on the Range.
//
// You can use cete.MinValue and cete.MaxValue to specify minimum and maximum
// bound values.
func (i *Index) Between(lower, upper interface{}, reverse ...bool) *Range {
	if lower == MaxValue || upper == MinValue {
		return newRange(func() (string, []byte, uint64, error) {
			return "", nil, 0, ErrEndOfRange
		}, func() {}, nil)
	}

	shouldReverse := (len(reverse) > 0) && reverse[0]

	itOpts := badger.DefaultIteratorOptions
	itOpts.PrefetchSize = prefetchSize
	itOpts.Reverse = shouldReverse
	it := i.index.NewIterator(itOpts)

	upperBytes := valueToBytes(upper)
	lowerBytes := valueToBytes(lower)

	if !shouldReverse {
		if lower == MinValue {
			it.Rewind()
		} else {
			it.Seek(lowerBytes)
		}
	} else {
		if upper == MaxValue {
			it.Rewind()
		} else {
			it.Seek(upperBytes)
		}
	}

	var lastRange *Range

	return newRange(i.betweenNext(it, lastRange, shouldReverse, lower, upper),
		func() {
			if lastRange != nil {
				lastRange.Close()
			}
			it.Close()
		}, i.table)
}

// CountBetween returns the number of documents whose index values are
// within the given bounds. It is an optimized version of
// Between(lower, upper).Count(). Note that like with Between, double counting
// for documents is possible if the document has multiple unique index values.
func (i *Index) CountBetween(lower, upper interface{}) int64 {
	if lower == MaxValue || upper == MinValue {
		return 0
	}

	itOpts := badger.DefaultIteratorOptions
	itOpts.PrefetchSize = prefetchSize
	it := i.index.NewIterator(itOpts)

	upperBytes := valueToBytes(upper)
	lowerBytes := valueToBytes(lower)

	if lower == MinValue {
		it.Rewind()
	} else {
		it.Seek(lowerBytes)
	}

	var count int64

	for it.Valid() {
		if upper != MaxValue &&
			bytes.Compare(it.Item().Key(), upperBytes) > 0 {
			return count
		}

		itemValue := getItemValue(it.Item())
		if len(itemValue) < 5 {
			// Malformed index value my cause a panic here
			count += decodeArrayCount(itemValue)
		} else {
			count += decodeArrayCount(itemValue[:5])
		}

		it.Next()
	}

	return count
}

func decodeArrayCount(header []byte) int64 {
	if (header[0] >> 4) == 9 {
		return int64(header[0] & 0xf)
	} else if header[0] == 0xdc {
		return int64(header[1])<<8 + int64(header[2])
	} else if header[0] == 0xdd {
		return int64(header[1])<<24 + int64(header[2])<<16 +
			int64(header[3])<<8 + int64(header[4])
	}

	// not a valid array
	return 0
}

func (i *Index) betweenNext(it *badger.Iterator, lastRange *Range,
	shouldReverse bool, lower,
	upper interface{}) func() (string, []byte, uint64, error) {
	upperBytes := valueToBytes(upper)
	lowerBytes := valueToBytes(lower)

	var entry bufferEntry

	return func() (string, []byte, uint64, error) {
		if lastRange != nil {
			entry = <-lastRange.buffer
			if entry.err != ErrEndOfRange {
				return entry.key, entry.data, entry.counter, entry.err
			}

			lastRange.Close()
		}

		for it.Valid() {
			if !shouldReverse && upper != MaxValue &&
				bytes.Compare(it.Item().Key(), upperBytes) > 0 {
				return "", nil, 0, ErrEndOfRange
			} else if shouldReverse && lower != MinValue &&
				bytes.Compare(it.Item().Key(), lowerBytes) < 0 {
				return "", nil, 0, ErrEndOfRange
			}

			r, err := i.getAllValues(getItemValue(it.Item()))
			it.Next()
			if err != nil {
				continue
			}

			lastRange = r

			entry = <-lastRange.buffer
			if entry.err != ErrEndOfRange {
				return entry.key, entry.data, entry.counter, entry.err
			}

			lastRange.Close()
		}

		return "", nil, 0, ErrEndOfRange
	}
}

// All returns all the documents which have an index value. It is shorthand
// for Between(MinValue, MaxValue, reverse...)
func (i *Index) All(reverse ...bool) *Range {
	return i.Between(MinValue, MaxValue, reverse...)
}

// Drop drops the index from the table, deleting its folder from the disk.
// All further calls to the index will result in undefined behaviour.
// Note that table.Index("deleted index") will be nil.
func (i *Index) Drop() error {
	i.table.db.configMutex.Lock()
	defer i.table.db.configMutex.Unlock()

	tableName := i.table.name()

	var indexName string

	for idxName, index := range i.table.indexes {
		if index == i {
			indexName = string(idxName)
		}
	}

	if indexName == "" {
		return ErrNotFound
	}

tableLoop:
	for key, table := range i.table.db.config.Tables {
		if table.TableName == tableName {
			for indexKey, index := range table.Indexes {
				if index.IndexName == indexName {
					indexes := i.table.db.config.Tables[key].Indexes
					indexes = append(indexes[:indexKey], indexes[indexKey+1:]...)
					i.table.db.config.Tables[key].Indexes = indexes
					break tableLoop
				}
			}
		}
	}

	if err := i.table.db.writeConfig(); err != nil {
		return err
	}

	i.index.Close()

	delete(i.table.indexes, Name(indexName))

	return os.RemoveAll(i.table.db.path + "/" + Name(tableName).Hex() + "/" +
		Name(indexName).Hex())
}
