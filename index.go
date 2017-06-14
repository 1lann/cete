package cete

import (
	"bytes"
	"log"
	"os"

	"github.com/dgraph-io/badger/badger"
	"gopkg.in/vmihailenco/msgpack.v2"
)

// The bounds type are for variables which represent a bound for Between.
type Bounds int

// Valid bounds.
var (
	MinBounds Bounds = 0
	MaxBounds Bounds = 1
)

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
		log.Println("cete: attempt to call new index on a non-existent table")
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

	r := t.Between(MinBounds, MaxBounds)

	idx := &Index{
		index: kv,
		table: t,
	}

	var entry bufferEntry
	var results []interface{}

	for {
		entry = <-r.buffer
		if entry.err == ErrEndOfRange {
			r.Close()
			break
		} else if entry.err != nil {
			r.Close()
			return entry.err
		}

		results, err = msgpack.NewDecoder(bytes.NewReader(entry.data)).Query(name)
		if err != nil {
			continue
		}

		for _, result := range results {
			err = idx.addToIndex(valueToBytes(result), entry.key)
			if err != nil {
				log.Println("cete: index error for index \""+name+"\":", err)
			}
		}
	}

	t.indexes[Name(name)] = idx

	return nil
}

// One puts the first matching value with the index's key into dst. dst
// must be a pointer. Note that indexes are non-unique, a single index key
// can map to multiple values. Use GetAll to get all such matching values.
func (i *Index) One(key interface{}, dst interface{}) (string, int, error) {
	r, err := i.GetAll(key)
	if err != nil {
		return "", 0, err
	}

	defer r.Close()

	tableKey, counter, err := r.Next(dst)
	if err == ErrEndOfRange {
		log.Println("cete: warning: corrupt index detected:", i.name())
		return tableKey, counter, ErrNotFound
	}

	return tableKey, counter, err
}

// GetAll returns all the matching values as a range for the provided index key.
func (i *Index) GetAll(key interface{}) (*Range, error) {
	var item badger.KVItem
	err := i.index.Get(valueToBytes(key), &item)
	if err != nil {
		return nil, err
	}

	if item.Value() == nil {
		return nil, ErrNotFound
	}

	var keys []string
	err = msgpack.Unmarshal(item.Value(), &keys)
	if err != nil {
		log.Println("cete: corrupt index \""+i.name()+"\":", err)
		return nil, ErrIndexError
	}

	if len(keys) == 0 {
		log.Println("cete: corrupt index \""+i.name()+"\":", err)
		return nil, ErrNotFound
	}

	c := 0
	var value []byte

	return newRange(func() (string, []byte, int, error) {
		for {
			if c >= len(keys) {
				return "", nil, 0, ErrEndOfRange
			}

			err = i.table.data.Get([]byte(keys[c]), &item)
			if err != nil {
				return "", nil, 0, err
			}

			if item.Value() == nil {
				c++
				continue
			}

			value = make([]byte, len(item.Value()))
			copy(value, item.Value())

			c++
			return keys[c-1], value, int(item.Counter()), nil
		}
	}, func() {}), nil
}

// Between returns all the values whose index key is within the specified
// bounds. The bounds are inclusive on both ends. It is possible to have
// duplicate documents if the same document has multiple unique index values.
func (i *Index) Between(lower interface{}, upper interface{},
	reverse ...bool) *Range {
	shouldReverse := (len(reverse) > 0) && reverse[0]

	itOpts := badger.DefaultIteratorOptions
	itOpts.PrefetchSize = 5
	itOpts.Reverse = shouldReverse
	it := i.index.NewIterator(itOpts)

	upperBytes := valueToBytes(upper)
	lowerBytes := valueToBytes(lower)

	if !shouldReverse {
		if lower == MinBounds {
			it.Rewind()
		} else {
			it.Seek(lowerBytes)
		}
	} else {
		if upper == MaxBounds {
			it.Rewind()
		} else {
			it.Seek(upperBytes)
		}
	}

	var entry bufferEntry
	var lastRange *Range

	return newRange(func() (string, []byte, int, error) {
		if lastRange != nil {
			entry = <-lastRange.buffer
			if entry.err != ErrEndOfRange {
				return entry.key, entry.data, entry.counter, entry.err
			}

			lastRange.Close()
		}

		for it.Valid() {
			if !shouldReverse && upper != MaxBounds &&
				bytes.Compare(it.Item().Key(), upperBytes) > 0 {
				return "", nil, 0, ErrEndOfRange
			} else if shouldReverse && lower != MinBounds &&
				bytes.Compare(it.Item().Key(), lowerBytes) < 0 {
				return "", nil, 0, ErrEndOfRange
			}

			r, err := i.GetAll(it.Item().Key())
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
	}, func() {
		if lastRange != nil {
			lastRange.Close()
		}
		it.Close()
	})
}

// All returns all the documents which have an index value. It is shorthand
// for Between(MinBounds, MaxBounds, reverse...)
func (i *Index) All(reverse ...bool) *Range {
	return i.Between(MinBounds, MaxBounds, reverse...)
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
