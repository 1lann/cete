package cete

import (
	"bytes"
	"errors"
	"log"
	"os"
	"reflect"
	"runtime/debug"
	"sync"

	"github.com/1lann/msgpack"
	"github.com/dgraph-io/badger"
)

// NewTable creates a new table in the database. You can optionally specify
// to disable transparent key compression by setting the second parameter to
// false. Transparent key compression is enabled by default. Disable it if your
// the keys in your document are very dynamic, as the key compression map
// is stored in memory.
func (d *DB) NewTable(name string, keyCompression ...bool) error {
	if name == "" || len(name) > 125 {
		return ErrBadIdentifier
	}

	useKeyCompression := true
	if len(keyCompression) > 0 {
		useKeyCompression = keyCompression[0]
	}

	d.configMutex.Lock()
	defer d.configMutex.Unlock()

	for _, table := range d.config.Tables {
		if table.TableName == name {
			return ErrAlreadyExists
		}
	}

	kv, err := d.newKV(Name(name))
	if err != nil {
		return err
	}

	d.config.Tables = append(d.config.Tables, tableConfig{
		TableName:         name,
		UseKeyCompression: useKeyCompression,
	})
	if err := d.writeConfig(); err != nil {
		return err
	}

	tb := &Table{
		indexes: make(map[Name]*Index),
		data:    kv,
		db:      d,
	}

	if useKeyCompression {
		tb.compressionLock = new(sync.RWMutex)
		tb.keyToCompressed = make(map[string]string)
		tb.compressedToKey = make(map[string]string)
		tb.nextKey = "0"
	}

	d.tables[Name(name)] = tb

	return nil
}

// Drop drops the table from the database.
func (t *Table) Drop() error {
	t.db.configMutex.Lock()
	defer t.db.configMutex.Unlock()

	var tableName Name
	for name, table := range t.db.tables {
		if t == table {
			tableName = name
		}
	}

	if string(tableName) == "" {
		return ErrNotFound
	}

	// Remove table from configuration
	for i, table := range t.db.config.Tables {
		if table.TableName == string(tableName) {
			t.db.config.Tables = append(t.db.config.Tables[:i],
				t.db.config.Tables[i+1:]...)
			break
		}
	}

	if err := t.db.writeConfig(); err != nil {
		return err
	}

	// Close the index and table stores
	for _, index := range t.indexes {
		index.index.Close()
	}
	t.data.Close()

	delete(t.db.tables, tableName)

	return os.RemoveAll(t.db.path + "/" + tableName.Hex())
}

// Get retrieves a value from a table with its primary key. dst must either be
// a pointer or nil if you only want to get the counter or check for existence.
func (t *Table) Get(key string, dst interface{}) (uint64, error) {
	var item badger.KVItem
	err := t.data.Get([]byte(key), &item)
	if err != nil {
		return 0, err
	}

	itemValue := getItemValue(&item)
	if itemValue == nil {
		return 0, ErrNotFound
	}

	if dst == nil {
		return item.Counter(), nil
	}

	if t.keyToCompressed != nil {
		return item.Counter(), msgpack.UnmarshalCompressed(t.cToKey,
			itemValue, dst)
	}

	return item.Counter(), msgpack.Unmarshal(itemValue, dst)
}

// Set sets a value in the table. An optional counter value can be provided
// to only set the value if the counter value is the same. A counter value
// of 0 is valid and represents a key that doesn't exist.
func (t *Table) Set(key string, value interface{}, counter ...uint64) error {
	var item badger.KVItem
	err := t.data.Get([]byte(key), &item)
	if err != nil {
		return err
	}

	if len(counter) > 0 {
		if item.Counter() != counter[0] {
			return ErrCounterChanged
		}
	}

	var data []byte
	if t.keyToCompressed != nil {
		data, err = msgpack.MarshalCompressed(t.keyToC, value)
	} else {
		data, err = msgpack.Marshal(value)
	}
	if err != nil {
		return err
	}

	if len(counter) > 0 {
		if counter[0] == 0 {
			err = t.data.SetIfAbsent([]byte(key), data, 0)
		} else {
			err = t.data.CompareAndSet([]byte(key), data, counter[0])
		}
	} else {
		err = t.data.Set([]byte(key), data, 0)
	}

	if err == badger.ErrCasMismatch || err == badger.ErrKeyExists {
		return ErrCounterChanged
	}

	if err != nil {
		return err
	}

	t.updateIndex(key, getItemValue(&item), data)

	return nil
}

type diffEntry struct {
	indexName string
	indexKey  []byte
}

func (t *Table) diffIndexes(old, new []byte) ([]diffEntry, []diffEntry) {
	var removals []diffEntry
	var additions []diffEntry

	for indexName, index := range t.indexes {
		oldRawValues, _ := index.indexQuery(old, string(indexName))
		newRawValues, _ := index.indexQuery(new, string(indexName))

		if oldRawValues == nil || len(old) == 0 {
			oldRawValues = []interface{}{}
		}

		if newRawValues == nil || len(new) == 0 {
			newRawValues = []interface{}{}
		}

		oldValues := make([][]byte, len(oldRawValues))
		newValues := make([][]byte, len(newRawValues))

		for i, oldRawValue := range oldRawValues {
			oldValues[i] = valueToBytes(oldRawValue)
		}

		for i, newRawValue := range newRawValues {
			newValues[i] = valueToBytes(newRawValue)
		}

		additions = append(additions, getOneWayDiffs(string(indexName),
			newValues, oldValues)...)

		removals = append(removals, getOneWayDiffs(string(indexName),
			oldValues, newValues)...)
	}

	return additions, removals
}

func getOneWayDiffs(indexName string, a, b [][]byte) []diffEntry {
	var results []diffEntry

	for _, aa := range a {
		found := false
		for _, bb := range b {
			if bytes.Equal(bb, aa) {
				found = true
				break
			}
		}

		if !found {
			results = append(results, diffEntry{indexName, aa})
		}
	}

	return results
}

func (t *Table) updateIndex(key string, old, new []byte) error {
	additions, removals := t.diffIndexes(old, new)

	var lastError error

	for _, removal := range removals {
		err := t.Index(removal.indexName).deleteFromIndex(removal.indexKey, key)
		if err != nil {
			log.Println("cete: error while updating index \""+
				removal.indexName+"\", index likely corrupt:", err)
			lastError = err
		}
	}

	for _, addition := range additions {
		err := t.Index(addition.indexName).addToIndex(addition.indexKey, key)
		if err != nil {
			log.Println("cete: error while updating index \""+
				addition.indexName+"\", index likely corrupt:", err)
			lastError = err
		}
	}

	return lastError
}

func (i *Index) deleteFromIndex(indexKey []byte, key string) error {
	var item badger.KVItem

	for {
		err := i.index.Get(indexKey, &item)
		if err != nil {
			return err
		}

		itemValue := getItemValue(&item)
		if itemValue == nil {
			log.Println("cete: warning: corrupt index detected:", i.name())
			return nil
		}

		var list []string
		err = msgpack.Unmarshal(itemValue, &list)
		if err != nil {
			log.Println("cete: warning: corrupt index detected:", i.name())
			return err
		}

		found := false

		for k, v := range list {
			if v == key {
				found = true
				list = append(list[:k], list[k+1:]...)
				break
			}
		}

		if !found {
			log.Println("cete: warning: corrupt index detected:", i.name())
			return nil
		}

		if len(list) == 0 {
			err = i.index.CompareAndDelete(indexKey, item.Counter())
			if err == badger.ErrCasMismatch {
				continue
			}

			return err
		}

		data, err := msgpack.Marshal(list)
		if err != nil {
			log.Fatal("cete: marshal should never fail: ", err)
		}

		err = i.index.CompareAndSet(indexKey, data, item.Counter())
		if err == badger.ErrCasMismatch {
			continue
		}

		return err
	}
}

func (i *Index) addToIndex(indexKey []byte, key string) error {
	var item badger.KVItem

	for {
		err := i.index.Get(indexKey, &item)
		if err != nil {
			return err
		}

		var list []string

		itemValue := getItemValue(&item)
		if itemValue != nil {
			err = msgpack.Unmarshal(itemValue, &list)
			if err != nil {
				log.Println("cete: warning: corrupt index detected:", i.name())
				return err
			}
		}

		for _, item := range list {
			// Already exists, no need to add.
			if item == key {
				return nil
			}
		}

		list = append(list, key)

		data, err := msgpack.Marshal(list)
		if err != nil {
			log.Fatal("cete: marshal should never fail: ", err)
		}

		if itemValue == nil {
			err = i.index.SetIfAbsent(indexKey, data, 0)
			if err == badger.ErrKeyExists {
				continue
			}
		} else {
			err = i.index.CompareAndSet(indexKey, data, item.Counter())
			if err == badger.ErrCasMismatch {
				continue
			}
		}

		return err
	}
}

func (i *Index) name() string {
	for indexName, index := range i.table.indexes {
		if index == i {
			return i.table.name() + "/" + string(indexName)
		}
	}

	return i.table.name() + "/__unknown_index"
}

// Delete deletes the key from the table. An optional counter value can be
// provided to only delete the document if the counter value is the same.
func (t *Table) Delete(key string, counter ...uint64) error {
	var item badger.KVItem
	err := t.data.Get([]byte(key), &item)
	if err != nil {
		return err
	}

	itemValue := getItemValue(&item)
	if itemValue == nil {
		return nil
	}

	if len(counter) > 0 {
		if item.Counter() != counter[0] {
			return ErrCounterChanged
		}

		err = t.data.CompareAndDelete([]byte(key), counter[0])
	} else {
		err = t.data.Delete([]byte(key))
	}

	if err == badger.ErrCasMismatch {
		return ErrCounterChanged
	}

	if err != nil {
		return err
	}

	t.updateIndex(key, itemValue, nil)

	return nil
}

// Index returns the index object of an index of the table. If the index does
// not exist, nil is returned.
func (t *Table) Index(index string) *Index {
	return t.indexes[Name(index)]
}

// Update updates a document in the table with the given modifier function.
// The modifier function should take in 1 argument, the variable to decode
// the current document value into. The modifier function should return 2
// values, the new value to set the document to, and an error which determines
// whether or not the update should be aborted, and will be returned back from
// Update.
//
// ErrNotFound will be returned if the document does not exist.
//
// The modifier function will be continuously called until the counter at the
// beginning of handler matches the counter when the document is updated.
// This allows for safe updates on a single document, such as incrementing a
// value.
func (t *Table) Update(key string, handler interface{}) error {
	handlerType := reflect.TypeOf(handler)
	if handlerType == nil || handlerType.Kind() != reflect.Func {
		return errors.New("cete: handler must be a function")
	}

	if handlerType.NumIn() != 1 {
		return errors.New("cete: handler must have 1 input argument")
	}

	if handlerType.NumOut() != 2 {
		return errors.New("cete: handler must have 2 return values")
	}

	if !handlerType.Out(1).Implements(reflect.TypeOf((*error)(nil)).
		Elem()) {
		return errors.New("cete: handler must have error as last return value")
	}

	for {
		doc := reflect.New(handlerType.In(0))
		counter, err := t.Get(key, doc.Interface())
		if err != nil {
			return err
		}

		result := reflect.ValueOf(handler).Call([]reflect.Value{doc.Elem()})
		if result[1].Interface() != nil {
			return result[1].Interface().(error)
		}

		err = t.Set(key, result[0].Interface(), counter, 0)
		if err == ErrCounterChanged {
			continue
		}

		return err
	}
}

func (t *Table) name() string {
	foundTable := "__unknown_table"

	for tableName, table := range t.db.tables {
		if table == t {
			foundTable = string(tableName)
			break
		}
	}

	return foundTable
}

// Between returns a Range of documents between the lower and upper key values
// provided. The range will be sorted in ascending order by key. You can
// reverse the sorting by specifying true to the optional reverse parameter.
// The bounds are inclusive on both ends.
//
// You can use cete.MinValue and cete.MaxValue to specify minimum and maximum
// bound values.
func (t *Table) Between(lower interface{}, upper interface{},
	reverse ...bool) *Range {
	if lower == MaxValue || upper == MinValue {
		return newRange(func() (string, []byte, uint64, error) {
			return "", nil, 0, ErrEndOfRange
		}, func() {}, nil)
	}

	shouldReverse := (len(reverse) > 0) && reverse[0]

	itOpts := badger.DefaultIteratorOptions
	itOpts.PrefetchSize = prefetchSize
	itOpts.Reverse = shouldReverse
	it := t.data.NewIterator(itOpts)

	upperString, upperIsString := upper.(string)
	_, upperIsBounds := upper.(Bounds)
	lowerString, lowerIsString := lower.(string)
	_, lowerIsBounds := lower.(Bounds)
	if (!upperIsString && !upperIsBounds) ||
		(!lowerIsString && !lowerIsBounds) {
		log.Println("cete: warning: lower and upper bounds of " +
			"table.Between must be a string or Bounds. An empty range has " +
			"been returned instead")
		return newRange(func() (string, []byte, uint64, error) {
			return "", nil, 0, ErrEndOfRange
		}, func() {}, nil)
	}

	upperBytes := []byte(upperString)
	lowerBytes := []byte(lowerString)

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

	var key string
	var counter uint64
	var value []byte

	return newRange(func() (string, []byte, uint64, error) {
		for it.Valid() {
			if !shouldReverse && upper != MaxValue &&
				bytes.Compare(it.Item().Key(), upperBytes) > 0 {
				return "", nil, 0, ErrEndOfRange
			} else if shouldReverse && lower != MinValue &&
				bytes.Compare(it.Item().Key(), lowerBytes) < 0 {
				return "", nil, 0, ErrEndOfRange
			}

			key = string(it.Item().Key())
			counter = it.Item().Counter()
			itemValue := getItemValue(it.Item())
			value = make([]byte, len(itemValue))
			copy(value, itemValue)
			it.Next()
			return key, value, counter, nil
		}

		return "", nil, 0, ErrEndOfRange
	}, it.Close, t)
}

// CountBetween returns the number of documents whose key values are
// within the given inclusive bounds. Lower and upper must be strings or Bounds.
// It's an optimized version of Between(lower, upper).Count().
func (t *Table) CountBetween(lower, upper interface{}) int64 {
	if lower == MaxValue || upper == MinValue {
		return 0
	}

	itOpts := badger.DefaultIteratorOptions
	itOpts.PrefetchSize = prefetchSize
	itOpts.PrefetchValues = false
	it := t.data.NewIterator(itOpts)

	upperString, isString := upper.(string)
	_, isBounds := upper.(Bounds)
	if !isString && !isBounds {
		log.Println("cete: warning: lower and upper bounds of " +
			"table.CountBetween must be a string or Bounds. A count of 0 has " +
			"been returned instead")
		return 0
	}

	lowerString, isString := lower.(string)
	_, isBounds = lower.(Bounds)
	if !isString && !isBounds {
		log.Println("cete: warning: lower and upper bounds of " +
			"table.CountBetween must be a string or Bounds. A count of 0 has " +
			"been returned instead")
		return 0
	}

	upperBytes := []byte(upperString)
	lowerBytes := []byte(lowerString)

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

		count++

		it.Next()
	}

	return count
}

// All returns all the documents in the table. It is shorthand
// for Between(MinValue, MaxValue, reverse...)
func (t *Table) All(reverse ...bool) *Range {
	return t.Between(MinValue, MaxValue, reverse...)
}

// Indexes returns the list of indexes in the table.
func (t *Table) Indexes() []string {
	var indexes []string
	for name := range t.indexes {
		indexes = append(indexes, string(name))
	}

	return indexes
}

func incrementKey(key string) string {
	byteKey := []byte(key)
	for i, letter := range byteKey {
		if letter < '~' {
			byteKey[i] = letter + 1
			return string(byteKey)
		}

		byteKey[i] = '0'
	}

	byteKey = append(byteKey, '0')
	return string(byteKey)
}

func (t *Table) keyToC(key string, noGenerate ...bool) (string, error) {
	t.compressionLock.RLock()

	shouldGenerate := true
	if len(noGenerate) > 0 && noGenerate[0] {
		shouldGenerate = false
	}

	if value, found := t.keyToCompressed[key]; found {
		t.compressionLock.RUnlock()
		return value, nil
	}
	t.compressionLock.RUnlock()

	if !shouldGenerate {
		return "", ErrNotFound
	}

	t.compressionLock.Lock()
	defer t.compressionLock.Unlock()

	if value, found := t.keyToCompressed[key]; found {
		return value, nil
	}

	newKey := t.nextKey
	t.keyToCompressed[key] = newKey
	t.compressedToKey[newKey] = key
	t.nextKey = incrementKey(newKey)

	if err := t.writeCompressedKeys(); err != nil {
		delete(t.keyToCompressed, key)
		delete(t.compressedToKey, newKey)
		t.nextKey = newKey
		return "", err
	}

	return newKey, nil
}

func (t *Table) writeCompressedKeys() error {
	t.db.configMutex.Lock()
	defer t.db.configMutex.Unlock()

	tableName := t.name()
	for i, table := range t.db.config.Tables {
		if table.TableName == tableName {
			t.db.config.Tables[i].KeyCompression = t.keyToCompressed
			t.db.config.Tables[i].NextKey = t.nextKey
			return t.db.writeConfig()
		}
	}

	return ErrNotFound
}

func (t *Table) cToKey(compressed string) string {
	t.compressionLock.RLock()
	defer t.compressionLock.RUnlock()

	value, found := t.compressedToKey[compressed]
	if !found {
		log.Println("cete: warning: failed to decompress non-existent "+
			"compressed key:", compressed)
		log.Println(string(debug.Stack()))
		return compressed
	}

	return value
}
