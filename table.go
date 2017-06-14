package cete

import (
	"bytes"
	"errors"
	"log"
	"os"
	"reflect"

	"github.com/dgraph-io/badger/badger"
	"gopkg.in/vmihailenco/msgpack.v2"
)

// NewTable creates a new table in the database.
func (db *DB) NewTable(name string) error {
	if name == "" || len(name) > 125 {
		return ErrBadIdentifier
	}

	db.configMutex.Lock()
	defer db.configMutex.Unlock()

	for _, table := range db.config.Tables {
		if table.TableName == name {
			return ErrAlreadyExists
		}
	}

	kv, err := db.newKV(Name(name))
	if err != nil {
		return err
	}

	db.config.Tables = append(db.config.Tables, tableConfig{TableName: name})
	if err := db.writeConfig(); err != nil {
		return err
	}

	db.tables[Name(name)] = &Table{
		indexes: make(map[Name]*Index),
		data:    kv,
		db:      db,
	}

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

// Get retrieves a value from a table with its primary key.
func (t *Table) Get(key string, dst interface{}) (int, error) {
	var item badger.KVItem
	err := t.data.Get([]byte(key), &item)
	if err != nil {
		return 0, err
	}

	if item.Value() == nil {
		return 0, ErrNotFound
	}

	return int(item.Counter()), msgpack.Unmarshal(item.Value(), dst)
}

// Set sets a value in the table, an optional counter value can be provided
// to only set the value if the counter value is the same.
func (t *Table) Set(key string, value interface{}, counter ...int) error {
	var item badger.KVItem
	err := t.data.Get([]byte(key), &item)
	if err != nil {
		return err
	}

	if len(counter) > 0 {
		if item.Counter() != uint16(counter[0]) {
			return ErrCounterChanged
		}
	}

	data, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}

	if len(counter) > 0 {
		err = t.data.CompareAndSet([]byte(key), data, uint16(counter[0]))
	} else {
		err = t.data.Set([]byte(key), data)
	}

	if err == badger.CasMismatch {
		return ErrCounterChanged
	}

	if err != nil {
		return err
	}

	t.updateIndex(key, item.Value(), data)

	return nil
}

func (t *Table) updateIndex(key string, old, new []byte) error {
	type entry struct {
		indexName string
		indexKey  []byte
	}

	var removals []entry
	var additions []entry

	for indexName := range t.indexes {
		oldRawValues, _ := msgpack.NewDecoder(bytes.NewReader(old)).
			Query(string(indexName))
		newRawValues, _ := msgpack.NewDecoder(bytes.NewReader(new)).
			Query(string(indexName))

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

		for _, newValue := range newValues {
			found := false
			for _, oldValue := range oldValues {
				if bytes.Equal(newValue, oldValue) {
					found = true
					break
				}
			}

			if !found {
				additions = append(additions, entry{string(indexName), newValue})
			}
		}

		for _, oldValue := range oldValues {
			found := false
			for _, newValue := range newValues {
				if bytes.Equal(newValue, oldValue) {
					found = true
					break
				}
			}

			if !found {
				removals = append(removals, entry{string(indexName), oldValue})
			}
		}
	}

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

		if item.Value() == nil {
			log.Println("cete: warning: corrupt index detected:", i.name())
			return nil
		}

		var list []string
		err = msgpack.Unmarshal(item.Value(), &list)
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
			if err == badger.CasMismatch {
				continue
			}

			return err
		}

		data, err := msgpack.Marshal(list)
		if err != nil {
			log.Fatal("cete: marshal should never fail: ", err)
		}

		err = i.index.CompareAndSet(indexKey, data, item.Counter())
		if err == badger.CasMismatch {
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

		if item.Value() != nil {
			err = msgpack.Unmarshal(item.Value(), &list)
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

		if item.Value() != nil {
			err = i.index.CompareAndSet(indexKey, data, item.Counter())
			if err == badger.CasMismatch {
				continue
			}

			return err
		}

		return i.index.Set(indexKey, data)
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

func (t *Table) Delete(key string, counter ...int) error {
	var item badger.KVItem
	err := t.data.Get([]byte(key), &item)
	if err != nil {
		return err
	}

	if item.Value() == nil {
		return nil
	}

	if len(counter) > 0 {
		if int(item.Counter()) != counter[0] {
			return ErrCounterChanged
		}

		err = t.data.CompareAndDelete([]byte(key), uint16(counter[0]))
	} else {
		err = t.data.Delete([]byte(key))
	}

	if err == badger.CasMismatch {
		return ErrCounterChanged
	}

	if err != nil {
		return err
	}

	t.updateIndex(key, item.Value(), nil)

	return nil
}

func (t *Table) Index(index string) *Index {
	return t.indexes[Name(index)]
}

func (t *Table) Update(key string, handler interface{}) error {
	handlerType := reflect.TypeOf(handler)
	if handlerType.Kind() != reflect.Func {
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

		err = t.Set(key, result[0].Interface(), counter)
		if err == ErrCounterChanged {
			continue
		}

		return err
	}
}

func (t *Table) name() string {
	for tableName, table := range t.db.tables {
		if table == t {
			return string(tableName)
		}
	}

	return "__unknown_table"
}

func (t *Table) Between(lower interface{}, upper interface{},
	reverse ...bool) *Range {
	shouldReverse := (len(reverse) > 0) && reverse[0]

	itOpts := badger.DefaultIteratorOptions
	itOpts.PrefetchSize = 5
	itOpts.Reverse = shouldReverse
	it := t.data.NewIterator(itOpts)

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

	var key string
	var counter int
	var value []byte

	return newRange(func() (string, []byte, int, error) {
		for it.Valid() {
			if !shouldReverse && upper != MaxBounds &&
				bytes.Compare(it.Item().Key(), upperBytes) > 0 {
				return "", nil, 0, ErrEndOfRange
			} else if shouldReverse && lower != MinBounds &&
				bytes.Compare(it.Item().Key(), lowerBytes) < 0 {
				return "", nil, 0, ErrEndOfRange
			}

			key = string(it.Item().Key())
			counter = int(it.Item().Counter())
			value = make([]byte, len(it.Item().Value()))
			copy(value, it.Item().Value())
			it.Next()
			return key, value, counter, nil
		}

		return "", nil, 0, ErrEndOfRange
	}, it.Close)
}

func (t *Table) All(reverse ...bool) *Range {
	return t.Between(MinBounds, MaxBounds, reverse...)
}

// Indexes returns the list of indexes in the table.
func (t *Table) Indexes() []string {
	var indexes []string
	for name := range t.indexes {
		indexes = append(indexes, string(name))
	}

	return indexes
}
