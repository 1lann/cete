package cete

import (
	"errors"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/1lann/msgpack"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
)

type indexConfig struct {
	IndexName string
}

type tableConfig struct {
	TableName         string
	Indexes           []indexConfig
	UseKeyCompression bool
	KeyCompression    map[string]string
	NextKey           string
}

type dbConfig struct {
	Tables []tableConfig
}

func (d *DB) newKV(names ...Name) (*badger.KV, error) {
	dir := d.path

	for _, name := range names {
		dir += "/" + name.Hex()
	}

	dir += "/data"

	if found, _ := exists(dir); !found {
		if err := os.MkdirAll(dir, 0744); err != nil {
			return nil, err
		}
	}

	opts := d.openOptions
	opts.Dir = dir
	opts.ValueDir = dir

	kv, err := badger.NewKV(&opts)
	if err != nil {
		return nil, err
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Println("cete: gc panic:", r)
			}
		}()

		for atomic.LoadInt32(&d.closed) == 0 {
			kv.RunValueLogGC(0.2)
			time.Sleep(time.Second * 10)
		}
	}()
	return kv, nil
}

// Open opens the database at the provided path. It will create a new
// database if the folder does not exist.
func Open(path string, opts ...badger.Options) (*DB, error) {
	defaultOpts := badger.DefaultOptions
	defaultOpts.TableLoadingMode = options.MemoryMap

	db := &DB{
		path:        path,
		tables:      make(map[Name]*Table),
		configMutex: new(sync.Mutex),
		openOptions: defaultOpts,
	}

	if len(opts) > 0 {
		db.openOptions = opts[0]
	}

	if ex, _ := exists(path); !ex {
		if err := os.MkdirAll(path, 0744); err != nil {
			return nil, errors.New("cete: failed to create database: " +
				err.Error())
		}

		return db, nil
	}

	file, err := os.Open(path + "/config.dat")
	if err != nil {
		return nil, errors.New("cete: failed to open database configuration. " +
			"If this is new database, please delete the database folder first: " +
			err.Error())
	}

	dec := msgpack.NewDecoder(file)
	var config dbConfig
	err = dec.Decode(&config)
	if err != nil {
		return nil, errors.New("cete: failed to read database configuration: " +
			err.Error())
	}

	db.config = config

	for _, table := range config.Tables {
		tb := &Table{indexes: make(map[Name]*Index)}
		for _, index := range table.Indexes {
			idx := &Index{}

			idx.index, err = db.newKV(Name(table.TableName), Name(index.IndexName))
			if err != nil {
				return nil, errors.New("cete: failed to open " +
					table.TableName + "/" +
					index.IndexName + ": " + err.Error())
			}
			idx.table = tb

			tb.indexes[Name(index.IndexName)] = idx
		}

		tb.data, err = db.newKV(Name(table.TableName))
		if err != nil {
			return nil, errors.New("cete: failed to open " +
				table.TableName + ": " + err.Error())
		}
		tb.db = db

		if table.UseKeyCompression {
			if table.KeyCompression != nil {
				tb.keyToCompressed = table.KeyCompression
			} else {
				tb.keyToCompressed = make(map[string]string)
			}

			tb.compressedToKey = make(map[string]string)
			tb.nextKey = table.NextKey
			tb.compressionLock = new(sync.RWMutex)
			for k, v := range table.KeyCompression {
				tb.compressedToKey[v] = k
			}
		}

		db.tables[Name(table.TableName)] = tb
	}

	return db, nil
}

func (d *DB) writeConfig() error {
	file, err := os.Create(d.path + "/config.dat")
	if err != nil {
		return err
	}

	defer file.Close()

	return msgpack.NewEncoder(file).Encode(d.config)
}
