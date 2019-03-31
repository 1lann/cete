package cete

import "github.com/dgraph-io/badger"

func intermediateGet(db *badger.DB, key []byte) (*badger.Item, error) {
	tx := db.NewTransaction(false)
	defer tx.Discard()
	return tx.Get(key)
}

func intermediateSet(db *badger.DB, key, value []byte) error {
	tx := db.NewTransaction(true)
	err := tx.Set(key, value)
	if err != nil {
		tx.Discard()
		return err
	}
	return tx.Commit()
}

func intermediateCAS(db *badger.DB, key, value []byte, counter uint64) error {
	tx := db.NewTransaction(true)
	defer tx.Discard()

	it, err := tx.Get(key)
	if err != nil && (err != badger.ErrKeyNotFound || counter != 0) {
		return err
	} else if err == nil && it.Version() != counter {
		return ErrCounterChanged
	}

	err = tx.Set(key, value)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func intermediateDelete(db *badger.DB, key []byte) error {
	tx := db.NewTransaction(true)
	err := tx.Delete(key)
	if err != nil {
		tx.Discard()
		return err
	}
	return tx.Commit()
}

func intermediateCAD(db *badger.DB, key []byte, counter uint64) error {
	tx := db.NewTransaction(true)
	defer tx.Discard()

	it, err := tx.Get(key)
	if err != nil && (err != badger.ErrKeyNotFound || counter != 0) {
		return err
	} else if err == nil && it.Version() != counter {
		return ErrCounterChanged
	}

	err = tx.Delete(key)
	if err != nil {
		return err
	}

	return tx.Commit()
}
