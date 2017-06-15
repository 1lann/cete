package cete

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func panicNotNil(err error) {
	if err != nil {
		panic(err)
	}
}

func TestBasic(t *testing.T) {
	t.Parallel()

	dir, err := ioutil.TempDir("", "cete_")
	panicNotNil(err)

	t.Log("testing directory:", dir)
	defer func() {
		if !t.Failed() {
			os.RemoveAll(dir)
		}
	}()

	db, err := Open(dir + "/data")
	panicNotNil(err)

	defer db.Close()

	err = db.NewTable("testing")
	panicNotNil(err)

	if db.Table("does not exist") != nil {
		t.Fatal("table should not exist, but does")
	}

	if db.Table("testing") == nil {
		t.Fatal("testing should exist but, it doesn't")
	}

	db.Table("testing").Set("bob", "hello")

	err = db.Table("testing").Set("bob", "something", 1000)
	if err != ErrCounterChanged {
		t.Fatal("error should be ErrCounterChanged, but isn't")
	}

	var result string
	_, err = db.Table("testing").Get("bob", &result)
	panicNotNil(err)

	if result != "hello" {
		t.Fatal("result should be hello, but isn't")
	}

	err = db.Table("testing").Delete("bob")
	panicNotNil(err)

	_, err = db.Table("testing").Get("bob", &result)
	if err != ErrNotFound {
		t.Fatal("err should be ErrNotFound, but isn't")
	}

	err = db.Table("testing").Drop()
	panicNotNil(err)

	if db.Table("testing") != nil {
		t.Fatal("testing should be nil, but isn't")
	}

	db.Close()

	db, err = Open(dir + "/data")
	panicNotNil(err)
}
