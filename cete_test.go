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

func TestBasic(t *testing.T) {
	t.Parallel()

	dir, err := ioutil.TempDir("", "cete_")
	if err != nil {
		t.Error(err)
	}

	t.Log("testing directory:", dir)
	defer func() {
		if !t.Failed() {
			os.RemoveAll(dir)
		}
	}()

	db, err := OpenDatabase(dir + "/data")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	err = db.NewTable("testing")
	if err != nil {
		t.Fatal(err)
	}

	if db.Table("does not exist") != nil {
		t.Fatal(err)
	}

	if db.Table("testing") == nil {
		t.Fatal("testing should exist but it doesn't")
	}

	db.Table("testing").Set("bob", "hello")

	err = db.Table("testing").Set("bob", "something", 1000)
	if err != ErrCounterChanged {
		t.Fatal("error should be ErrCounterChanged, but isn't")
	}

	var result string
	_, err = db.Table("testing").Get("bob", &result)
	if err != nil {
		t.Fatal(err)
	}

	if result != "hello" {
		t.Fatal("result should be hello, but isn't")
	}

	err = db.Table("testing").Delete("bob")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Table("testing").Get("bob", &result)
	if err != ErrNotFound {
		t.Fatal("err should be ErrNotFound, but isn't")
	}

	err = db.Table("testing").Drop()
	if err != nil {
		t.Fatal(err)
	}

	if db.Table("testing") != nil {
		t.Fatal("testing should be nil, but isn't")
	}

	db.Close()

	db, err = OpenDatabase(dir + "/data")
	if err != nil {
		t.Fatal(err)
	}
}
