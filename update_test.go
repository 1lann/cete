package cete

import (
	"errors"
	"io/ioutil"
	"os"
	"sync"
	"testing"
)

type Counter struct {
	Count int
}

func TestConsistency(t *testing.T) {
	if testing.Short() {
		t.Parallel()
	}

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

	err = db.NewTable("table_update")
	panicNotNil(err)

	err = db.Table("table_update").Set("test", Counter{
		Count: 0,
	})
	panicNotNil(err)

	gun := new(sync.WaitGroup)
	gun.Add(1)

	wg := new(sync.WaitGroup)
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()

			gun.Wait()

			uErr := db.Table("table_update").Update("test",
				func(c Counter) (Counter, error) {
					c.Count++
					return c, nil
				})
			panicNotNil(uErr)
		}()
	}

	gun.Done()

	wg.Wait()
	var counter Counter
	_, err = db.Table("table_update").Get("test", &counter)
	panicNotNil(err)

	if counter.Count != 100 {
		t.Fatal("count should be 200, but isn't")
	}
}

func TestUpdateErrors(t *testing.T) {
	if testing.Short() {
		t.Parallel()
	}

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

	err = db.NewTable("table_update")
	panicNotNil(err)

	err = db.Table("table_update").Set("test", Counter{
		Count: 0,
	})
	panicNotNil(err)

	err = db.Table("table_update").Update("test", nil)
	if err.Error() != "cete: handler must be a function" {
		t.Fatal("incorrect error message")
	}

	err = db.Table("table_update").Update("test", func() {})
	if err.Error() != "cete: handler must have 1 input argument" {
		t.Fatal("incorrect error message")
	}

	err = db.Table("table_update").Update("test", func(c Counter) {})
	if err.Error() != "cete: handler must have 2 return values" {
		t.Fatal("incorrect error message")
	}

	err = db.Table("table_update").Update("test",
		func(c Counter) (error, Counter) { return nil, Counter{} })
	if err.Error() != "cete: handler must have error as last return value" {
		t.Fatal("incorrect error message")
	}

	err = db.Table("table_update").Update("not exist",
		func(c Counter) (Counter, error) { return Counter{}, nil })
	if err != ErrNotFound {
		t.Fatal("error should be ErrNotFound, but isn't")
	}

	err = db.Table("table_update").Update("test",
		func(c func()) (Counter, error) { return Counter{}, nil })
	if err == nil {
		t.Fatal("error should not be nil, but is")
	}

	err = db.Table("table_update").Update("test",
		func(c Counter) (func(), error) { return func() {}, nil })
	if err == nil {
		t.Fatal("error should not be nil, but is")
	}

	err = db.Table("table_update").Update("test",
		func(c Counter) (Counter, error) { return c, nil })
	panicNotNil(err)

	testError := errors.New("cete testing: test error")
	err = db.Table("table_update").Update("test",
		func(c Counter) (Counter, error) { return c, testError })
	if err != testError {
		t.Fatal("error should be testError, but isn't")
	}
}
