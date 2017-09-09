package cete

import (
	"errors"
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestDo(t *testing.T) {
	if testing.Short() {
		t.Parallel()
	}

	people := map[string]Person{
		"jason": {
			Name:   "Jason",
			City:   "Sydney",
			Age:    17,
			Height: 1.76,
			DOB:    time.Date(1999, 1, 28, 01, 01, 01, 01, time.UTC),
		},
		"ben": {
			Name:   "Ben",
			City:   "Melbourne",
			Age:    19,
			Height: 1.83,
			DOB:    time.Date(1998, 5, 23, 01, 01, 01, 01, time.UTC),
		},
		"drew": {
			Name:   "Drew",
			City:   "London",
			Age:    18,
			Height: 1.72,
			DOB:    time.Date(2001, 7, 13, 01, 01, 01, 01, time.UTC),
		},
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

	defer func() {
		db.Close()
	}()

	panicNotNil(db.NewTable("do_testing"))

	for name, person := range people {
		panicNotNil(db.Table("do_testing").Set(name, person))
	}

	var sum int32
	panicNotNil(db.Table("do_testing").All().Do(func(key string, counter uint64, doc Document) error {
		atomic.AddInt32(&sum, 1)
		return nil
	}))

	if sum != 3 {
		t.Fatal("sum should be 3, but isn't")
	}

	sum = 0

	panicNotNil(db.Table("do_testing").All().Do(func(key string, counter uint64, doc Document) error {
		sum++
		return nil
	}, 1))

	if sum != 3 {
		t.Fatal("sum should be 3, but isn't")
	}

	sum = 0
	testError := errors.New("cete testing: test do")

	err = db.Table("do_testing").All().Do(func(key string, counter uint64, doc Document) error {
		if key == "ben" {
			time.Sleep(time.Millisecond * 100)
			return testError
		}

		atomic.AddInt32(&sum, 1)
		return nil
	}, 5)
	if err != testError {
		t.Fatal("error should be testError, but isn't")
	}

	if sum != 2 {
		t.Fatal("sum should be 2, but isn't")
	}

	r := newRange(func() (string, []byte, uint64, error) {
		return "", nil, 0, testError
	}, func() {}, nil)

	err = r.Do(func(key string, counter uint64, doc Document) error {
		t.Fatal("do should not run, but does")
		return nil
	})
	if err != testError {
		t.Fatal("error should be testError, but isn't")
	}
}

func TestFilter(t *testing.T) {
	if testing.Short() {
		t.Parallel()
	}

	people := map[string]Person{
		"jason": {
			Name:   "Jason",
			City:   "Sydney",
			Age:    17,
			Height: 1.76,
			DOB:    time.Date(1999, 1, 28, 01, 01, 01, 01, time.UTC),
		},
		"ben": {
			Name:   "Ben",
			City:   "Melbourne",
			Age:    19,
			Height: 1.83,
			DOB:    time.Date(1998, 5, 23, 01, 01, 01, 01, time.UTC),
		},
		"drew": {
			Name:   "Drew",
			City:   "London",
			Age:    18,
			Height: 1.72,
			DOB:    time.Date(2001, 7, 13, 01, 01, 01, 01, time.UTC),
		},
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

	defer func() {
		db.Close()
	}()

	err = db.NewTable("filter_testing")
	panicNotNil(err)

	for name, person := range people {
		err = db.Table("filter_testing").Set(name, person)
		panicNotNil(err)
	}

	r := db.Table("filter_testing").All().Filter(func(doc Document) (bool, error) {
		return doc.QueryInt("Age") > 17, nil
	}, 2)

	expectPerson("ben", r, people["ben"])
	expectPerson("drew", r, people["drew"])

	if r.Next() || r.Error() != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("filter_testing").All().Filter(func(doc Document) (bool, error) {
		return doc.QueryFloat64("Height") > 1.75, nil
	}, 1)

	expectPerson("ben", r, people["ben"])
	expectPerson("jason", r, people["jason"])

	if r.Next() || r.Error() != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("filter_testing").All().Filter(func(doc Document) (bool, error) {
		return doc.QueryTime("DOB").After(time.Date(2000, 01, 01, 01, 01, 01, 01, time.UTC)), nil
	})

	expectPerson("drew", r, people["drew"])

	if r.Next() || r.Error() != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("filter_testing").All().Filter(func(doc Document) (bool, error) {
		return doc.QueryFloat64("Height") > 1.75, nil
	})

	n, err := r.Count()
	panicNotNil(err)
	if n != 2 {
		t.Fatal("count should be 2, but isn't")
	}

	if r.Next() || r.Error() != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("filter_testing").All().Filter(func(doc Document) (bool, error) {
		return doc.QueryFloat64("Height") > 1.75, nil
	}).Skip(2)

	if r.Next() || r.Error() != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("filter_testing").All().Filter(func(doc Document) (bool, error) {
		return doc.QueryFloat64("Height") > 1.75, nil
	}).Skip(1)

	expectPerson("jason", r, people["jason"])

	r = db.Table("filter_testing").All().Filter(func(doc Document) (bool, error) {
		return doc.QueryFloat64("Height") > 1.75, nil
	}).Skip(3)

	if r.Next() || r.Error() != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("filter_testing").All().Filter(func(doc Document) (bool, error) {
		return doc.QueryFloat64("Height") > 0.5, nil
	})

	if !r.Next() {
		t.Fatal("Next should be successful")
	}

	if r.Key() != "ben" {
		t.Fatal("key should be ben, but isn't")
	}

	if !r.Next() {
		t.Fatal("Next should be successful")
	}

	if r.Key() != "drew" {
		t.Fatal("key should be drew, but isn't")
	}

	if !r.Next() {
		t.Fatal("Next should be successful")
	}

	if r.Key() != "jason" {
		t.Fatal("key should be jason, but isn't")
	}

	filterError := errors.New("cete testing: filter error")

	r = db.Table("filter_testing").All().Filter(func(doc Document) (bool, error) {
		return false, filterError
	})

	if r.Next() || r.Error() != filterError {
		t.Fatal("error should be filter error, but isn't")
	}
}
