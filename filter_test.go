package cete

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestFilter(t *testing.T) {
	// t.Parallel()

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

	r := db.Table("filter_testing").All().Filter(func(doc Document) bool {
		return doc.QueryInt("Age") > 17
	})

	expectPerson("ben", r, people["ben"])
	expectPerson("drew", r, people["drew"])

	var person Person
	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("filter_testing").All().Filter(func(doc Document) bool {
		return doc.QueryFloat64("Height") > 1.75
	})

	expectPerson("ben", r, people["ben"])
	expectPerson("jason", r, people["jason"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("filter_testing").All().Filter(func(doc Document) bool {
		return doc.QueryTime("DOB").After(time.Date(2000, 01, 01, 01, 01, 01, 01, time.UTC))
	})

	expectPerson("drew", r, people["drew"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("filter_testing").All().Filter(func(doc Document) bool {
		return doc.QueryFloat64("Height") > 1.75
	})

	n, err := r.Count()
	panicNotNil(err)
	if n != 2 {
		t.Fatal("count should be 2, but isn't")
	}

	_, _, err = r.Next(nil)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("filter_testing").All().Filter(func(doc Document) bool {
		return doc.QueryFloat64("Height") > 1.75
	})

	err = r.Skip(2)
	panicNotNil(err)

	r = db.Table("filter_testing").All().Filter(func(doc Document) bool {
		return doc.QueryFloat64("Height") > 1.75
	})

	err = r.Skip(1)
	panicNotNil(err)

	expectPerson("jason", r, people["jason"])

	r = db.Table("filter_testing").All().Filter(func(doc Document) bool {
		return doc.QueryFloat64("Height") > 1.75
	})

	err = r.Skip(3)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}
}
