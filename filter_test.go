package cete

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestFilter(t *testing.T) {
	t.Parallel()

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

	defer func() {
		db.Close()
	}()

	err = db.NewTable("filter_testing")
	if err != nil {
		t.Fatal(err)
	}

	for name, person := range people {
		err = db.Table("filter_testing").Set(name, person)
		if err != nil {
			t.Fatal(err)
		}
	}

	r := db.Table("filter_testing").All().Filter(func(doc Document) bool {
		return doc.QueryInt("Age") > 17
	})

	defer func() {
		r.Close()
	}()

	expectPerson("ben", r, people["ben"])
	expectPerson("drew", r, people["drew"])

	var person Person
	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r.Close()

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

}
