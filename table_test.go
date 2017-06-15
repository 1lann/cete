package cete

import (
	"io/ioutil"
	"os"
	"testing"
)

func expectPerson(key string, r *Range, person Person) {
	var nextPerson Person
	nextKey, _, err := r.Next(&nextPerson)
	panicNotNil(err)

	if nextKey != key {
		panic("key should be " + key + ", but isn't")
	}

	if !nextPerson.IsSame(person) {
		panic("person should be the same as " + key + ", but isn't")
	}
}

func TestTableBetween(t *testing.T) {
	// t.Parallel()

	people := map[string]Person{
		"jason": {
			Name: "Jason",
			City: "Sydney",
			Age:  18,
		},
		"ben": {
			Name: "Ben",
			City: "Melbourne",
			Age:  19,
		},
		"drew": {
			Name: "Drew",
			City: "London",
			Age:  18,
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

	err = db.NewTable("table_testing")
	panicNotNil(err)

	for name, person := range people {
		err = db.Table("table_testing").Set(name, person)
		panicNotNil(err)
	}

	r := db.Table("table_testing").Between(MinBounds, MaxBounds)

	var person Person

	expectPerson("ben", r, people["ben"])
	expectPerson("drew", r, people["drew"])
	expectPerson("jason", r, people["jason"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("table_testing").Between(MinBounds, MaxBounds, true)

	expectPerson("jason", r, people["jason"])
	expectPerson("drew", r, people["drew"])
	expectPerson("ben", r, people["ben"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("table_testing").Between("draw", "drfw", true)

	expectPerson("drew", r, people["drew"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("table_testing").Between("drgw", "drfw", true)

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("table_testing").Between("jason", "draw")

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("table_testing").Between("draw", "jason", true)

	expectPerson("jason", r, people["jason"])
	expectPerson("drew", r, people["drew"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("table_testing").Between("a", "draw")

	expectPerson("ben", r, people["ben"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("table_testing").Between("a", "ivan")

	expectPerson("ben", r, people["ben"])
	expectPerson("drew", r, people["drew"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("table_testing").Between("ivan", "a", true)

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("table_testing").Between("a", "ivan", true)

	expectPerson("drew", r, people["drew"])
	expectPerson("ben", r, people["ben"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}
}

func TestTableLoading(t *testing.T) {
	// t.Parallel()

	people := map[string]Person{
		"jason": {
			Name: "Jason",
			City: "Sydney",
			Age:  18,
		},
		"ben": {
			Name: "Ben",
			City: "Melbourne",
			Age:  19,
		},
		"drew": {
			Name: "Drew",
			City: "London",
			Age:  18,
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

	err = db.NewTable("table_testing")
	panicNotNil(err)

	if db.Tables()[0] != "table_testing" {
		t.Fatal("Tables should return index_testing, but it didn't")
	}

	for name, person := range people {
		err = db.Table("table_testing").Set(name, person)
		panicNotNil(err)
	}

	db.Close()

	db, err = Open(dir + "/data")
	panicNotNil(err)

	if db.Tables()[0] != "table_testing" {
		t.Fatal("Tables should return index_testing, but it didn't")
	}

	if db.Table("table_testing") == nil {
		t.Fatal("table should not be nil, but is")
	}

	var person Person
	_, err = db.Table("table_testing").Get("jason", &person)
	panicNotNil(err)

	if !person.IsSame(people["jason"]) {
		t.Fatal("person should be same as jason, but isn't")
	}

	err = db.Table("table_testing").Drop()
	panicNotNil(err)

	if len(db.Tables()) != 0 {
		t.Fatal("Tables should be empty, but isn't")
	}

	if db.Table("table_testing") != nil {
		t.Fatal("table should be nil, but isn't")
	}

	db.Close()

	db, err = Open(dir + "/data")
	panicNotNil(err)

	if len(db.Tables()) != 0 {
		t.Fatal("Tables should be empty, but isn't")
	}

	if db.Table("table_testing") != nil {
		t.Fatal("table should be nil, but isn't")
	}
}

func TestTableCounter(t *testing.T) {
	// t.Parallel()

	people := map[string]Person{
		"jason": {
			Name: "Jason",
			City: "Sydney",
			Age:  18,
		},
		"ben": {
			Name: "Ben",
			City: "Melbourne",
			Age:  19,
		},
		"drew": {
			Name: "Drew",
			City: "London",
			Age:  18,
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

	err = db.NewTable("table_testing")
	panicNotNil(err)

	for name, person := range people {
		err = db.Table("table_testing").Set(name, person)
		panicNotNil(err)
	}

	var person Person
	counter, err := db.Table("table_testing").Get("jason", &person)
	panicNotNil(err)

	err = db.Table("table_testing").Set("jason", people["ben"], counter+1)
	if err != ErrCounterChanged {
		t.Fatal("error should be ErrCounterChanged, but isn't")
	}

	err = db.Table("table_testing").Set("jason", people["ben"], counter)
	panicNotNil(err)

	counter, err = db.Table("table_testing").Get("jason", &person)
	panicNotNil(err)

	if !person.IsSame(people["ben"]) {
		t.Fatal("person should be same as ben, but isn't")
	}

	err = db.Table("table_testing").Delete("jason", counter+1)
	if err != ErrCounterChanged {
		t.Fatal("error should be ErrCounterChanged, but isn't")
	}

	err = db.Table("table_testing").Set("jason", people["ben"])
	panicNotNil(err)

	newCounter, err := db.Table("table_testing").Get("jason", &person)
	panicNotNil(err)

	err = db.Table("table_testing").Delete("jason", counter)
	if err != ErrCounterChanged {
		t.Fatal("error should be ErrCounterChanged, but isn't")
	}

	err = db.Table("table_testing").Delete("jason", newCounter)
	panicNotNil(err)

	_, err = db.Table("table_testing").Get("jason", &person)
	if err != ErrNotFound {
		t.Fatal("error should be ErrNotFound, but isn't")
	}
}
