package cete

import (
	"io/ioutil"
	"log"
	"os"
	"strings"
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
	if testing.Short() {
		t.Parallel()
	}

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

	r := db.Table("table_testing").All().Limit(2)

	var person Person

	expectPerson("ben", r, people["ben"])
	expectPerson("drew", r, people["drew"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("table_testing").All().Limit(1)

	expectPerson("ben", r, people["ben"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("table_testing").All().Limit(3)

	expectPerson("ben", r, people["ben"])
	expectPerson("drew", r, people["drew"])
	expectPerson("jason", r, people["jason"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("table_testing").All().Limit(4)

	expectPerson("ben", r, people["ben"])
	expectPerson("drew", r, people["drew"])
	expectPerson("jason", r, people["jason"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("table_testing").All().Limit(1000)

	expectPerson("ben", r, people["ben"])
	expectPerson("drew", r, people["drew"])
	expectPerson("jason", r, people["jason"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("table_testing").Between(MinValue, MaxValue)

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

	r = db.Table("table_testing").Between(MinValue, MaxValue, true)

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

	r = db.Table("table_testing").Between(MinValue, MinValue)

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("table_testing").Between(MaxValue, MaxValue)

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("table_testing").Between("zzzzzzz", MaxValue)

	_, _, err = r.Next(nil)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("table_testing").Between(0, MaxValue)

	_, _, err = r.Next(nil)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("table_testing").Between(MinValue, 0)

	_, _, err = r.Next(nil)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}
}

func TestTableLoading(t *testing.T) {
	if testing.Short() {
		t.Parallel()
	}

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
	if testing.Short() {
		t.Parallel()
	}

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

	person = Person{}
	counter, err = db.Table("table_testing").Get("jason", &person)
	panicNotNil(err)

	if !person.IsSame(people["ben"]) {
		log.Println("this is person:", person)
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

func TestTableNaming(t *testing.T) {
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

	defer func() {
		db.Close()
	}()

	err = db.NewTable("")
	if err != ErrBadIdentifier {
		t.Fatal("error should be ErrBadIdentifier, but isn't")
	}

	err = db.NewTable(strings.Repeat("abcdefghijklmnopqrstuvwxyz", 10))
	if err != ErrBadIdentifier {
		t.Fatal("error should be ErrBadIdentifier, but isn't")
	}

	tableName := "testing 😀 😃 😄 😁 😆 😅 😂 🤣 😊 😇 🙂 🙃 😉 😌 😍 😘 😗 "

	panicNotNil(db.NewTable(tableName))

	err = db.NewTable(tableName)
	if err != ErrAlreadyExists {
		t.Fatal("error should be ErrAlreadyExists, but isn't")
	}

	err = db.Table(tableName).NewIndex("")
	if err != ErrBadIdentifier {
		t.Fatal("error should be ErrBadIdentifier, but isn't")
	}

	err = db.Table(tableName).NewIndex(strings.Repeat("abcdefghijklmnopqrstuvwxyz", 10))
	if err != ErrBadIdentifier {
		t.Fatal("error should be ErrBadIdentifier, but isn't")
	}

	panicNotNil(db.Table(tableName).NewIndex(tableName))

	err = db.Table(tableName).NewIndex(tableName)
	if err != ErrAlreadyExists {
		t.Fatal("error should be ErrAlreadyExists, but isn't")
	}
}

func TestInvalidTypes(t *testing.T) {
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

	defer func() {
		db.Close()
	}()

	panicNotNil(db.NewTable("types_testing"))

	err = db.Table("types_testing").Set("invalid", func() {})
	if err == nil {
		t.Fatal("set should have an error, but doesn't")
	}

	panicNotNil(db.Table("types_testing").Set("valid", "just some data"))

}
