package cete

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

type Person struct {
	Name   string
	City   string
	Age    int
	Height float64
	DOB    time.Time
}

func (a Person) IsSame(b Person) bool {
	return a.Name == b.Name && a.City == b.City && a.Age == b.Age
}

func TestPostIndex(t *testing.T) {
	t.Parallel()

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

	defer db.Close()

	err = db.NewTable("index_testing")
	panicNotNil(err)

	err = db.Table("index_testing").NewIndex("Age")
	panicNotNil(err)

	for name, person := range people {
		err = db.Table("index_testing").Set(name, person)
		panicNotNil(err)
	}

	var person Person
	key, _, err := db.Table("index_testing").Index("Age").One(19, &person)
	panicNotNil(err)

	if key != "ben" {
		t.Fatal("key should be ben, but isn't")
	}

	if !person.IsSame(people["ben"]) {
		t.Fatal("person should ben, but isn't")
	}

	_, _, err = db.Table("index_testing").Index("Age").One(22, &person)
	if err != ErrNotFound {
		t.Fatal("error should be ErrNotFound, but isn't")
	}

	var a Person
	var b Person

	r, err := db.Table("index_testing").Index("Age").GetAll(18)
	panicNotNil(err)

	_, _, err = r.Next(&a)
	panicNotNil(err)
	_, _, err = r.Next(&b)
	panicNotNil(err)

	_, _, err = r.Next(&b)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if !((a.IsSame(people["jason"]) || a.IsSame(people["drew"])) &&
		(b.IsSame(people["jason"]) || b.IsSame(people["drew"]))) {
		t.Fatal("a and b should be jason or drew, but aren't")
	}

	if a.IsSame(b) {
		t.Fatal("a and be should not be the same, but are")
	}

	a = Person{}
	b = Person{}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(18, 18, false)
	_, _, err = r.Next(&a)
	panicNotNil(err)
	_, _, err = r.Next(&b)
	panicNotNil(err)

	_, _, err = r.Next(&b)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if !((a.IsSame(people["jason"]) || a.IsSame(people["drew"])) &&
		(b.IsSame(people["jason"]) || b.IsSame(people["drew"]))) {
		t.Fatal("a and b should be jason or drew, but aren't")
	}

	if a.IsSame(b) {
		t.Fatal("a and be should not be the same, but are")
	}
}

func TestPreIndex(t *testing.T) {
	t.Parallel()

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

	defer db.Close()

	err = db.NewTable("index_testing")
	panicNotNil(err)

	for name, person := range people {
		err = db.Table("index_testing").Set(name, person)
		panicNotNil(err)
	}

	err = db.Table("index_testing").NewIndex("Age")
	panicNotNil(err)

	var person Person
	key, _, err := db.Table("index_testing").Index("Age").One(19, &person)
	panicNotNil(err)

	if key != "ben" {
		t.Fatal("key should be ben, but isn't")
	}

	if !person.IsSame(people["ben"]) {
		t.Fatal("person should ben, but isn't")
	}

	_, _, err = db.Table("index_testing").Index("Age").One(22, &person)
	if err != ErrNotFound {
		t.Fatal("error should be ErrNotFound, but isn't")
	}

	var a Person
	var b Person

	r, err := db.Table("index_testing").Index("Age").GetAll(18)
	panicNotNil(err)

	_, _, err = r.Next(&a)
	panicNotNil(err)
	_, _, err = r.Next(&b)
	panicNotNil(err)

	_, _, err = r.Next(&b)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if !((a.IsSame(people["jason"]) || a.IsSame(people["drew"])) &&
		(b.IsSame(people["jason"]) || b.IsSame(people["drew"]))) {
		t.Fatal("a and b should be jason or drew, but aren't")
	}

	if a.IsSame(b) {
		t.Fatal("a and be should not be the same, but are")
	}

	a = Person{}
	b = Person{}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(18, 18, false)
	_, _, err = r.Next(&a)
	panicNotNil(err)
	_, _, err = r.Next(&b)
	panicNotNil(err)

	_, _, err = r.Next(&b)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if !((a.IsSame(people["jason"]) || a.IsSame(people["drew"])) &&
		(b.IsSame(people["jason"]) || b.IsSame(people["drew"]))) {
		t.Fatal("a and b should be jason or drew, but aren't")
	}

	if a.IsSame(b) {
		t.Fatal("a and be should not be the same, but are")
	}
}

func TestIndexDrop(t *testing.T) {
	t.Parallel()

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

	defer db.Close()

	err = db.NewTable("index_testing")
	panicNotNil(err)

	err = db.Table("index_testing").NewIndex("Age")
	panicNotNil(err)

	for name, person := range people {
		err = db.Table("index_testing").Set(name, person)
		panicNotNil(err)
	}

	err = db.Table("index_testing").NewIndex("Age")
	if err != ErrAlreadyExists {
		t.Fatal("error should be ErrAlreadyExists, but isn't")
	}

	err = db.Table("index_testing").Index("Age").Drop()
	panicNotNil(err)

	if db.Table("index_testing").Index("Age") != nil {
		t.Fatal("index Age should be nil, but isn't")
	}

	err = db.Table("index_testing").NewIndex("Age")
	panicNotNil(err)

	var person Person
	key, _, err := db.Table("index_testing").Index("Age").One(19, &person)
	panicNotNil(err)

	if key != "ben" {
		t.Fatal("key should be ben, but isn't")
	}

	if !person.IsSame(people["ben"]) {
		t.Fatal("person should ben, but isn't")
	}

	_, _, err = db.Table("index_testing").Index("Age").One(22, &person)
	if err != ErrNotFound {
		t.Fatal("error should be ErrNotFound, but isn't")
	}

	var a Person
	var b Person

	r, err := db.Table("index_testing").Index("Age").GetAll(18)
	panicNotNil(err)

	_, _, err = r.Next(&a)
	panicNotNil(err)
	_, _, err = r.Next(&b)
	panicNotNil(err)

	_, _, err = r.Next(&b)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if !((a.IsSame(people["jason"]) || a.IsSame(people["drew"])) &&
		(b.IsSame(people["jason"]) || b.IsSame(people["drew"]))) {
		t.Fatal("a and b should be jason or drew, but aren't")
	}

	if a.IsSame(b) {
		t.Fatal("a and be should not be the same, but are")
	}

	a = Person{}
	b = Person{}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(18, 18, false)
	_, _, err = r.Next(&a)
	panicNotNil(err)
	_, _, err = r.Next(&b)
	panicNotNil(err)

	_, _, err = r.Next(&b)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if !((a.IsSame(people["jason"]) || a.IsSame(people["drew"])) &&
		(b.IsSame(people["jason"]) || b.IsSame(people["drew"]))) {
		t.Fatal("a and b should be jason or drew, but aren't")
	}

	if a.IsSame(b) {
		t.Fatal("a and be should not be the same, but are")
	}
}

func TestIndexBetween(t *testing.T) {
	t.Parallel()

	people := map[string]Person{
		"jason": {
			Name: "Jason",
			City: "Sydney",
			Age:  20,
		},
		"ben": {
			Name: "Ben",
			City: "Melbourne",
			Age:  10,
		},
		"drew": {
			Name: "Drew",
			City: "London",
			Age:  15,
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

	defer db.Close()

	err = db.NewTable("index_testing")
	panicNotNil(err)

	err = db.Table("index_testing").NewIndex("Age")
	panicNotNil(err)

	for name, person := range people {
		err = db.Table("index_testing").Set(name, person)
		panicNotNil(err)
	}

	r := db.Table("index_testing").Index("Age").Between(MinBounds, MaxBounds)

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

	r = db.Table("index_testing").Index("Age").Between(MinBounds, MaxBounds, true)

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

	r = db.Table("index_testing").Index("Age").Between(14, 16, true)

	expectPerson("drew", r, people["drew"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(16, 14, true)

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(20, 14)

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(14, 20, true)

	expectPerson("jason", r, people["jason"])
	expectPerson("drew", r, people["drew"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(1, 14)

	expectPerson("ben", r, people["ben"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(1, 17)

	expectPerson("ben", r, people["ben"])
	expectPerson("drew", r, people["drew"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(17, 1, true)

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(1, 17, true)

	expectPerson("drew", r, people["drew"])
	expectPerson("ben", r, people["ben"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}
}

func TestIndexSet(t *testing.T) {
	t.Parallel()

	people := map[string]Person{
		"jason": {
			Name: "Jason",
			City: "Sydney",
			Age:  20,
		},
		"ben": {
			Name: "Ben",
			City: "Melbourne",
			Age:  10,
		},
		"drew": {
			Name: "Drew",
			City: "London",
			Age:  15,
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

	defer db.Close()

	err = db.NewTable("index_testing")
	panicNotNil(err)

	err = db.Table("index_testing").NewIndex("Age")
	panicNotNil(err)

	for name := range people {
		err = db.Table("index_testing").Set(name, people["jason"])
		panicNotNil(err)
	}

	for name, person := range people {
		err = db.Table("index_testing").Set(name, person)
		panicNotNil(err)
	}

	r := db.Table("index_testing").Index("Age").Between(MinBounds, MaxBounds)

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

	r = db.Table("index_testing").Index("Age").Between(MinBounds, MaxBounds, true)

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

	r = db.Table("index_testing").Index("Age").Between(14, 16, true)

	expectPerson("drew", r, people["drew"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(16, 14, true)

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(20, 14)

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(14, 20, true)

	expectPerson("jason", r, people["jason"])
	expectPerson("drew", r, people["drew"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(1, 14)

	expectPerson("ben", r, people["ben"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(1, 17)

	expectPerson("ben", r, people["ben"])
	expectPerson("drew", r, people["drew"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(17, 1, true)

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(1, 17, true)

	expectPerson("drew", r, people["drew"])
	expectPerson("ben", r, people["ben"])

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}
}

func TestTimeOrder(t *testing.T) {
	t.Parallel()

	if bytes.Compare(valueToBytes(time.Now()),
		valueToBytes(time.Now().Add(time.Minute))) >= 0 {
		t.Fatal("now should be less than a minute in a future, but isn't")
	}

	if bytes.Compare(valueToBytes(time.Now().Add(time.Minute)),
		valueToBytes(time.Now())) <= 0 {
		t.Fatal("now should be less than a minute in a future, but isn't")
	}

	sameTime := time.Now()

	if bytes.Compare(valueToBytes(sameTime), valueToBytes(sameTime)) != 0 {
		t.Fatal("time should obey reflexive property of equality, but isn't")
	}
}

func TestFloatOrder(t *testing.T) {
	t.Parallel()

	if bytes.Compare(valueToBytes(12.34),
		valueToBytes(12.35)) >= 0 {
		t.Fatal("now should be less than a minute in a future, but isn't")
	}

	if bytes.Compare(valueToBytes(12.35),
		valueToBytes(12.34)) <= 0 {
		t.Fatal("now should be less than a minute in a future, but isn't")
	}

	if bytes.Compare(valueToBytes(12.34), valueToBytes(12.34)) != 0 {
		t.Fatal("time should obey reflexive property of equality, but isn't")
	}
}

func TestIndexLoading(t *testing.T) {
	t.Parallel()

	people := map[string]Person{
		"jason": {
			Name: "Jason",
			City: "Sydney",
			Age:  17,
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

	err = db.NewTable("index_testing")
	panicNotNil(err)

	for name, person := range people {
		err = db.Table("index_testing").Set(name, person)
		panicNotNil(err)
	}

	err = db.Table("index_testing").NewIndex("Age")
	panicNotNil(err)

	if db.Tables()[0] != "index_testing" {
		t.Fatal("Tables should return index_testing, but it didn't")
	}

	if db.Table("index_testing").Indexes()[0] != "Age" {
		t.Fatal("Indexes should return Age, but it didn't")
	}

	db.Close()

	db, err = Open(dir + "/data")
	panicNotNil(err)

	if db.Tables()[0] != "index_testing" {
		t.Fatal("Tables should return index_testing, but it didn't")
	}

	if db.Table("index_testing").Indexes()[0] != "Age" {
		t.Fatal("Indexes should return Age, but it didn't")
	}

	var person Person
	_, _, err = db.Table("index_testing").Index("Age").One(17, &person)
	panicNotNil(err)

	if !person.IsSame(people["jason"]) {
		t.Fatal("person should be same as jason, but isn't")
	}

	err = db.Table("index_testing").Index("Age").Drop()
	panicNotNil(err)

	db.Close()

	db, err = Open(dir + "/data")
	panicNotNil(err)

	if db.Tables()[0] != "index_testing" {
		t.Fatal("Tables should return index_testing, but it didn't")
	}

	if len(db.Table("index_testing").Indexes()) != 0 {
		t.Fatal("Indexes should be empty, but isn't")
	}

	if db.Table("index_testing").Index("Age") != nil {
		t.Fatal("table should be nil, but isn't")
	}
}

func TestIndexDelete(t *testing.T) {
	t.Parallel()

	people := map[string]Person{
		"jason": {
			Name: "Jason",
			City: "Sydney",
			Age:  17,
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

	err = db.NewTable("index_testing")
	panicNotNil(err)

	for name, person := range people {
		err = db.Table("index_testing").Set(name, person)
		panicNotNil(err)
	}

	err = db.Table("index_testing").NewIndex("Age")
	panicNotNil(err)

	db.Table("index_testing").Delete("jason")

	var person Person
	_, _, err = db.Table("index_testing").Index("Age").One(17, &person)
	if err != ErrNotFound {
		t.Fatal("error should be ErrNotFound, but isn't")
	}

	db.Table("index_testing").Set("jason", people["ben"])

	_, _, err = db.Table("index_testing").Index("Age").One(17, &person)
	if err != ErrNotFound {
		t.Fatal("error should be ErrNotFound, but isn't")
	}

	db.Table("index_testing").Set("jason", people["jason"])

	_, _, err = db.Table("index_testing").Index("Age").One(17, &person)
	panicNotNil(err)

	if !person.IsSame(people["jason"]) {
		t.Fatal("person should be same as jason, but isn't")
	}
}
