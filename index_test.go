package cete

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Person struct {
	Name   string
	City   string
	Age    int
	Height float64
	Likes  []string
	DOB    time.Time
	Data   []byte
}

func (a Person) IsSame(b Person) bool {
	return a.Name == b.Name && a.City == b.City && a.Age == b.Age
}

func TestPostIndex(t *testing.T) {
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
	if testing.Short() {
		t.Parallel()
	}

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

	r := db.Table("index_testing").Index("Age").Between(MinValue, MaxValue)

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

	r = db.Table("index_testing").Index("Age").Between(MinValue, MaxValue, true)

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

	r = db.Table("index_testing").Index("Age").Between(MinValue, MinValue)

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(MaxValue, MaxValue)

	_, _, err = r.Next(&person)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should have automatically closed, but hasn't")
	}

	r = db.Table("index_testing").Index("Age").Between(100, MaxValue)

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
	if testing.Short() {
		t.Parallel()
	}

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

	r := db.Table("index_testing").Index("Age").Between(MinValue, MaxValue)

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

	r = db.Table("index_testing").Index("Age").Between(MinValue, MaxValue, true)

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

func TestOrdering(t *testing.T) {
	if testing.Short() {
		t.Parallel()
	}

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

	if bytes.Compare(valueToBytes(12.34), valueToBytes(12.35)) >= 0 {
		t.Fatal("12.34 should come before 12.35, but doesn't")
	}

	if bytes.Compare(valueToBytes(12.35), valueToBytes(12.34)) <= 0 {
		t.Fatal("12.35 should come after 12.34, but doesn't")
	}

	if bytes.Compare(valueToBytes(12.34), valueToBytes(12.34)) != 0 {
		t.Fatal("float64 should obey reflexive property of equality, but doesn't")
	}

	if bytes.Compare(valueToBytes(float32(12.34)),
		valueToBytes(float32(12.35))) >= 0 {
		t.Fatal("12.34 should come before 12.35, but doesn't")
	}

	if bytes.Compare(valueToBytes(float32(12.35)),
		valueToBytes(float32(12.34))) <= 0 {
		t.Fatal("12.35 should come after 12.34, but doesn't")
	}

	if bytes.Compare(valueToBytes(float32(12.34)),
		valueToBytes(float32(12.34))) != 0 {
		t.Fatal("float64 should obey reflexive property of equality, but doesn't")
	}

	if bytes.Compare(valueToBytes(uint32(1)), valueToBytes(uint32(2))) >= 0 {
		t.Fatal("1 should be come before 2, but doesn't")
	}

	if bytes.Compare(valueToBytes(uint32(2)), valueToBytes(uint32(1))) <= 0 {
		t.Fatal("2 should be come after 1, but doesn't")
	}

	if bytes.Compare(valueToBytes(uint32(2)), valueToBytes(uint32(2))) != 0 {
		t.Fatal("number should obey reflexive property of equality, but doesn't")
	}

	if bytes.Compare(valueToBytes(uint16(1)), valueToBytes(uint16(2))) >= 0 {
		t.Fatal("1 should be come before 2, but doesn't")
	}

	if bytes.Compare(valueToBytes(uint16(2)), valueToBytes(uint16(1))) <= 0 {
		t.Fatal("2 should be come after 1, but doesn't")
	}

	if bytes.Compare(valueToBytes(uint16(2)), valueToBytes(uint16(2))) != 0 {
		t.Fatal("number should obey reflexive property of equality, but doesn't")
	}

	if bytes.Compare(valueToBytes(uint64(1)), valueToBytes(uint64(2))) >= 0 {
		t.Fatal("1 should be come before 2, but doesn't")
	}

	if bytes.Compare(valueToBytes(uint64(2)), valueToBytes(uint64(1))) <= 0 {
		t.Fatal("2 should be come after 1, but doesn't")
	}

	if bytes.Compare(valueToBytes(uint64(2)), valueToBytes(uint64(2))) != 0 {
		t.Fatal("number should obey reflexive property of equality, but doesn't")
	}

	if bytes.Compare(valueToBytes(1), valueToBytes(2)) >= 0 {
		t.Fatal("1 should be come before 2, but doesn't")
	}

	if bytes.Compare(valueToBytes(2), valueToBytes(1)) <= 0 {
		t.Fatal("2 should be come after 1, but doesn't")
	}

	if bytes.Compare(valueToBytes(-1), valueToBytes(1)) >= 0 {
		t.Fatal("-1 should be come before 1, but doesn't")
	}

	if bytes.Compare(valueToBytes(-1), valueToBytes(0)) >= 0 {
		t.Fatal("-1 should be come before 0, but doesn't")
	}

	if bytes.Compare(valueToBytes(0), valueToBytes(1)) >= 0 {
		t.Fatal("0 should be come before 1, but doesn't")
	}

	if bytes.Compare(valueToBytes(2), valueToBytes(2)) != 0 {
		t.Fatal("number should obey reflexive property of equality, but doesn't")
	}

	if bytes.Compare(valueToBytes(int16(1)), valueToBytes(int16(2))) >= 0 {
		t.Fatal("1 should be come before 2, but doesn't")
	}

	if bytes.Compare(valueToBytes(int16(2)), valueToBytes(int16(1))) <= 0 {
		t.Fatal("2 should be come after 1, but doesn't")
	}

	if bytes.Compare(valueToBytes(int16(-1)), valueToBytes(int16(1))) >= 0 {
		t.Fatal("-1 should be come before 1, but doesn't")
	}

	if bytes.Compare(valueToBytes(int16(-1)), valueToBytes(int16(0))) >= 0 {
		t.Fatal("-1 should be come before 0, but doesn't")
	}

	if bytes.Compare(valueToBytes(int16(0)), valueToBytes(int16(1))) >= 0 {
		t.Fatal("0 should be come before 1, but doesn't")
	}

	if bytes.Compare(valueToBytes(int16(2)), valueToBytes(int16(2))) != 0 {
		t.Fatal("number should obey reflexive property of equality, but doesn't")
	}

	if bytes.Compare(valueToBytes(int32(1)), valueToBytes(int32(2))) >= 0 {
		t.Fatal("1 should be come before 2, but doesn't")
	}

	if bytes.Compare(valueToBytes(int32(2)), valueToBytes(int32(1))) <= 0 {
		t.Fatal("2 should be come after 1, but doesn't")
	}

	if bytes.Compare(valueToBytes(int32(-1)), valueToBytes(int32(1))) >= 0 {
		t.Fatal("-1 should be come before 1, but doesn't")
	}

	if bytes.Compare(valueToBytes(int32(-1)), valueToBytes(int32(0))) >= 0 {
		t.Fatal("-1 should be come before 0, but doesn't")
	}

	if bytes.Compare(valueToBytes(int32(0)), valueToBytes(int32(1))) >= 0 {
		t.Fatal("0 should be come before 1, but doesn't")
	}

	if bytes.Compare(valueToBytes(int32(2)), valueToBytes(int32(2))) != 0 {
		t.Fatal("number should obey reflexive property of equality, but doesn't")
	}

	if bytes.Compare(valueToBytes(int64(1)), valueToBytes(int64(2))) >= 0 {
		t.Fatal("1 should be come before 2, but doesn't")
	}

	if bytes.Compare(valueToBytes(int64(2)), valueToBytes(int64(1))) <= 0 {
		t.Fatal("2 should be come after 1, but doesn't")
	}

	if bytes.Compare(valueToBytes(int64(-1)), valueToBytes(int64(1))) >= 0 {
		t.Fatal("-1 should be come before 1, but doesn't")
	}

	if bytes.Compare(valueToBytes(int64(-1)), valueToBytes(int64(0))) >= 0 {
		t.Fatal("-1 should be come before 0, but doesn't")
	}

	if bytes.Compare(valueToBytes(int64(0)), valueToBytes(int64(1))) >= 0 {
		t.Fatal("0 should be come before 1, but doesn't")
	}

	if bytes.Compare(valueToBytes(int64(2)), valueToBytes(int64(2))) != 0 {
		t.Fatal("number should obey reflexive property of equality, but doesn't")
	}

	minVal := valueToBytes(MinValue)
	if !(minVal[0] == 0 && minVal[1] == 0 && minVal[2] == 0 && minVal[3] == 0 &&
		minVal[4] == 0 && minVal[5] == 0 && minVal[6] == 0 && minVal[7] == 0) {
		t.Fatal("MinValue should be represented as 0, but isn't")
	}

	maxVal := valueToBytes(MaxValue)
	if !(maxVal[0] == 0xff && maxVal[1] == 0xff && maxVal[2] == 0xff && maxVal[3] == 0xff &&
		maxVal[4] == 0xff && maxVal[5] == 0xff && maxVal[6] == 0xff && maxVal[7] == 0xff) {
		t.Fatal("MaxValue should be represented as 0xffffffffffffffff, but isn't")
	}

	b := []byte{1, 2, 3, 4, 5, 6}
	if bytes.Compare(valueToBytes(b), append(b, 0)) != 0 {
		t.Fatal("bytes should be the same, but isn't")
	}
}

func testArrayCount(t *testing.T, count int) {
	stringsList := make([]string, count)
	data, err := msgpack.Marshal(stringsList)
	panicNotNil(err)
	result := decodeArrayCount(data)
	if result != int64(count) {
		t.Fatal("expected count of", count, "instead got", result)
	}
}

func TestArrayCounting(t *testing.T) {
	if testing.Short() {
		t.Parallel()
	}

	testArrayCount(t, 0)
	testArrayCount(t, 1)
	testArrayCount(t, 2)
	testArrayCount(t, 3)
	testArrayCount(t, 4)
	testArrayCount(t, 5)
	testArrayCount(t, 10)
	testArrayCount(t, 13)
	testArrayCount(t, 14)
	testArrayCount(t, 15)
	testArrayCount(t, 16)
	testArrayCount(t, 17)
	testArrayCount(t, 65534)
	testArrayCount(t, 65535)
	testArrayCount(t, 65536)
	testArrayCount(t, 65537)
	testArrayCount(t, 100000)
}

func TestIndexLoading(t *testing.T) {
	if testing.Short() {
		t.Parallel()
	}

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

	if db.Table("index_testing").Index("Age").name() != "index_testing/Age" {
		t.Fatal("index name should be index_testing/Age, but isn't")
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
	if testing.Short() {
		t.Parallel()
	}

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

func TestSmallIndex(t *testing.T) {
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

	defer db.Close()

	err = db.NewTable("index_testing")
	panicNotNil(err)

	panicNotNil(db.Table("index_testing").Set("a", Person{
		Name: "",
	}))

	panicNotNil(db.Table("index_testing").NewIndex("Name"))

	if db.Table("index_testing").Index("Name").CountBetween(MinValue, MaxValue) != 1 {
		t.Fatal("count should be 1, but isn't")
	}

	if decodeArrayCount([]byte{0}) != 0 {
		t.Fatal("count should be 0, but isn't")
	}
}

func TestIndexAll(t *testing.T) {
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

	defer db.Close()

	err = db.NewTable("index_testing")
	panicNotNil(err)

	panicNotNil(db.Table("index_testing").Set("a", map[string]string{
		"Name": "Jason",
	}))

	panicNotNil(db.Table("index_testing").Set("b", map[string]string{
		"Name": "Alex",
	}))

	panicNotNil(db.Table("index_testing").Set("c", map[string]string{
		"NotName": "Bob",
	}))

	panicNotNil(db.Table("index_testing").NewIndex("Name"))

	r := db.Table("index_testing").Index("Name").All()

	type pp struct {
		Name    string
		NotName string
	}

	var result pp
	key, _, err := r.Next(&result)
	panicNotNil(err)

	if key != "b" || result.Name != "Alex" || result.NotName != "" {
		t.Fatal("result should be Alex, but isn't")
	}

	key, _, err = r.Next(&result)
	panicNotNil(err)

	if key != "a" || result.Name != "Jason" || result.NotName != "" {
		t.Fatal("result should be Jason, but isn't")
	}

	_, _, err = r.Next(&result)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}
}
