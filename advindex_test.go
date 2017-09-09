package cete

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestCompoundIndex(t *testing.T) {
	if testing.Short() {
		t.Parallel()
	}

	testCompoundIndex(t, false)
}

func TestCompoundIndexCompressed(t *testing.T) {
	if testing.Short() {
		t.Parallel()
	}

	testCompoundIndex(t, true)
}

func TestMultiIndex(t *testing.T) {
	if testing.Short() {
		t.Parallel()
	}

	testMultiIndex(t, false)
}

func TestMultiIndexCompressed(t *testing.T) {
	if testing.Short() {
		t.Parallel()
	}

	testMultiIndex(t, true)
}

func testCompoundIndex(t *testing.T, compression bool) {
	people := map[string]Person{
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
		"jason": {
			Name: "Jason",
			City: "Sydney",
			Age:  18,
		},
		"matheus": {
			Name: "Matheus",
			City: "Rio",
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

	panicNotNil(db.NewTable("index_testing", compression))

	panicNotNil(db.Table("index_testing").NewIndex("Age,Name"))
	panicNotNil(db.Table("index_testing").NewIndex("Name,Age"))

	for name, person := range people {
		err = db.Table("index_testing").Set(name, person)
		panicNotNil(err)
	}

	r := db.Table("index_testing").Index("Age,Name").Between(18, 19)

	expectPerson("drew", r, people["drew"])
	expectPerson("jason", r, people["jason"])
	expectPerson("matheus", r, people["matheus"])

	if r.Next() || r.Error() != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("index_testing").Index("Age,Name").Between(18, 20)

	expectPerson("drew", r, people["drew"])
	expectPerson("jason", r, people["jason"])
	expectPerson("matheus", r, people["matheus"])
	expectPerson("ben", r, people["ben"])

	if r.Next() || r.Error() != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should be closed, but isn't")
	}

	r = db.Table("index_testing").Index("Age,Name").Between(
		[]interface{}{18, "da"}, []interface{}{18, "ko"})
	expectPerson("drew", r, people["drew"])
	expectPerson("jason", r, people["jason"])

	if r.Next() || r.Error() != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("index_testing").Index("Name,Age").All()
	expectPerson("ben", r, people["ben"])
	expectPerson("drew", r, people["drew"])
	expectPerson("jason", r, people["jason"])
	expectPerson("matheus", r, people["matheus"])

	if r.Next() || r.Error() != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("index_testing").Index("Name,Age").Between("b", "e")
	expectPerson("ben", r, people["ben"])
	expectPerson("drew", r, people["drew"])

	if r.Next() || r.Error() != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}
}

func testMultiIndex(t *testing.T, compression bool) {
	people := map[string]Person{
		"ben": {
			Name:  "Ben",
			City:  "Melbourne",
			Age:   19,
			Likes: []string{"c", "go", "rust"},
		},
		"drew": {
			Name:  "Drew",
			City:  "London",
			Age:   18,
			Likes: []string{"js", "java"},
		},
		"jason": {
			Name:  "Jason",
			City:  "Sydney",
			Age:   18,
			Likes: []string{"go", "js"},
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

	panicNotNil(db.NewTable("index_testing", compression))

	panicNotNil(db.Table("index_testing").NewIndex("Likes.*"))

	panicNotNil(db.Table("index_testing").Set("ben", people["ben"]))
	panicNotNil(db.Table("index_testing").Set("drew", people["drew"]))
	panicNotNil(db.Table("index_testing").Set("jason", people["jason"]))

	r := db.Table("index_testing").Index("Likes.*").GetAll("java")

	expectPerson("drew", r, people["drew"])

	if r.Next() || r.Error() != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("index_testing").Index("Likes.*").GetAll("go")

	expectPerson("ben", r, people["ben"])
	expectPerson("jason", r, people["jason"])

	if r.Next() || r.Error() != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("index_testing").Index("Likes.*").GetAll("js")

	expectPerson("drew", r, people["drew"])
	expectPerson("jason", r, people["jason"])

	if r.Next() || r.Error() != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if r.closed != 1 {
		t.Fatal("range should be closed, but isn't")
	}

	r = db.Table("index_testing").Index("Likes.*").All()
	expectPerson("ben", r, people["ben"])
	expectPerson("ben", r, people["ben"])
	expectPerson("jason", r, people["jason"])
	expectPerson("drew", r, people["drew"])
	expectPerson("drew", r, people["drew"])
	expectPerson("jason", r, people["jason"])
	expectPerson("ben", r, people["ben"])

	if r.Next() || r.Error() != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	r = db.Table("index_testing").Index("Likes.*").All().Unique()
	expectPerson("ben", r, people["ben"])
	expectPerson("jason", r, people["jason"])
	expectPerson("drew", r, people["drew"])

	if r.Next() || r.Error() != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}
}
