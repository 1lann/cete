package cete

import (
	"io/ioutil"
	"os"
	"testing"
)

func populateDB(people map[string]Person) (*DB, string) {
	dir, err := ioutil.TempDir("", "cete_")
	panicNotNil(err)

	db, err := Open(dir + "/data")
	panicNotNil(err)

	db.NewTable("filter_testing")
	for name, person := range people {
		err = db.Table("filter_testing").Set(name, person)
		panicNotNil(err)
	}

	return db, dir
}

func TestError(t *testing.T) {
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

	db, dir := populateDB(people)

	t.Log("testing directory:", dir)

	panicNotNil(os.Chmod(dir, 0000))

	err := db.NewTable("should fail")
	if err == nil {
		t.Fatal("new table should have an error, but doesn't")
	}

	db.Close()

	db, err = Open(dir)
	if err == nil {
		if err == nil {
			t.Fatal("open should have an error, but doesn't")
		}
	}

	panicNotNil(os.Chmod(dir, 0777))
	panicNotNil(os.RemoveAll(dir))
}
