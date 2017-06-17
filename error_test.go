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

	db.NewTable("error_testing")
	for name, person := range people {
		err = db.Table("error_testing").Set(name, person)
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

func TestBadFiles(t *testing.T) {
	if testing.Short() {
		t.Parallel()
	}

	file, err := ioutil.TempFile("", "cete_")
	fileName := file.Name()
	file.Close()

	panicNotNil(err)

	_, err = Open(fileName)
	if err == nil {
		t.Fatal("error should not be nil, but is")
	}

	panicNotNil(os.Remove(fileName))

	dir, err := ioutil.TempDir("", "cete_")
	panicNotNil(err)

	db, err := Open(dir + "/data")
	panicNotNil(err)

	panicNotNil(db.NewTable("error_testing"))
	db.Close()

	panicNotNil(os.Remove(dir + "/data/config.dat"))

	_, err = Open(dir + "/data")
	if err == nil {
		t.Fatal("error should not be nil, but is")
	}

	panicNotNil(os.RemoveAll(dir))

	dir, err = ioutil.TempDir("", "cete_")
	panicNotNil(err)

	db, err = Open(dir + "/data")
	panicNotNil(err)
	panicNotNil(db.NewTable("error_testing"))
	panicNotNil(db.Table("error_testing").NewIndex("test"))
	db.Close()

	t.Log("using:", dir)

	panicNotNil(os.Chmod(dir+"/data/6572726f725f74657374696e67/74657374", 0000))

	_, err = Open(dir + "/data")
	if err == nil {
		t.Fatal("error should not be nil, but is")
	}

	panicNotNil(os.Chmod(dir+"/data/6572726f725f74657374696e67/74657374", 0777))

	panicNotNil(os.Chmod(dir+"/data/6572726f725f74657374696e67", 0000))

	_, err = Open(dir + "/data")
	if err == nil {
		t.Fatal("error should not be nil, but is")
	}

	panicNotNil(os.Chmod(dir+"/data/6572726f725f74657374696e67", 0777))

	db, err = Open(dir + "/data")
	panicNotNil(err)

	db.Close()

	panicNotNil(os.Rename(dir+"/data/6572726f725f74657374696e67/data",
		dir+"/data/6572726f725f74657374696e67/old"))
	file, err = os.Create(dir + "/data/6572726f725f74657374696e67/data")
	panicNotNil(err)

	file.Write([]byte("junk"))
	file.Close()

	_, err = Open(dir + "/data")
	if err == nil {
		t.Fatal("error should not be nil, but is")
	}

	panicNotNil(os.Remove(dir + "/data/6572726f725f74657374696e67/data"))
	panicNotNil(os.Rename(dir+"/data/6572726f725f74657374696e67/old",
		dir+"/data/6572726f725f74657374696e67/data"))

	db, err = Open(dir + "/data")
	panicNotNil(err)

	db.Close()

	file, err = os.Create(dir + "/data/config.dat")
	panicNotNil(err)
	file.Write([]byte{0x87})
	file.Close()

	_, err = Open(dir + "/data")
	if err == nil {
		t.Fatal("error should not be nil, but is")
	}

	panicNotNil(os.RemoveAll(dir))
}
