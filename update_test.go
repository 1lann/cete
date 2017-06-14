package cete

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestUpdate(t *testing.T) {
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
	if err != nil {
		t.Error(err)
	}

	t.Log("testing directory:", dir)
	defer func() {
		if !t.Failed() {
			os.RemoveAll(dir)
		}
	}()

	db, err := Open(dir + "/data")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		db.Close()
	}()

	err = db.NewTable("update_testing")
	if err != nil {
		t.Fatal(err)
	}

	for name, person := range people {
		err = db.Table("update_testing").Set(name, person)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = db.Table("update_testing").NewIndex("Age")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Table("update_testing").Update("someoneelse", func(jason Person) (Person, error) {
		jason.Age++
		return jason, nil
	})
	if err != ErrNotFound {
		t.Fatal("error should be ErrNotFound, but isn't")
	}

	err = db.Table("update_testing").Update("jason", func(jason Person) (Person, error) {
		jason.Age++
		return jason, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	var person Person
	_, err = db.Table("update_testing").Get("jason", &person)
	if err != nil {
		t.Fatal(err)
	}

	newJason := people["jason"]
	newJason.Age = 18

	if !person.IsSame(newJason) {
		t.Fatal("jason's age should have changed, but it hasn't")
	}

	var a Person
	var b Person

	r, err := db.Table("update_testing").Index("Age").GetAll(18)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		r.Close()
	}()

	_, _, err = r.Next(&a)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = r.Next(&b)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = r.Next(&b)
	if err != ErrEndOfRange {
		t.Fatal("error should be ErrEndOfRange, but isn't")
	}

	if !((a.IsSame(newJason) || a.IsSame(people["drew"])) &&
		(b.IsSame(newJason) || b.IsSame(people["drew"]))) {
		t.Fatal("a and b should be newJason or drew, but aren't")
	}

	if a.IsSame(b) {
		t.Fatal("a and be should not be the same, but are")
	}

}
