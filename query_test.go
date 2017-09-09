package cete

import (
	"testing"
	"time"

	"github.com/1lann/msgpack"
)

func TestQuery(t *testing.T) {
	if testing.Short() {
		t.Parallel()
	}

	now := time.Now().Round(0) // strip off monatonic time
	jason := Person{
		Name:   "Jason",
		City:   "Sydney",
		Age:    18,
		Height: 172.8,
		Likes:  []string{"go", "js"},
		DOB:    now,
		Data:   []byte("hello"),
	}

	data, err := msgpack.Marshal(jason)

	panicNotNil(err)

	doc := Document{
		data:  data,
		table: nil,
	}

	if string(doc.QueryBytes("Data")) != "hello" {
		t.Fatal("query should be hello, but isn't")
	}

	if string(doc.QueryBytes("Age")) != "" {
		t.Fatal("query should be empty, but isn't")
	}

	if string(doc.QueryBytes("Nothing")) != "" {
		t.Fatal("query should be empty, but isn't")
	}

	if doc.QueryFloat64("Height") != 172.8 {
		t.Fatal("query should be 172.8, but isn't")
	}

	if doc.QueryFloat64("Age") != 0 {
		t.Fatal("query should be 0, but isn't")
	}

	if doc.QueryInt("Age") != 18 {
		t.Fatal("query should be 18, but isn't")
	}

	if doc.QueryInt("Name") != 0 {
		t.Fatal("query should be 0, but isn't")
	}

	if doc.QueryInt64("Age") != 18 {
		t.Fatal("query should be 18, but isn't")
	}

	if doc.QueryInt64("Name") != 0 {
		t.Fatal("query should be 0, but isn't")
	}

	if doc.QueryString("Name") != "Jason" {
		t.Fatal("query should be Jason, but isn't")
	}

	if doc.QueryString("Age") != "" {
		t.Fatal("query should be empty, but isn't")
	}

	if doc.QueryTime("DOB") != now {
		t.Fatal("query should be now, but isn't")
	}

	if !doc.QueryTime("Data").IsZero() {
		t.Fatal("query should be zero, but isn't")
	}

	var person Person
	panicNotNil(doc.Decode(&person))

	if !person.IsSame(jason) {
		t.Fatal("person should be Jason, but isn't")
	}

	if doc.QueryAll("Name")[0] != "Jason" {
		t.Fatal("query should be Jason, but isn't")
	}

	if doc.QueryAll("Nothing") != nil {
		t.Fatal("query should be nil, but isn't")
	}
}
