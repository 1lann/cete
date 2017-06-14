# Cete

[![Go Report Card](https://goreportcard.com/badge/github.com/1lann/cete)](https://goreportcard.com/report/github.com/1lann/cete)
[![GoDoc](https://godoc.org/github.com/1lann/cete?status.svg)](https://godoc.org/github.com/1lann/cete)
[![codecov](https://codecov.io/gh/1lann/cete/branch/master/graph/badge.svg)](https://codecov.io/gh/1lann/cete)
[![Travis CI](https://travis-ci.org/1lann/cete.svg?branch=master)](https://travis-ci.org/1lann/cete)

>**Cete**
>_noun_
>
>A group of badgers.

Cete is a simple, lightweight abstraction layer of [Badger](https://github.com/dgraph-io/badger) for  use in your Go programs. It's great for cases where you need a fast, on-disk, embedded database. Cete is licensed under the [MIT License](/LICENSE).

**This is just a personal hobby project, I may not maintain this! I just wanted to make my own database for fun.**

Here's a short example to show how easy to use the database is:

```go
package main

import (
	"github.com/1lann/cete"
	"fmt"
)

type Person struct {
	Name string
	Age int
}

func main() {
	db, _ := cete.OpenDatabase("./cete_data")

	defer db.Close()

	db.NewTable("people")
	db.Table("people").Set("ash", Person{
		Name: "Ash Ketchum",
		Age: 10,
	})

	db.Table("people").NewIndex("Age")

	var result Person
	db.Table("people").Index("Age").One(10, &result)
	fmt.Printf("People who are 10: %+v\n", result)

	// Or if you just want simple key-value usage
	db.Table("people").Get("ash", &result)
	fmt.Printf("This is Ash: %+v\n", result)
}
```

## Features

- Indexes.
- All range queries are sorted (ascending by default).
- Uses [MessagePack](https://github.com/vmihailenco/msgpack) as underlying storage structure.
- All range queries are buffered in the background, 100 results at a time.
- Supports filtering.
- Lockless read/writes. Achieve safe updates with `Update` and counters.
- Schemaless!
- Thread safe.
- Pure Go.
- Uses the fastest pure Go key-value store in the world 😉.

## Documentation

Find documentation on [GoDoc](https://godoc.org/github.com/1lann/cete).

## Examples

The following examples don't handle errors for the sake of example. It is strongly recommended to handle errors as the library will not print out when errors occur (unless it detects a corrupt index).

### Filtering

Here's an example of filtering:

```go
package main

import (
	"github.com/1lann/cete"
	"fmt"
)

type Person struct {
	Name string
	Age int
}

func main() {
	db, _ := cete.OpenDatabase("./cete_data")

	defer db.Close()

	db.NewTable("people")
	db.Table("people").Set("ash", Person{
		Name: "Ash Ketchum",
		Age: 10,
	})
	db.Table("people").Set("brock", Person{
		Name: "Brock",
		Age: 15,
	})

	// Find who is no younger than 13
	r := db.Table("people").All().Filter(func(doc cete.Document) bool {
		// Doesn't decode the entire document, makes filtering faster!
		return doc.QueryInt("Age") >= 13
	})

	defer r.Close()

	var person Person
	r.Next(&person)
	fmt.Printf("%+v\n", person) // Should print Brock's information
}
```

### Between on an index

The same example as above, but faster with indexes:

```go
package main

import (
	"github.com/1lann/cete"
	"fmt"
)

type Person struct {
	Name string
	Age int
}

func main() {
	db, _ := cete.OpenDatabase("./cete_data")

	defer db.Close()

	db.NewTable("people")
	db.Table("people").Set("ash", Person{
		Name: "Ash Ketchum",
		Age: 10,
	})
	db.Table("people").Set("brock", Person{
		Name: "Brock",
		Age: 15,
	})

	db.Table("people").NewIndex("Age")

	// Find who is no younger than 13. This would also be sorted ascending by age.
	r := db.Table("people").Index("Age").Between(13, cete.MaxBounds)

	defer r.Close()

	var person Person
	r.Next(&person)
	fmt.Printf("%+v\n", person) // Should print Brock's information
}
```

## FAQ
### What happens if a document is missing the attribute for an index?
The index is skipped for that document! The document won't ever appear in the index.

### Are there transactions?
No, this library is meant to be a very simple and basic abstraction layer of Badger, and Badger does not support transactions.

For single document updates (such as incrementing a value), you can use the `Update` method which constantly re-attempts the update until the counter matches, eradicating race conditions. Alternatively you can use the counter yourself and implement the logic to handle unmatched counters.

For more complex transactions, you'll need to implement your own solution. Although typically if you need more complex transactions you would be willing to sacrifice performance for an ACID compliant database.

If you're desperate to use transactions with Cete, you can implement your own 2 phase commits.

On the upside, because there is no support for transactions, all read/writes are lockless, making it super fast!
