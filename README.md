# Cete

[![Go Report Card](https://goreportcard.com/badge/github.com/1lann/cete)](https://goreportcard.com/report/github.com/1lann/cete)
[![GoDoc](https://godoc.org/github.com/1lann/cete?status.svg)](https://godoc.org/github.com/1lann/cete)
[![codecov](https://codecov.io/gh/1lann/cete/branch/master/graph/badge.svg)](https://codecov.io/gh/1lann/cete)
[![Travis CI](https://travis-ci.org/1lann/cete.svg?branch=master)](https://travis-ci.org/1lann/cete)

>**Cete**
>_noun_
>
>A group of badgers.

Cete is a simple, lightweight, pure Go database abstraction layer of [Badger](https://github.com/dgraph-io/badger) for use in your Go programs. Unlike most other embedded database toolkits for Go, Cete is schemaless, yet still blazing fast. It's great for cases where you need a fast, on-disk, embedded database. Cete is licensed under the [MIT License](/LICENSE).

**Cete is currently in alpha, breaking changes may be released.**

Here's a short example to show how easy it is to use the database:

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
	db, _ := cete.Open("./cete_data")

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
- Compound indexes.
- Multi-indexes (tags).
- Transparent key compression.
- All range queries are sorted (ascending by default).
- Uses a [custom version](https://github.com/1lann/msgpack) of [MessagePack](https://github.com/vmihailenco/msgpack) as underlying storage structure.
- Efficient, concurrent range retrievers, filters, and index generation.
- Supports filtering.
- Lockless read/writes. Achieve safe updates with `Update` and counters.
- Schemaless!
- Thread safe.
- Pure Go.
- Uses the fastest pure Go key-value store in the world 😉.

## Important limitations

- When indexed, strings are case unsensitized using `strings.ToLower`. If you don't want this behavior, use a byte slice instead.
- Indexing with numbers above maximum int64 is unsupported and will result in undefined behavior when using `Between`. Note that it's fine to index uint64, just values over max int64 (9,223,372,036,854,775,807) will result in issues when using `Between`.
- If your documents' keys have any of the following characters: `.,*`, `Query` will not work on them. Use `Decode` instead.
- When working with compound indexes, you may use `MaxValue` and `MinValue` as maximum integers or minimum integers of any size and float64s. This however cannot be be used for float32.

## Documentation and examples

Find documentation on [GoDoc](https://godoc.org/github.com/1lann/cete).

Examples can be found on the [wiki](https://github.com/1lann/cete/wiki).

## Todo

- [ ] Concurrent indexing to speed up index creation
- [ ] Write more examples

## Performance

I've performed some benchmarks comparing Cete to two other pure Go database wrappers, [Storm](https://github.com/asdine/storm) and [BoltHold](https://github.com/timshannon/bolthold). The source code for this benchmark can be found [here](https://github.com/1lann/db-benchmark).

For this test, Storm was running in batched write modes.

These benchmarks consists of simple sets and gets. However the gets were by secondary index instead of primary index, as is in a lot of real-world cases. If it were all by primary index, it will be more of a performance indicator of the underlying key-value store.

![Cete benchmarks](https://chuie.io/cete.png)

Cete is typically twice as fast as Storm for concurrent operations, and BoltHold was magnitudes slower than either. Cete is actually quite slow when it comes to sequential write operations (and isn't shown here), so it's strongly recommended to write concurrently. Cete also fairs similarly to Storm with sequential reads.

## FAQ
### What happens if a document is missing the attribute for an index?
The index is skipped for that document! The document won't ever appear in the index. This also applies to compound indexes, if any of the queries for the compound index fails/results in nil, the document won't be indexed for that compound index.

### Are there transactions?
No, this library is meant to be a very simple and basic abstraction layer of Badger, and Badger does not support transactions.

For single document updates (such as incrementing a value), you can use the `Update` method which constantly re-attempts the update until the counter matches, eradicating race conditions. Alternatively you can use the counter yourself and implement the logic to handle unmatched counters.

For more complex transactions, you'll need to implement your own solution. Although typically if you need more complex transactions you would be willing to sacrifice performance for an ACID compliant database. That being said if you need ACID compliance, I recommend you to use one of the great BoltDB wrappers that are available, such as [Storm](https://github.com/asdine/storm).

If you're desperate to use transactions with Cete, you can implement your own 2 phase commits.

On the upside, because there is no support for transactions, all read/writes are lockless, making it super fast!
