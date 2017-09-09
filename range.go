package cete

import (
	"errors"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/1lann/msgpack"
)

const bufferSize = 100

type bufferEntry struct {
	key     string
	data    []byte
	counter uint64
	err     error
}

// Range represents a result with multiple values in it and is usually sorted
// by index/key.
type Range struct {
	buffer chan bufferEntry
	next   func() (string, []byte, uint64, error)
	close  func()
	closed int32

	lastEntry bufferEntry

	table *Table
}

// Next retrieves the next item in the range, and returns true if the
// next item is successfully retrieved.
func (r *Range) Next() bool {
	if r.lastEntry.err != nil {
		return false
	}

	entry, more := <-r.buffer
	if !more {
		r.lastEntry.err = ErrEndOfRange
		return false
	}

	r.lastEntry = entry

	if entry.err != nil {
		return false
	}

	return true
}

// Document returns the current item's Document representation.
func (r *Range) Document() Document {
	return Document{
		data:  r.lastEntry.data,
		table: r.table,
	}
}

// Decode decodes the current item into a pointer.
func (r *Range) Decode(dst interface{}) error {
	if r.table != nil && r.table.keyToCompressed != nil {
		return msgpack.UnmarshalCompressed(r.table.cToKey, r.lastEntry.data,
			dst)
	}

	return msgpack.Unmarshal(r.lastEntry.data, dst)
}

// Counter returns the counter of the current item.
func (r *Range) Counter() uint64 {
	return r.lastEntry.counter
}

// Key returns the key of the current item.
func (r *Range) Key() string {
	return r.lastEntry.key
}

// Error returns the last error causing Next to return false. It will be nil
// if Next returned true.
func (r *Range) Error() error {
	return r.lastEntry.err
}

// All stores all of the results into slice dst provided by as a pointer.
// A nil error will be returned if the range reaches ErrEndOfRange.
func (r *Range) All(dst interface{}) error {
	// Code baseed on github.com/GoRethink/gorethink's `Cursor.All`.
	slicePtr := reflect.ValueOf(dst)
	if slicePtr.Kind() != reflect.Ptr ||
		slicePtr.Elem().Kind() != reflect.Slice {
		return errors.New("cete: dst must be a pointer to a silce")
	}

	sliceValue := slicePtr.Elem()
	sliceValue = sliceValue.Slice(0, sliceValue.Cap())
	elemType := sliceValue.Type().Elem()
	i := 0
	compressed := r.table != nil && r.table.keyToCompressed != nil

	defer func() {
		slicePtr.Elem().Set(sliceValue.Slice(0, i))
	}()

	var err error
	for {
		entry, more := <-r.buffer
		if !more {
			return nil
		}

		if entry.err == ErrEndOfRange {
			return nil
		} else if entry.err != nil {
			return entry.err
		}

		if sliceValue.Len() == i {
			thisElem := reflect.New(elemType)

			if compressed {
				err = msgpack.UnmarshalCompressed(r.table.cToKey, entry.data,
					thisElem.Interface())
			} else {
				err = msgpack.Unmarshal(entry.data, thisElem.Interface())
			}
			if err != nil {
				return err
			}

			sliceValue = reflect.Append(sliceValue, thisElem.Elem())
			sliceValue = sliceValue.Slice(0, sliceValue.Cap())
		} else {
			if compressed {
				err = msgpack.UnmarshalCompressed(r.table.cToKey, entry.data,
					sliceValue.Index(i).Addr().Interface())
			} else {
				err = msgpack.Unmarshal(entry.data,
					sliceValue.Index(i).Addr().Interface())
			}
			if err != nil {
				return err
			}
		}
		i++
	}
}

// Limit limits the number of documents that can be read from the range.
// When this limit is reached, ErrEndOfRange will be returned.
func (r *Range) Limit(n int64) *Range {
	return newRange(func() (string, []byte, uint64, error) {
		entry := <-r.buffer

		if n <= 0 {
			return "", nil, 0, ErrEndOfRange
		}
		n--

		return entry.key, entry.data, entry.counter, entry.err
	}, r.Close, r.table)
}

// Close closes the range. The range will automatically close upon the
// first encountered error.
func (r *Range) Close() {
	if atomic.CompareAndSwapInt32(&r.closed, 0, 1) {
		r.close()
	}
}

func newRange(next func() (string, []byte, uint64, error), closer func(),
	table *Table) *Range {
	r := &Range{
		buffer: make(chan bufferEntry, bufferSize),
		next:   next,
		close:  closer,
		table:  table,
	}

	go func() {
		for {
			key, data, counter, err := r.next()
			// r.Close before sending to channel to prevent race condition
			if err != nil {
				r.Close()
			}
			r.buffer <- bufferEntry{key, data, counter, err}
			if err != nil {
				close(r.buffer)
				return
			}
		}
	}()

	return r
}

// Filter applies a filter onto the range, skipping values where the provided
// filter returns false. If the filter returns a non-nil error, the range
// will be stopped, and the error will be returned.
//
// You can optionally specify the number of workers
// to concurrently operate the filter to speed up long running filter queries.
// Note that you will still be limited by the read speed, and having too many
// workers will increase concurrency overhead. The default number of workers
// is 5.
func (r *Range) Filter(filter func(doc Document) (bool, error),
	workers ...int) *Range {

	numWorkers := 5
	if len(workers) > 0 && workers[0] != 0 {
		numWorkers = workers[0]
	}

	inboxes := make([]chan *bufferEntry, numWorkers)
	outboxes := make([]chan *bufferEntry, numWorkers)
	for i := range inboxes {
		inboxes[i] = make(chan *bufferEntry)
		outboxes[i] = make(chan *bufferEntry)
		go filterWorker(filter, r.table, inboxes[i], outboxes[i])
	}

	go func() {
		sendToWorker := 0

		for {
			entry, more := <-r.buffer
			if !more {
				break
			}

			inboxes[sendToWorker] <- &entry
			sendToWorker = (sendToWorker + 1) % numWorkers
		}

		for _, inbox := range inboxes {
			close(inbox)
		}
	}()

	readFromWorker := 0
	var entry *bufferEntry

	return newRange(func() (string, []byte, uint64, error) {
		for {
			entry = <-outboxes[readFromWorker]
			readFromWorker = (readFromWorker + 1) % numWorkers
			if entry.key == "" && entry.err == nil {
				continue
			}

			if entry.err != nil {
				r.Close()
			}

			return entry.key, entry.data, entry.counter, entry.err
		}
	}, r.Close, r.table)
}

func filterWorker(filter func(doc Document) (bool, error),
	table *Table, inbox chan *bufferEntry, outbox chan *bufferEntry) {
	var entry *bufferEntry
	var ok bool
	var err error
	var more bool

	for {
		entry, more = <-inbox
		if !more {
			return
		}

		if entry.err != nil {
			outbox <- entry
			continue
		}

		ok, err = filter(Document{
			data:  entry.data,
			table: table,
		})
		if err != nil {
			entry.err = err
			outbox <- entry
			continue
		}

		if !ok {
			entry.key = ""
		}

		outbox <- entry
	}
}

// Do applies a operation onto the range concurrently. Order is not guaranteed.
// If the operation returns an error, Do will stop and return the error.
// An error with the operation may not stop Do immediately, as the range buffer
// must be exhausted first.
// Errors during the range will also be returned. A nil error will be returned
// if ErrEndOfRange is reached.
//
// You can optionally specify the number of workers to concurrently operate
// on. By default the number of workers is 10.
func (r *Range) Do(operation func(key string, counter uint64, doc Document) error,
	workers ...int) error {

	numWorkers := 10
	if len(workers) > 0 && workers[0] != 0 {
		numWorkers = workers[0]
	}

	wg := new(sync.WaitGroup)
	wg.Add(numWorkers)

	completion := make(chan error, numWorkers)
	inboxes := make([]chan *bufferEntry, numWorkers)
	for i := range inboxes {
		inboxes[i] = make(chan *bufferEntry)
		go doWorker(wg, operation, r.table, inboxes[i], completion)
	}

	go func() {
		sendToWorker := 0

		for {
			entry, more := <-r.buffer
			if !more {
				break
			}

			inboxes[sendToWorker] <- &entry
			sendToWorker = (sendToWorker + 1) % numWorkers
		}

		for _, inbox := range inboxes {
			close(inbox)
		}
	}()

	result := <-completion

	if result == nil {
		r.Close()
		wg.Wait()

		// Find errors
		for {
			select {
			case err := <-completion:
				if err != nil {
					return err
				}
			default:
				return nil
			}
		}
	}

	r.Close()
	wg.Wait()
	return result
}

func doWorker(wg *sync.WaitGroup, operation func(key string, counter uint64,
	doc Document) error, table *Table, inbox chan *bufferEntry,
	completion chan error) {
	var entry *bufferEntry
	var err error
	var more bool

	defer wg.Done()

	for {
		entry, more = <-inbox
		if !more {
			return
		}

		if entry.err == ErrEndOfRange {
			completion <- nil
			return
		} else if entry.err != nil {
			completion <- entry.err
			return
		}

		err = operation(entry.key, entry.counter, Document{
			data:  entry.data,
			table: table,
		})
		if err != nil {
			completion <- err

			return
		}
	}
}

// Skip skips a number of values from the range, and returns back
// the range. Any errors during skip will result in a range that
// only returns that error.
func (r *Range) Skip(n int) *Range {
	var entry bufferEntry
	for i := 0; i < n; i++ {
		entry = <-r.buffer
		if entry.err != nil {
			return newRange(func() (string, []byte, uint64, error) {
				return "", nil, 0, entry.err
			}, func() {}, nil)
		}
	}

	return r
}

// Count will count the number of elements in the range and consume the values
// in the range. If it reaches the end of the range, it will return the count
// with a nil error. If a non-nil error is encountered, it returns the
// current count and the error.
func (r *Range) Count() (int64, error) {
	var count int64
	var entry bufferEntry

	for {
		entry = <-r.buffer
		if entry.err != nil {
			if entry.err == ErrEndOfRange {
				return count, nil
			}

			return count, entry.err
		}

		count++
	}
}

// Unique will remove all duplicate entries from the range. It does this by
// saving all of the seen keys to a map. If there are a lot of unique keys,
// Unique may use a lot of memory.
func (r *Range) Unique() *Range {
	var entry bufferEntry
	seen := make(map[string]bool)

	return newRange(func() (string, []byte, uint64, error) {
		for {
			entry = <-r.buffer

			if entry.err != nil {
				return entry.key, entry.data, entry.counter, entry.err
			}

			if !seen[entry.key] {
				seen[entry.key] = true
				return entry.key, entry.data, entry.counter, entry.err
			}
		}
	}, r.Close, r.table)
}
