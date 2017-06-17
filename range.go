package cete

import (
	"sync/atomic"

	"gopkg.in/vmihailenco/msgpack.v2"
)

const bufferSize = 100

type bufferEntry struct {
	key     string
	data    []byte
	counter int
	err     error
}

// Range represents a result with multiple values in it and is usually sorted
// by index/key.
type Range struct {
	buffer chan bufferEntry
	next   func() (string, []byte, int, error)
	close  func()
	closed int32
}

// Next stores the next item in the range into dst. dst must be a pointer
// to a value, or nil. If dst is nil then the value will be discarded, but
// the counter and key will still be returned.
func (r *Range) Next(dst interface{}) (string, int, error) {
	entry, more := <-r.buffer
	if !more {
		return "", 0, ErrEndOfRange
	}

	if entry.err != nil {
		return entry.key, entry.counter, entry.err
	}

	if dst != nil {
		return entry.key, entry.counter, msgpack.Unmarshal(entry.data, dst)
	}

	return entry.key, entry.counter, nil
}

// Close closes the range. The range will automatically close upon the
// first encountered error.
func (r *Range) Close() {
	if atomic.CompareAndSwapInt32(&r.closed, 0, 1) {
		r.close()
	}
}

func newRange(next func() (string, []byte, int, error), closer func()) *Range {
	r := &Range{
		buffer: make(chan bufferEntry, bufferSize),
		next:   next,
		close:  closer,
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
		go filterWorker(filter, inboxes[i], outboxes[i])
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

	return newRange(func() (string, []byte, int, error) {
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
	}, r.Close)
}

func filterWorker(filter func(doc Document) (bool, error),
	inbox chan *bufferEntry, outbox chan *bufferEntry) {
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

		ok, err = filter(Document(entry.data))
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

// Skip skips a number of values from the range.
// The first encountered error while skipping will be returned.
func (r *Range) Skip(n int) error {
	var entry bufferEntry
	for i := 0; i < n; i++ {
		entry = <-r.buffer
		if entry.err != nil {
			return entry.err
		}
	}

	return nil
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

	return newRange(func() (string, []byte, int, error) {
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
	}, r.Close)
}
