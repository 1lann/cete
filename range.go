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
// filter returns false.
func (r *Range) Filter(filter func(doc Document) bool) *Range {
	var entry bufferEntry

	return newRange(func() (string, []byte, int, error) {
		for {
			entry = <-r.buffer

			if entry.err != nil {
				return entry.key, entry.data, entry.counter, entry.err
			}

			if filter(Document(entry.data)) {
				return entry.key, entry.data, entry.counter, entry.err
			}
		}
	}, r.Close)
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
