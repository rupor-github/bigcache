package bigcache

import (
	"bytes"
	"errors"
	"time"
)

var (
	ErrQueueEmpty        = errors.New("byte queue is empty")
	ErrQueueFull         = errors.New("byte queue is full, size limit is reached")
	ErrQueueInvalidIndex = errors.New("byte queue index is out of bounds (0 <= index < right)")
	ErrQueueEntryTooBig  = errors.New("byte queue cannot expand, entry is too big")
)

// bytesQueue is unprotected ring buffer implementing type of FIFO queue.
// It is based on bytes array - for every push operation index of entry (qref) is returned.
// It can be used to read the entry later.
type bytesQueue struct {
	count       int
	maxCapacity int
	array       []byte
	head        qref
	tail        qref
	right       qref
	logger      Logger
}

// newBytesQueue initialize new queue.
// Initial capacity is used in bytes array allocation.
func newBytesQueue(initialCapacity, maxCapacity int, logger Logger) *bytesQueue {
	return &bytesQueue{
		array:       make([]byte, initialCapacity),
		maxCapacity: maxCapacity,
		logger:      logger,
	}
}

// Push copies entry to the end of queue and moves tail. Expands backing array by allocating more space if needed.
// Returns index for pushed data or error if maximum size queue limit is reached.
func (q *bytesQueue) push(ce *CacheEntry) (qref, error) {

	size, blobSize := ce.Size(), (*CacheEntry).Size(nil)

	if q.tail >= q.head {
		// o___hDDDDDDDDtr___c
		// check after tail
		if cap(q.array)-q.tail.idx() < size {
			// check before head
			if q.head.idx()-blobSize >= size {
				q.tail.wrap()
			} else if q.maxCapacity > 0 && cap(q.array)+size >= q.maxCapacity {
				return 0, ErrQueueFull
			} else if err := q.expand(size); err != nil {
				return 0, err
			}
		}
	} else {
		// oDDDt________hDDDrc
		// see if entry fits between head and tail
		if q.head.sub(q.tail)-blobSize < size {
			if q.maxCapacity > 0 && cap(q.array)+size >= q.maxCapacity {
				return 0, ErrQueueFull
			}
			if err := q.expand(size); err != nil {
				return 0, err
			}
		}
	}

	// store entry in cache
	q.tail.write(q.array, ce)
	// move tail to the next position
	ref := q.tail.move(size)
	// move end of the data marker
	if q.tail > q.head {
		q.right = q.tail
	}
	q.count++
	return ref, nil
}

// reallocates array keeping all existing indices unchanged.
func (q *bytesQueue) expand(minimum int) error {

	start := time.Now()

	capacity := cap(q.array)
	if capacity < minimum {
		capacity += minimum
	}
	capacity *= 2
	if q.maxCapacity > 0 && capacity > q.maxCapacity {
		capacity = q.maxCapacity
	}
	if capacity < minimum || capacity <= 0 {
		return ErrQueueEntryTooBig
	}

	old := q.array
	q.array = make([]byte, capacity)

	if q.right != 0 {
		copy(q.array, old[:q.right])
		if q.tail < q.head {
			// o___hDDDDDDDDtr_c
			// push
			// oDt__hDDDDDDDDr_c
			// expand+copy
			// oDt__hDDDDDDDDr______________________c
			// to keep indexes unchanged we need to plug a hole
			q.tail.plug(q.head, q.array)
			// ohDeeDDDDDDDDtr______________________c
			q.head.wrap()
			q.tail = q.right
			q.count++
		}
	}

	q.logger.Printf("Allocated new queue in %s; Capacity: %d \n", time.Since(start), capacity)
	return nil
}

// pop reads the oldest entry from queue and moves head pointer to the next one.
func (q *bytesQueue) pop() (qref, error) {
	if q.count == 0 {
		return -1, ErrQueueEmpty
	}
	if !q.head.valid(q.array) {
		return -1, ErrQueueInvalidIndex
	}
	ref := q.head.next(q.array)
	if q.head == q.right {
		q.head.wrap()
		if q.tail == q.right {
			q.tail.wrap()
		}
		q.right = q.tail
	}
	q.count--
	return ref, nil
}

// peek checks that reference could be read.
func (q *bytesQueue) peek(r qref) error {
	if q.count == 0 {
		return ErrQueueEmpty
	}
	if !r.valid(q.array) || r > q.right {
		return ErrQueueInvalidIndex
	}
	return nil
}

func (q *bytesQueue) oldest() (qref, error) {
	if err := q.peek(q.head); err != nil {
		return -1, err
	}
	return q.head, nil
}

func (q *bytesQueue) collide(r qref, key []byte) bool {
	return !bytes.Equal(key, r.key(q.array))
}

// get reads full entry from position without moving any pointers.
// NOTE: this is expensive as it allocates new CacheEntry.
func (q *bytesQueue) get(r qref) (*CacheEntry, error) {
	return r.read(q.array)
}

func (q *bytesQueue) getTS(r qref) uint64 {
	return r.ts(q.array)
}

func (q *bytesQueue) getHash(r qref) uint64 {
	return r.hash(q.array)
}

func (q *bytesQueue) getKey(r qref) []byte {
	return r.key(q.array)
}

// Returns copy of entry's data safe to use outside of shard lock.
func (q *bytesQueue) getDataCopy(r qref) []byte {
	return append([]byte{}, r.data(q.array)...)
}

// mark entry as deleted without destroying information.
func (q *bytesQueue) delete(r qref) error {
	if q.count == 0 {
		return ErrQueueEmpty
	}
	if !r.valid(q.array) || r > q.right {
		return ErrQueueInvalidIndex
	}
	r.clearHash(q.array)
	return nil
}

// reset removes all entries from queue.
func (q *bytesQueue) reset() {
	// Just reset indexes
	q.tail, q.head, q.right, q.count = 0, 0, 0, 0
}

// cap returns number of allocated bytes for queue.
func (q *bytesQueue) cap() int {
	return cap(q.array)
}

// len returns number of entries kept in queue.
func (q *bytesQueue) len() int {
	return q.count
}
