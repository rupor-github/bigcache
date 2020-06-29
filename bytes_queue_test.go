package bigcache

import (
	"testing"
)

func TestPushAndPop(t *testing.T) {
	t.Parallel()

	// given
	q := newBytesQueue(10, 0, newNopLogger())
	ce := makeCacheEntry("key", "hello")

	// when
	ref, err := q.pop()
	// then
	assertEqual(t, qref(-1), ref)
	assertEqual(t, "byte queue is empty", err.Error())

	// when
	ref, err = q.push(ce)
	// then
	noError(t, err)
	assertEqual(t, qref(0), ref)

	// when
	ref, err = q.pop()
	ce1, err1 := q.get(ref)
	// then
	noError(t, err)
	noError(t, err1)

	assertEqual(t, ce, ce1)
}

func TestLen(t *testing.T) {
	t.Parallel()

	// given
	q := newBytesQueue(100, 0, newNopLogger())
	assertEqual(t, 0, q.len())
	ce := makeCacheEntry("key", "hello")

	// when
	ref, err := q.push(ce)
	// then
	noError(t, err)
	assertEqual(t, qref(0), ref)
	assertEqual(t, q.len(), 1)

	// when
	ref, err = q.pop()
	// then
	noError(t, err)
	assertEqual(t, q.len(), 0)
}

func TestPeek(t *testing.T) {
	t.Parallel()

	// given
	q := newBytesQueue(100, 0, newNopLogger())
	ce := makeCacheEntry("key", "hello")

	// when
	err := q.peek(q.head)
	// then
	assertEqual(t, "byte queue is empty", err.Error())

	// when
	ref, err := q.push(ce)
	// then
	noError(t, err)
	assertEqual(t, qref(0), ref)

	// when
	ref, err = q.pop()
	ce1, err1 := q.get(ref)
	// then
	noError(t, err)
	noError(t, err1)
	assertEqual(t, ce, ce1)
}

func TestReset(t *testing.T) {
	t.Parallel()

	// given
	queue := newBytesQueue(100, 0, newNopLogger())
	ce := makeCacheEntry("key", "hello")

	// when
	queue.push(ce)
	queue.push(ce)
	queue.push(ce)
	queue.reset()
	err := queue.peek(queue.head)
	// then
	assertEqual(t, "byte queue is empty", err.Error())
	assertEqual(t, 0, queue.len())

	// when
	queue.push(ce)
	ref, err := queue.pop()
	ce1, err1 := queue.get(ref)
	// then
	noError(t, err)
	noError(t, err1)
	assertEqual(t, ce, ce1)

	// when
	err = queue.peek(queue.head)
	// then
	assertEqual(t, "byte queue is empty", err.Error())
}

func TestReuseAvailableSpace(t *testing.T) {
	t.Parallel()

	// given
	queue := newBytesQueue(110, 0, newNopLogger())

	// when
	queue.push(makeCacheBlob('a', 48))
	queue.push(makeCacheBlob('b', 8))
	queue.pop()
	queue.push(makeCacheBlob('c', 8))

	// then
	assertEqual(t, 110, queue.cap())

	ref, err := queue.pop()
	ce, err1 := queue.get(ref)

	noError(t, err)
	noError(t, err1)

	assertEqual(t, makeCacheBlob('b', 8), ce)
}

func TestAllocateAdditionalSpace(t *testing.T) {
	t.Parallel()

	// given
	queue := newBytesQueue(32, 0, newNopLogger())

	// when
	queue.push(makeCacheBlob('a', 8))
	queue.push(makeCacheBlob('b', 8))

	// then
	assertEqual(t, 64, queue.cap())
}

func TestAllocateAdditionalSpaceForInsufficientFreeFragmentedSpaceWhereHeadIsBeforeTail(t *testing.T) {
	t.Parallel()

	smallest := (*CacheEntry).Size(nil) // 22

	// given
	blobA := makeCacheBlob('a', 3) // 25 bytres
	assertEqual(t, smallest+3, blobA.Size())
	blobB := makeCacheBlob('b', 6) // 28 byts
	assertEqual(t, smallest+6, blobB.Size())
	blobC := makeCacheBlob('c', 6) // 28 bytes
	assertEqual(t, smallest+6, blobC.Size())

	qsize := blobA.Size() + blobB.Size() + blobC.Size() + 10
	queue := newBytesQueue(qsize, 0, newNopLogger())

	// when
	queue.push(blobA) // 25 bytes
	queue.push(blobB) // additional 28 bytes
	queue.pop()       // space freed, 25 bytes available at the beginning
	queue.push(blobC) // 28 bytes needed,   10 bytes available at the tail

	// then
	assertEqual(t, qsize, queue.cap())
	ref, err := queue.pop()
	ce, err1 := queue.get(ref)
	noError(t, err)
	noError(t, err1)
	assertEqual(t, blobB, ce)
	ref, err = queue.pop()
	ce, err1 = queue.get(ref)
	noError(t, err)
	noError(t, err1)
	assertEqual(t, blobC, ce)
}

func TestUnchangedEntriesIndexesAfterAdditionalMemoryAllocationWhereHeadIsBeforeTail(t *testing.T) {
	t.Parallel()

	smallest := (*CacheEntry).Size(nil) // 22

	// given
	blobA := makeCacheBlob('a', 3) // 25 bytres
	assertEqual(t, smallest+3, blobA.Size())
	blobB := makeCacheBlob('b', 6) // 28 byts
	assertEqual(t, smallest+6, blobB.Size())
	blobC := makeCacheBlob('c', 6) // 28 bytes
	assertEqual(t, smallest+6, blobC.Size())
	blobD := makeCacheBlob('d', 6) // 28 bytes
	assertEqual(t, smallest+6, blobD.Size())

	qsize := blobA.Size() + blobB.Size() + blobC.Size() + 10
	queue := newBytesQueue(qsize, 0, newNopLogger())

	// when
	queue.push(blobA)            // 25 bytes
	refB, _ := queue.push(blobB) // additional 28 bytes
	queue.pop()                  // space freed, 25 bytes available at the beginning
	refC, _ := queue.push(blobC) // 28 bytes needed,   10 bytes available at the tail

	// reallocation
	queue.push(blobD) // another 28 bytes

	// then
	assertEqual(t, qsize*2, queue.cap())
	ce, err := queue.get(refB)
	noError(t, err)
	assertEqual(t, blobB, ce)
	ce, err = queue.get(refC)
	noError(t, err)
	assertEqual(t, blobC, ce)
}

func TestAllocateAdditionalSpaceForInsufficientFreeFragmentedSpaceWhereTailIsBeforeHead(t *testing.T) {
	t.Parallel()

	smallest := (*CacheEntry).Size(nil) // 22

	// given
	blobA := makeCacheBlob('a', 70)
	assertEqual(t, smallest+70, blobA.Size())
	blobB := makeCacheBlob('b', 10)
	assertEqual(t, smallest+10, blobB.Size())
	blobC := makeCacheBlob('c', 30)
	assertEqual(t, smallest+30, blobC.Size())
	blobD := makeCacheBlob('d', 40)
	assertEqual(t, smallest+40, blobD.Size())

	qsize := blobA.Size() + blobB.Size() + 10
	queue := newBytesQueue(qsize, 0, newNopLogger())

	// when
	queue.push(blobA) // 92 bytes
	queue.push(blobB) // 92 + 32 = 124 bytes
	queue.pop()       // space freed at the beginning 92 bytes
	queue.push(blobC) // 52 bytes used at the beginning, tail pointer is before head pointer
	queue.push(blobD) // 62 bytes needed but no available in one segment, allocate new memory

	// then
	assertEqual(t, qsize*2, queue.cap())

	ref, err := queue.pop()
	ce, err1 := queue.get(ref)
	noError(t, err)
	noError(t, err1)
	assertEqual(t, blobC, ce)
	// empty blob fills space between tail and head, created when additional memory was allocated to keep indexes unchanged
	ref, err = queue.pop()
	ce, err1 = queue.get(ref)
	noError(t, err)
	noError(t, err1)
	assertEqual(t, 40, ce.Size())
	ref, err = queue.pop()
	ce, err1 = queue.get(ref)
	noError(t, err)
	noError(t, err1)
	assertEqual(t, blobB, ce)
	ref, err = queue.pop()
	ce, err1 = queue.get(ref)
	noError(t, err)
	noError(t, err1)
	assertEqual(t, blobD, ce)
}

func TestUnchangedEntriesIndexesAfterAdditionalMemoryAllocationWhereTailIsBeforeHead(t *testing.T) {
	t.Parallel()

	smallest := (*CacheEntry).Size(nil) // 22

	// given
	blobA := makeCacheBlob('a', 70)
	assertEqual(t, smallest+70, blobA.Size())
	blobB := makeCacheBlob('b', 10)
	assertEqual(t, smallest+10, blobB.Size())
	blobC := makeCacheBlob('c', 30)
	assertEqual(t, smallest+30, blobC.Size())
	blobD := makeCacheBlob('d', 40)
	assertEqual(t, smallest+40, blobD.Size())

	qsize := blobA.Size() + blobB.Size() + 10
	queue := newBytesQueue(qsize, 0, newNopLogger())

	// when
	queue.push(blobA)            // 92 bytes
	refB, _ := queue.push(blobB) // 92 + 32 = 124 bytes
	queue.pop()                  // space freed at the beginning 92 bytes
	queue.push(blobC)            // 52 bytes used at the beginning, tail pointer is before head pointer
	refD, _ := queue.push(blobD) // 62 bytes needed but no available in one segment, allocate new memory

	// then
	assertEqual(t, qsize*2, queue.cap())
	ce, err := queue.get(refB)
	noError(t, err)
	assertEqual(t, blobB, ce)
	ce, err = queue.get(refD)
	noError(t, err)
	assertEqual(t, blobD, ce)
}

func TestAllocateAdditionalSpaceForValueBiggerThanInitQueue(t *testing.T) {
	t.Parallel()

	smallest := (*CacheEntry).Size(nil) // 22

	// given
	queue := newBytesQueue(11, 0, newNopLogger())

	// when
	queue.push(makeCacheBlob('a', 100))
	// then
	ref, err := queue.pop()
	ce, err1 := queue.get(ref)
	noError(t, err)
	noError(t, err1)
	assertEqual(t, makeCacheBlob('a', 100), ce)

	// 266 = (100 + 22 + 11) * 2
	assertEqual(t, (smallest+11+100)*2, queue.cap())
}

func TestAllocateAdditionalSpaceForValueBiggerThanQueue(t *testing.T) {
	t.Parallel()

	smallest := (*CacheEntry).Size(nil) // 22

	// given
	blobA := makeCacheBlob('a', 2)
	blobB := makeCacheBlob('b', 3)
	blobC := makeCacheBlob('c', 100)

	qsize := blobA.Size() + blobB.Size()
	queue := newBytesQueue(qsize, 0, newNopLogger())

	// when
	queue.push(blobA)
	queue.push(blobB)
	queue.push(blobC)

	// then
	queue.pop()
	queue.pop()
	ref, err := queue.pop()
	ce, err1 := queue.get(ref)
	noError(t, err)
	noError(t, err1)
	assertEqual(t, blobC, ce)
	// 342 = (49 + 122) * 2
	assertEqual(t, (qsize+smallest+100)*2, queue.cap())
}

func TestGetEntryFromIndex(t *testing.T) {
	t.Parallel()

	// given
	queue := newBytesQueue(20, 0, newNopLogger())

	// when
	queue.push(makeCacheBlob('a', 1))
	ref, _ := queue.push(makeCacheBlob('b', 1))
	queue.push(makeCacheBlob('c', 1))
	ce, err := queue.get(ref)
	// then
	noError(t, err)
	assertEqual(t, makeCacheBlob('b', 1), ce)
}

func TestGetEntryFromInvalidIndex(t *testing.T) {
	t.Parallel()

	// given
	queue := newBytesQueue(1, 0, newNopLogger())
	queue.push(makeCacheBlob('a', 1))

	// when
	err := queue.peek(-1)

	// then
	assertEqual(t, "byte queue index is out of bounds (0 <= index < right)", err.Error())
}

func TestGetEntryFromIndexOutOfRange(t *testing.T) {
	t.Parallel()

	// given
	blobA := makeCacheBlob('a', 1)
	queue := newBytesQueue(1, 0, newNopLogger())
	queue.push(blobA)

	// when
	err := queue.peek(qref(blobA.Size() + 1))

	// then
	assertEqual(t, "byte queue index is out of bounds (0 <= index < right)", err.Error())
}

func TestGetEntryFromEmptyQueue(t *testing.T) {
	t.Parallel()

	// given
	queue := newBytesQueue(13, 0, newNopLogger())

	// when
	err := queue.peek(1)

	// then
	assertEqual(t, "byte queue is empty", err.Error())
}

func TestMaxSizeLimit(t *testing.T) {
	t.Parallel()

	// given
	blobA := makeCacheBlob('a', 25)
	blobB := makeCacheBlob('b', 5)
	blobC := makeCacheBlob('c', 25)

	maxsize := blobA.Size() + blobB.Size() + 5
	queue := newBytesQueue(blobA.Size(), maxsize, newNopLogger())

	// when
	queue.push(blobA)
	queue.push(blobB)
	capacity := queue.cap()
	_, err := queue.push(blobC)

	// then
	assertEqual(t, maxsize, capacity)
	assertEqual(t, "byte queue is full, size limit is reached", err.Error())
	assertEqual(t, maxsize, queue.cap())

	ref, err := queue.pop()
	ce, err1 := queue.get(ref)
	noError(t, err)
	noError(t, err1)
	assertEqual(t, blobA, ce)

	ref, err = queue.pop()
	ce, err1 = queue.get(ref)
	noError(t, err)
	noError(t, err1)
	assertEqual(t, blobB, ce)
}
