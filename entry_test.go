package bigcache

import (
	"bytes"
	"math/rand"
	"testing"
	"time"
)

func makeCacheEntry(key, buffer string) *CacheEntry {
	return &CacheEntry{
		TS:   uint64(time.Now().Unix()),
		Hash: uint64(rand.Int63()),
		Key:  []byte(key),
		Data: []byte(buffer),
	}
}

func makeCacheBlob(char byte, entrySize int) *CacheEntry {
	return &CacheEntry{
		TS:   0x5555_5555_5555_5555,
		Hash: 0xDADA_DADA_DADA_DADA,
		Key:  []byte{},
		Data: bytes.Repeat([]byte{char}, entrySize),
	}
}

func TestEncodeDecode(t *testing.T) {
	// given
	ce := makeCacheEntry("key", "data")
	buffer := make([]byte, 100)
	r := qref(0)

	// when
	r.write(buffer, ce)
	ce1, err := r.read(buffer)

	// then
	noError(t, err)
	assertEqual(t, ce, ce1)
}

func TestPlug(t *testing.T) {

	// given
	buffer := bytes.Repeat([]byte("x"), 100)
	head := qref(0)
	tail := qref(len(buffer))

	// when
	head.plug(tail, buffer)
	ce, err := head.read(buffer)

	// then
	noError(t, err)
	assertEqual(t, 100, ce.Size())
}
