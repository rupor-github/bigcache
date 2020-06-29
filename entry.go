package bigcache

import (
	"encoding/binary"
	"errors"
)

var (
	ErrCacheEntryCorrupted = errors.New("cache entry is corrupted, unable to read")
)

const (
	sizeLen    = 4 // Number of bytes to be used for full length of serialized cache entry
	sizeTS     = 8 // Number of bytes used for timestamp
	sizeHash   = 8 // Number of bytes used for hash
	sizeKeyLen = 2 // Number of bytes used for size of entry key

	offLen    = 0
	offTS     = offLen + sizeLen
	offHash   = offTS + sizeTS
	offKeyLen = offHash + sizeHash
	offKeyStr = offKeyLen + sizeKeyLen
)

type qref int

func (r qref) valid(buf []byte) bool {
	if r < 0 {
		return false
	}
	bl := len(buf)
	if bl < sizeLen {
		return false
	}
	l := r.size(buf)
	return l >= offKeyStr || bl >= l
}

func (r qref) idx() int {
	return int(r)
}

func (r *qref) wrap() {
	*r = 0
}

func (r *qref) move(size int) qref {
	q := *r
	*r += qref(size)
	return q
}

func (r qref) sub(s qref) int {
	return int(r - s)
}

func (r *qref) next(buf []byte) qref {
	q := *r
	*r += qref(r.size(buf))
	return q
}

func (r qref) size(buf []byte) int {
	return int(binary.LittleEndian.Uint32(buf[r+offLen:]))
}

func (r qref) ts(buf []byte) uint64 {
	return binary.LittleEndian.Uint64(buf[r+offTS:])
}

func (r qref) hash(buf []byte) uint64 {
	return binary.LittleEndian.Uint64(buf[r+offHash:])
}

func (r qref) key(buf []byte) []byte {
	kl := int(binary.LittleEndian.Uint16(buf[r+offKeyLen:]))
	return buf[r+offKeyStr : int(r)+offKeyStr+kl]
}

func (r qref) data(buf []byte) []byte {
	l, kl := r.size(buf), int(binary.LittleEndian.Uint16(buf[r+offKeyLen:]))
	return buf[int(r)+offKeyStr+kl : int(r)+l]
}

// Reads buffer from qref position returning CacheEntry which is not safe to be used without shard lock.
func (r qref) read(buf []byte) (*CacheEntry, error) {
	if !r.valid(buf) {
		return nil, ErrCacheEntryCorrupted
	}
	return &CacheEntry{
		TS:   r.ts(buf),
		Hash: r.hash(buf),
		Key:  r.key(buf),
		Data: r.data(buf), // could save 2 buffer reads here - beauty first
	}, nil
}

// Writes entry into buffer at qref position. If buffer is too small it will panic.
// NOTE: for efficiency it is assumed that all checks necessary on the buffer availability happen before write was called.
func (r qref) write(buf []byte, ce *CacheEntry) {
	binary.LittleEndian.PutUint32(buf[int(r)+offLen:], uint32(ce.Size()))
	binary.LittleEndian.PutUint64(buf[int(r)+offTS:], ce.TS)
	binary.LittleEndian.PutUint64(buf[int(r)+offHash:], ce.Hash)
	binary.LittleEndian.PutUint16(buf[int(r)+offKeyLen:], uint16(len(ce.Key)))
	copy(buf[int(r)+offKeyStr:], ce.Key)
	copy(buf[int(r)+offKeyStr+len(ce.Key):], ce.Data)
}

// Plugs empty space between position held and position passed by creating empty cache entry to cover the whole area.
// NOTE: it assumed that size of empty area cannot ever be smaller than minimal cache entry size and it is enforced before plug is called.
func (r qref) plug(s qref, buf []byte) {
	l := s.sub(r)
	binary.LittleEndian.PutUint32(buf[int(r)+offLen:], uint32(l))
	// memclr
	z := buf[int(r)+sizeLen : s]
	for i := range z {
		z[i] = 0
	}
}

// If hash is 0 entry was explicitly deleted.
func (r qref) clearHash(buf []byte) {
	binary.LittleEndian.PutUint64(buf[int(r)+offHash:], 0)
}

// CacheEntry is used to interface between bigcache and shards and to provide user with access to low level cache memory when necessary.
// NOTE: In some cases (usually in callbacks) for efficiency Key and Data fields give access to underlying queue buffer, which is only
// safe while shard lock is held. To make this obvious Copy methods exit - it is up to user to decide how to use it.
type CacheEntry struct {
	TS   uint64
	Hash uint64
	Key  []byte
	Data []byte
}

// Size returns number of bytes needed to store entry. When called on nil entry returns size of the header - minimal size of any entry in the cache.
// NOTE: size includes size of the size itself and that is what is being written into underlying buffer when entry is stored.
func (ce *CacheEntry) Size() int {
	l := offKeyStr
	if ce != nil {
		l += len(ce.Key) + len(ce.Data)
	}
	return l
}

// CopyKey returns copy of Key slice as a string - safe to use without shard lock.
func (ce *CacheEntry) CopyKey() string {
	return string(ce.Key)
}

// CopyData returns copy of underlying slice - safe to use without shard lock.
// As an optimization you could allocate bigger backing array.
func (ce *CacheEntry) CopyData(newCap int) []byte {
	l := len(ce.Data)
	if newCap < l {
		newCap = l
	}
	s := make([]byte, l, newCap)
	copy(s, ce.Data)
	return s
}
