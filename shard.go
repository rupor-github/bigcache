package bigcache

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

type cacheShard struct {
	sync.RWMutex
	hashmap    map[uint64]qref
	entries    *bytesQueue
	onRemove   OnRemoveCallback
	lifeWindow uint64
	clock      clock
	logger     Logger
	stats      Stats
}

func (s *cacheShard) get(key string, hash uint64, f Processor) ([]byte, error) {

	s.RLock()
	defer s.RUnlock()

	return s.getWithoutLock(key, hash, f)
}

func (s *cacheShard) getWithoutLock(key string, hash uint64, f Processor) ([]byte, error) {
	ref, found := s.hashmap[hash]
	if !found {
		s.miss()
		return nil, ErrEntryNotFound
	}
	err := s.entries.peek(ref)
	if err != nil {
		s.miss()
		return nil, err
	}
	if len(key) > 0 && s.entries.collide(ref, []byte(key)) {
		// TODO: do we actually need this print - our logger is not level'ed?
		s.logger.Printf("Collision detected. Both %q and %q have the same hash %x", key, s.entries.getKey(ref), hash)
		s.collision()
		return nil, ErrEntryNotFound
	}
	s.hit()
	if f != nil {
		ce, _ := s.entries.get(ref)
		return nil, f(ce)
	}
	return s.entries.getDataCopy(ref), nil
}

func (s *cacheShard) set(key string, hash uint64, entry []byte) error {

	s.Lock()
	defer s.Unlock()

	return s.setWithoutLock(key, hash, entry)
}

func (s *cacheShard) setWithoutLock(key string, hash uint64, entry []byte) error {

	current := s.clock.epoch()

	if prev, found := s.hashmap[hash]; found {
		if err := s.entries.delete(prev); err == nil {
			delete(s.hashmap, hash)
		}
	}

	if oldest, err := s.entries.oldest(); err == nil {
		if current-s.entries.getTS(oldest) > s.lifeWindow {
			_ = s.evictOldest(Expired)
		}
	}

	ce := &CacheEntry{
		TS:   current,
		Hash: hash,
		Key:  []byte(key),
		Data: entry,
	}

	for {
		if ref, err := s.entries.push(ce); err == nil {
			s.hashmap[hash] = ref
			return nil
		}
		if err := s.evictOldest(NoSpace); err != nil {
			return fmt.Errorf("new entry is bigger than max shard size: %w", err)
		}
	}
}

func (s *cacheShard) cleanUp(timestamp uint64) {

	s.Lock()
	defer s.Unlock()

	var oldest qref
	var err error
	for {
		if oldest, err = s.entries.oldest(); err != nil {
			break
		}
		if timestamp-s.entries.getTS(oldest) <= s.lifeWindow {
			break
		}
		if err = s.evictOldest(Expired); err != nil {
			break
		}
	}
}

func (s *cacheShard) evictOldest(reason RemoveReason) error {
	oldest, err := s.entries.pop()
	if err != nil {
		return err
	}
	hash := s.entries.getHash(oldest)
	if hash == 0 {
		// ignore explicitly deleted entries
		return nil
	}
	delete(s.hashmap, hash)

	// NOTE: User should not have a call back just to count evictions - it is expensive
	switch reason {
	case Expired:
		s.expired()
	case NoSpace:
		s.nospace()
	case Deleted:
		fallthrough
	case NoReason:
		panic("this should never happen")
	}

	if s.onRemove != nil {
		// only allocate memory if needed
		ce, _ := s.entries.get(oldest)
		s.onRemove(ce, reason)
	}
	return nil
}

func (s *cacheShard) append(key string, hash uint64, entry []byte) error {

	s.Lock()
	defer s.Unlock()

	var data []byte
	appender := func(ce *CacheEntry) error {
		data = append(ce.CopyData(len(ce.Data)+len(entry)), entry...)
		return nil
	}

	if _, err := s.getWithoutLock(key, hash, appender); err != nil {
		if !errors.Is(err, ErrEntryNotFound) {
			return err
		}
		data = entry
	}
	return s.setWithoutLock(key, hash, data)
}

func (s *cacheShard) del(hash uint64) error {

	s.Lock()
	defer s.Unlock()

	ref, found := s.hashmap[hash]
	if !found {
		s.delmiss()
		return ErrEntryNotFound
	}

	if err := s.entries.delete(ref); err != nil {
		s.delmiss()
		return err
	}

	delete(s.hashmap, hash)
	if s.onRemove != nil {
		// only allocate memory if needed
		ce, _ := s.entries.get(ref)
		// restore hash value - it was set to 0 by deletion
		ce.Hash = hash
		s.onRemove(ce, Deleted)
	}
	s.delhit()
	return nil
}

// Used during Range only - does not update stats and does not check for collisions.
// NOTE: always returns entry if found, even if processor produces error.
func (s *cacheShard) getEntry(r qref, f Processor) (*CacheEntry, error) {

	s.RLock()
	defer s.RUnlock()

	ce, err := s.entries.get(r)
	if err != nil {
		return nil, err
	}
	if f != nil {
		return ce, f(ce)
	}
	return ce, nil
}

// Used during Range only - expensive copy of entry references available in the shard's queue at the moment.
func (s *cacheShard) copyRefs() []qref {

	s.RLock()
	defer s.RUnlock()

	indices := make([]qref, 0, len(s.hashmap))
	for _, r := range s.hashmap {
		indices = append(indices, r)
	}
	return indices
}

func (s *cacheShard) reset(config Config) {

	s.Lock()
	defer s.Unlock()

	s.hashmap = make(map[uint64]qref, config.initialShardSize())
	s.entries.reset()
}

func (s *cacheShard) len() int {

	s.RLock()
	defer s.RUnlock()

	res := len(s.hashmap)
	return res
}

func (s *cacheShard) cap() int {
	s.RLock()
	defer s.RUnlock()
	res := s.entries.cap()
	return res
}

func (s *cacheShard) getStats() Stats {
	var stats = Stats{
		Hits:           atomic.LoadInt64(&s.stats.Hits),
		Misses:         atomic.LoadInt64(&s.stats.Misses),
		DelHits:        atomic.LoadInt64(&s.stats.DelHits),
		DelMisses:      atomic.LoadInt64(&s.stats.DelMisses),
		Collisions:     atomic.LoadInt64(&s.stats.Collisions),
		EvictedExpired: atomic.LoadInt64(&s.stats.EvictedExpired),
		EvictedNoSpace: atomic.LoadInt64(&s.stats.EvictedNoSpace),
	}
	return stats
}

func (s *cacheShard) hit() {
	atomic.AddInt64(&s.stats.Hits, 1)
}

func (s *cacheShard) miss() {
	atomic.AddInt64(&s.stats.Misses, 1)
}

func (s *cacheShard) delhit() {
	atomic.AddInt64(&s.stats.DelHits, 1)
}

func (s *cacheShard) delmiss() {
	atomic.AddInt64(&s.stats.DelMisses, 1)
}

func (s *cacheShard) collision() {
	atomic.AddInt64(&s.stats.Collisions, 1)
}

func (s *cacheShard) expired() {
	atomic.AddInt64(&s.stats.EvictedExpired, 1)
}

func (s *cacheShard) nospace() {
	atomic.AddInt64(&s.stats.EvictedNoSpace, 1)
}

func initNewShard(config Config, clock clock) *cacheShard {
	bytesQueueInitialCapacity := config.initialShardSize() * config.MaxEntrySize
	maximumShardSizeInBytes := config.maximumShardSizeInBytes()
	if maximumShardSizeInBytes > 0 && bytesQueueInitialCapacity > maximumShardSizeInBytes {
		bytesQueueInitialCapacity = maximumShardSizeInBytes
	}
	return &cacheShard{
		hashmap:    make(map[uint64]qref, config.initialShardSize()),
		entries:    newBytesQueue(bytesQueueInitialCapacity, maximumShardSizeInBytes, config.Logger),
		onRemove:   config.OnRemove,
		logger:     config.Logger,
		clock:      clock,
		lifeWindow: uint64(config.LifeWindow.Seconds()),
	}
}
