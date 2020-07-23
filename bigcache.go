package bigcache

import (
	"errors"
	"time"
)

var (
	ErrEntryNotFound       = errors.New("entry not found")
	ErrInvalidShardsNumber = errors.New("invalid number of shards, must be power of two")
)

// BigCache is fast, concurrent, evicting cache created to keep big number of entries without impact on performance.
// It keeps entries on heap but omits GC for them. To achieve that, operations take place on byte arrays,
// therefore entries (de)serialization in front of the cache will be needed in most use cases.
type BigCache struct {
	shards       []*cacheShard
	clock        clock
	hash         Hasher
	config       Config
	shardMask    uint64
	maxShardSize uint32
	close        chan struct{}
}

// Processor is a closure supplied to set of WithProcessing functions to take ownership of data []byte avoiding extra memory
// allocation and copy. It has access to exported CacheEntry interface, including byte slice pointing to actual underlying shard buffer
// (not a copy!).
type Processor func(*CacheEntry) error

// NewBigCache initializes new instance of BigCache.
func NewBigCache(config Config) (*BigCache, error) {
	return newBigCache(config, &systemClock{})
}

func newBigCache(config Config, clock clock) (*BigCache, error) {

	if !isPowerOfTwo(config.Shards) {
		return nil, ErrInvalidShardsNumber
	}

	if config.Hasher == nil {
		config.Hasher = newDefaultHasher()
	}
	if config.Logger == nil {
		config.Logger = newNopLogger()
	}

	cache := &BigCache{
		shards:       make([]*cacheShard, config.Shards),
		clock:        clock,
		hash:         config.Hasher,
		config:       config,
		shardMask:    uint64(config.Shards - 1),
		maxShardSize: uint32(config.maximumShardSize()),
		close:        make(chan struct{}),
	}

	for i := 0; i < config.Shards; i++ {
		cache.shards[i] = initNewShard(config, clock)
	}

	if config.CleanWindow > 0 {
		go func() {
			ticker := time.NewTicker(config.CleanWindow)
			defer ticker.Stop()
			for {
				select {
				case t := <-ticker.C:
					cache.cleanUp(uint64(t.Unix()))
				case <-cache.close:
					return
				}
			}
		}()
	}
	return cache, nil
}

// Close is used to signal a shutdown of the cache when you are done with it.
// This allows the cleaning goroutines to exit and ensures references are not
// kept to the cache preventing GC of the entire cache.
func (c *BigCache) Close() error {
	close(c.close)
	return nil
}

// Get reads entry for the key returning copy of cached data.
// It returns an ErrEntryNotFound when no entry exists for the given key.
func (c *BigCache) Get(key string) ([]byte, error) {
	hashedKey := c.hash.Sum64(key)
	shard := c.getShard(hashedKey)
	return shard.get(key, hashedKey, nil)
}

// GetHashed reads entry for the key returning copy of cached data.
// It returns an ErrEntryNotFound when no entry exists for the given key.
// NOTE: it expectes already hashed key.
func (c *BigCache) GetHashed(hashedKey uint64) ([]byte, error) {
	shard := c.getShard(hashedKey)
	return shard.get("", hashedKey, nil)
}

// GetWithProcessing reads entry for the key.
// If found it gives provided Processor closure a chance to process cached entry effectively.
// It returns an ErrEntryNotFound when no entry exists for the given key.
func (c *BigCache) GetWithProcessing(key string, processor Processor) error {
	hashedKey := c.hash.Sum64(key)
	shard := c.getShard(hashedKey)
	_, err := shard.get(key, hashedKey, processor)
	return err
}

// GetHashedWithProcessing reads entry for the key.
// If found it gives provided Processor closure a chance to process cached entry effectively.
// It returns an ErrEntryNotFound when no entry exists for the given key.
// NOTE: it expectes already hashed key.
func (c *BigCache) GetHashedWithProcessing(hashedKey uint64, processor Processor) error {
	shard := c.getShard(hashedKey)
	_, err := shard.get("", hashedKey, processor)
	return err
}

// Set saves entry under the key.
func (c *BigCache) Set(key string, entry []byte) error {
	hashedKey := c.hash.Sum64(key)
	shard := c.getShard(hashedKey)
	return shard.set(key, hashedKey, entry)
}

// SetHashed saves entry under the key.
// NOTE: it expectes already hashed key.
func (c *BigCache) SetHashed(hashedKey uint64, entry []byte) error {
	shard := c.getShard(hashedKey)
	return shard.set("", hashedKey, entry)
}

// Append appends entry under the key if key exists, otherwise
// it will set the key (same behaviour as Set()). With Append() you can
// concatenate multiple entries under the same key in an lock optimized way.
func (c *BigCache) Append(key string, entry []byte) error {
	hashedKey := c.hash.Sum64(key)
	shard := c.getShard(hashedKey)
	return shard.append(key, hashedKey, entry)
}

// AppendHashed appends entry under the key if key exists, otherwise
// it will set the key (same behaviour as Set()). With Append() you can
// concatenate multiple entries under the same key in an lock optimized way.
// NOTE: it expectes already hashed key.
func (c *BigCache) AppendHashed(hashedKey uint64, entry []byte) error {
	shard := c.getShard(hashedKey)
	return shard.append("", hashedKey, entry)
}

// Delete removes the key.
func (c *BigCache) Delete(key string) error {
	hashedKey := c.hash.Sum64(key)
	shard := c.getShard(hashedKey)
	return shard.del(hashedKey)
}

// DeleteHashed removes the key.
// NOTE: it expectes already hashed key.
func (c *BigCache) DeleteHashed(hashedKey uint64) error {
	shard := c.getShard(hashedKey)
	return shard.del(hashedKey)
}

// Reset empties all cache shards.
func (c *BigCache) Reset() error {
	for _, shard := range c.shards {
		shard.reset(c.config)
	}
	return nil
}

// Len computes number of entries in cache.
func (c *BigCache) Len() int {
	var len int
	for _, shard := range c.shards {
		len += shard.len()
	}
	return len
}

// Capacity returns amount of bytes store in the cache.
func (c *BigCache) Capacity() int {
	var len int
	for _, shard := range c.shards {
		len += shard.cap()
	}
	return len
}

// Stats returns cache's statistics.
func (c *BigCache) Stats() Stats {
	var s Stats
	for _, shard := range c.shards {
		tmp := shard.getStats()
		s.Hits += tmp.Hits
		s.Misses += tmp.Misses
		s.DelHits += tmp.DelHits
		s.DelMisses += tmp.DelMisses
		s.Collisions += tmp.Collisions
		s.EvictedExpired += tmp.EvictedExpired
		s.EvictedNoSpace += tmp.EvictedNoSpace
	}
	return s
}

// Range attempts to call f sequentially for each key and value present in the cache.
// If at any point f returns ErrEntryNotFound, Range stops the iteration.
//
// Range does not necessarily correspond to any consistent snapshot of the cache
// contents: no entry will be visited more than once, but if the entry is stored
// or deleted concurrently, Range may reflect any mapping for that entry from any
// point during the Range call or miss it completely.
//
// Range may be O(N) with the number of elements in the map even if f returns
// false after a constant number of calls.
//
// Range is replacement for over-complicated EntryInfoIterator.
func (c *BigCache) Range(f Processor) error {

	// make sure entry is safe to use while shard is unlocked
	duplicator := func(ce *CacheEntry) error {
		ce.Data = ce.CopyData(0)
		ce.Key = append([]byte{}, ce.Key...)
		return nil
	}

	for _, shard := range c.shards {
		// taking snapshot of shard indices
		for _, ref := range shard.copyRefs() {
			if entry, err := shard.getEntry(ref, duplicator); err != nil {
				if !errors.Is(err, ErrEntryNotFound) {
					return err
				}
				continue
			} else if err = f(entry); err != nil {
				if errors.Is(err, ErrEntryNotFound) {
					// stop is requested
					return nil
				}
				return err
			}
		}
	}
	return nil
}

func (c *BigCache) cleanUp(currentTimestamp uint64) {
	for _, shard := range c.shards {
		shard.cleanUp(currentTimestamp)
	}
}

func (c *BigCache) getShard(hashedKey uint64) (shard *cacheShard) {
	return c.shards[hashedKey&c.shardMask]
}
