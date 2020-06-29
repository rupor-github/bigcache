package bigcache

// Stats stores cache statistics.
type Stats struct {
	// Hits is a number of successfully found keys
	Hits int64 `json:"hits"`
	// Misses is a number of not found keys
	Misses int64 `json:"misses"`
	// DelHits is a number of successfully deleted keys
	DelHits int64 `json:"delete_hits"`
	// DelMisses is a number of not deleted keys
	DelMisses int64 `json:"delete_misses"`
	// Collisions is a number of happened key-collisions
	Collisions int64 `json:"collisions"`
	// EvictedExpired is a number of entries evicted due to expiration
	EvictedExpired int64 `json:"expired"`
	// EvictedNoSpace is a number of entries evicted due to absence of free space
	EvictedNoSpace int64 `json:"nospace"`
}
