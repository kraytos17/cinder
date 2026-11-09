// Package sharding holds the capacity-sharding rule shared by the KV
// store and the counter store: one shard per 64 KiB, clamped to [1, 16],
// so tiny stores stay single-shard. Keys stripe by seeded maphash;
// the seeds stay per-store (each store keeps its own placement), only
// the rule and the hash application are shared.
//
// Shard placement is internal only — never on the wire — unlike the
// ring positions and checksums where xxh3 stays byte-identical by law.
package sharding

import (
	"hash/maphash"
)

const (
	// MaxShards caps the striping; more shards only add lock overhead.
	MaxShards = 16
	// MinShardBytes is the capacity slice per shard.
	MinShardBytes = 65_536
)

// CountFor returns the shard count for a capacity budget.
func CountFor(capacityBytes uint64) int {
	return min(max(int(capacityBytes/MinShardBytes), 1), MaxShards)
}

// ForKey returns the shard index for key under the store's seed.
func ForKey(key string, n int, seed maphash.Seed) int {
	return int(maphash.String(seed, key) % uint64(n))
}
