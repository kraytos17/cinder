package ae

import (
	"github.com/zeebo/xxh3"
)

// BucketFor maps a key to its anti-entropy bucket: XXH3(key) mod buckets.
func BucketFor(key string, numBuckets uint32) uint32 {
	if numBuckets == 0 {
		return 0
	}
	return uint32(xxh3.HashString(key) % uint64(numBuckets))
}

// KeyHash is the digest identity atom's key component.
func KeyHash(key string) uint64 {
	return xxh3.HashString(key)
}

// EntryHash is the per-entry digest atom: XXH3 over the key, mixed with
// the packed (version, flags) word and writer, then splitmix64-finalized.
// Arithmetic wraps mod 2^64 exactly like the C++ unsigned arithmetic.
func EntryHash(key string, versionAndTTL, writer uint64) uint64 {
	h := xxh3.HashString(key)
	h += 0x9E3779B97F4A7C15 + (versionAndTTL << 1) + versionAndTTL
	h ^= writer + 0x9E3779B97F4A7C15 + (h << 6) + (h >> 2)
	h ^= h >> 30
	h *= 0xBF58476D1CE4E5B9
	h ^= h >> 27
	h *= 0x94D049BB133111EB
	h ^= h >> 31
	return h
}
