package ae

import (
	"cmp"
	"encoding/binary"
	"slices"
	"testing"
)

// FuzzIBLTDifference fuzzes invertible-Bloom-lookup reconciliation with
// structured item sets. Unlike ibltFuzz (arbitrary blobs, termination
// only), it splits decoded items into shared/remote-only/local-only
// subsets and asserts the exact symmetric difference when peeling
// succeeds — plus two deterministic properties that must always hold:
//
//   - a single-item difference always peels (its cells are pure by
//     construction), so !ok there is a bug, not bad luck;
//   - identical multisets always peel to an empty diff.
//
// Generated items are deduplicated by full identity: identical rows cancel
// in every XOR field while their counts add, making the sketch ambiguous
// (see DifferenceIBLT), and production never emits them. Multi-item peel
// can stall legitimately (ambiguous overlap); !ok there is a skip, never
// a failure. All assertions use full-struct identity (tombstone included),
// matching TestIBLTDifference's live-vs-delete separation. The fixed
// 64-cell width matches production (IBLTCells).
func FuzzIBLTDifference(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{3, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	f.Add([]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})
	f.Fuzz(func(t *testing.T, data []byte) {
		var split [3]byte
		copy(split[:], data)
		rest := data
		if len(rest) >= 3 {
			rest = rest[3:]
		} else {
			rest = nil
		}

		var items []IBLTItem
		seen := make(map[IBLTItem]struct{})
		for len(rest) >= 11 && len(items) < 12 {
			it := IBLTItem{
				KeyHash: binary.LittleEndian.Uint64(rest[:8]),
				Version: uint64(rest[8]),
				Writer:  uint64(rest[9]),
				// Tombstone rides its own byte so the fuzzer can flip
				// liveness without touching identity fields.
				Tombstone: rest[10]&1 != 0,
			}
			rest = rest[11:]
			// Skip exact duplicates: identical rows cancel in every
			// XOR field while their counts add, making the sketch
			// ambiguous (see DifferenceIBLT). Production never emits
			// them (one metadata row per store key per scan).
			if _, dup := seen[it]; dup {
				continue
			}
			seen[it] = struct{}{}
			items = append(items, it)
		}

		n := len(items)
		a := int(split[0]) % (n + 1)
		b := a + int(split[1])%(n+1-a)
		c := b + int(split[2])%(n+1-b)
		remoteAll, localAll := symDiffInputs(items, a, b, c)
		remote := BuildIBLT(remoteAll, IBLTCells)
		local := BuildIBLT(localAll, IBLTCells)
		diff, ok := DifferenceIBLT(remote, local)

		wantRemote, wantLocal := symDiff(remoteAll, localAll)
		if len(wantRemote)+len(wantLocal) == 1 {
			// Deterministic: one item's cells are pure by construction.
			if !ok {
				t.Fatalf("single-item diff failed to peel (remote=%d local=%d)", len(wantRemote), len(wantLocal))
			}
		}
		if len(wantRemote) == 0 && len(wantLocal) == 0 {
			// Deterministic: identical multisets cancel to zero cells.
			if !ok || len(diff.RemoteOnly) != 0 || len(diff.LocalOnly) != 0 {
				t.Fatalf("identical sets did not peel clean: ok=%v %+v", ok, diff)
			}
			return
		}
		if !ok {
			return // legitimate stall on ambiguous overlap
		}
		if !itemSetsEqual(diff.RemoteOnly, wantRemote) {
			t.Fatalf("remote mismatch:\ngot  %+v\nwant %+v", diff.RemoteOnly, wantRemote)
		}
		if !itemSetsEqual(diff.LocalOnly, wantLocal) {
			t.Fatalf("local mismatch:\ngot  %+v\nwant %+v", diff.LocalOnly, wantLocal)
		}
	})
}

// symDiffInputs partitions items into the remote set (shared prefix plus
// remote-only middle) and the local set (shared prefix plus local-only
// tail), mirroring an anti-entropy round where both sides hold the same
// range-hash bucket prefix.
func symDiffInputs(items []IBLTItem, a, b, c int) (remote, local []IBLTItem) {
	remote = append(slices.Clone(items[:a]), items[a:b]...)
	local = append(slices.Clone(items[:a]), items[b:c]...)
	return remote, local
}

// symDiff returns the multiset symmetric difference of remote minus local
// and vice versa, keyed on full item identity (tombstone included, so a
// live write and its delete never cancel).
func symDiff(remote, local []IBLTItem) (onlyRemote, onlyLocal []IBLTItem) {
	counts := make(map[IBLTItem]int, len(remote)+len(local))
	for _, r := range remote {
		counts[r]++
	}
	for _, l := range local {
		counts[l]--
	}
	for item, n := range counts {
		// max() guards: range-over-int panics on negative bounds.
		for range max(n, 0) {
			onlyRemote = append(onlyRemote, item)
		}
		for range max(-n, 0) {
			onlyLocal = append(onlyLocal, item)
		}
	}
	return onlyRemote, onlyLocal
}

func itemSetsEqual(a, b []IBLTItem) bool {
	ac := slices.Clone(a)
	bc := slices.Clone(b)
	tombInt := func(t bool) int {
		if t {
			return 1
		}
		return 0
	}
	less := func(x, y IBLTItem) int {
		return cmp.Or(cmp.Compare(x.KeyHash, y.KeyHash), cmp.Compare(x.Version, y.Version), cmp.Compare(x.Writer, y.Writer), cmp.Compare(tombInt(x.Tombstone), tombInt(y.Tombstone)))
	}
	slices.SortFunc(ac, less)
	slices.SortFunc(bc, less)
	return slices.Equal(ac, bc)
}
