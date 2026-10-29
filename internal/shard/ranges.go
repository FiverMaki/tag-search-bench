// Package shard splits the UUID key space into contiguous ranges.
package shard

import "fmt"

// Range is a half-open interval [Lo, Hi) over lowercase UUID strings.
// An empty Lo or Hi means unbounded on that side.
type Range struct {
	Lo, Hi string
}

// keySpace is the number of distinct 4-hex-digit UUID prefixes. Splitting on
// 4 digits (instead of 1 or 2) allows any shard count, not just powers of 16.
const keySpace = 1 << 16

// Ranges splits the UUID space into n ranges of (almost) equal width.
// Because UUID v4 is random, equal width means roughly equal row counts.
// This does not hold for time-ordered ids such as UUID v7.
func Ranges(n int) ([]Range, error) {
	if n < 1 || n > keySpace {
		return nil, fmt.Errorf("shard count must be in [1, %d], got %d", keySpace, n)
	}
	bound := func(i int) string {
		if i == 0 || i == n {
			return ""
		}
		return fmt.Sprintf("%04x", i*keySpace/n)
	}
	ranges := make([]Range, n)
	for i := range ranges {
		ranges[i] = Range{Lo: bound(i), Hi: bound(i + 1)}
	}
	return ranges, nil
}
