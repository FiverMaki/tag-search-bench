package strategy

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"

	"tagbench/internal/shard"
)

// buildShards is how many UUID ranges Build loads in parallel.
const buildShards = 64

// Bitmap keeps one Roaring Bitmap per tag (and per region) in memory, keyed
// by users.seq. A query is an AND over the requested bitmaps.
//
// The index is a snapshot taken by Build; later writes to TiDB are not seen.
type Bitmap struct {
	DB      *sql.DB
	Workers int // Build concurrency

	ids     [][16]byte // seq -> user id; 16 bytes instead of a 36-byte string
	tags    map[int]*roaring.Bitmap
	regions map[int]*roaring.Bitmap
}

func (b *Bitmap) Name() string { return "bitmap" }

// Build loads users and user_tags range by range in parallel. Each range
// produces its own partial bitmaps, merged with OR at the end, so workers
// never share a bitmap.
func (b *Bitmap) Build(ctx context.Context) error {
	var maxSeq sql.NullInt64
	if err := b.DB.QueryRowContext(ctx, "SELECT MAX(seq) FROM users").Scan(&maxSeq); err != nil {
		return err
	}
	if !maxSeq.Valid {
		return errors.New("users table is empty")
	}
	b.ids = make([][16]byte, maxSeq.Int64+1)

	ranges, err := shard.Ranges(buildShards)
	if err != nil {
		return err
	}
	type partial struct{ tags, regions map[int]*roaring.Bitmap }
	parts := make([]partial, len(ranges))
	err = runRanges(ctx, ranges, max(b.Workers, 1), func(ctx context.Context, i int, r shard.Range) error {
		p := partial{tags: map[int]*roaring.Bitmap{}, regions: map[int]*roaring.Bitmap{}}
		if err := b.loadUsers(ctx, r, p.regions); err != nil {
			return fmt.Errorf("load users: %w", err)
		}
		if err := b.loadTags(ctx, r, p.tags); err != nil {
			return fmt.Errorf("load user_tags: %w", err)
		}
		parts[i] = p
		return nil
	})
	if err != nil {
		return err
	}

	b.tags = map[int]*roaring.Bitmap{}
	b.regions = map[int]*roaring.Bitmap{}
	for _, p := range parts {
		mergeInto(b.tags, p.tags)
		mergeInto(b.regions, p.regions)
	}
	for _, bm := range b.tags {
		bm.RunOptimize()
	}
	for _, bm := range b.regions {
		bm.RunOptimize()
	}
	return nil
}

// loadUsers fills b.ids for the range. Ranges are disjoint, so each seq is
// written by exactly one worker and the shared slice needs no lock.
func (b *Bitmap) loadUsers(ctx context.Context, r shard.Range, regions map[int]*roaring.Bitmap) error {
	where, args := rangeWhere(r, "id")
	rows, err := b.DB.QueryContext(ctx, "SELECT seq, id, region FROM users"+where, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			seq    uint32
			id     string
			region int
		)
		if err := rows.Scan(&seq, &id, &region); err != nil {
			return err
		}
		if int(seq) >= len(b.ids) {
			return fmt.Errorf("seq %d out of range (users changed during build?)", seq)
		}
		if b.ids[seq], err = parseUUID(id); err != nil {
			return err
		}
		bitmapFor(regions, region).Add(seq)
	}
	return rows.Err()
}

func (b *Bitmap) loadTags(ctx context.Context, r shard.Range, tags map[int]*roaring.Bitmap) error {
	where, args := rangeWhere(r, "u.id", "t.user_id")
	rows, err := b.DB.QueryContext(ctx,
		"SELECT u.seq, t.tag_id FROM users u JOIN user_tags t ON t.user_id = u.id"+where, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			seq uint32
			tag int
		)
		if err := rows.Scan(&seq, &tag); err != nil {
			return err
		}
		bitmapFor(tags, tag).Add(seq)
	}
	return rows.Err()
}

// Match returns the seqs of matching users. This is the whole query; Run
// only adds the seq -> id conversion.
func (b *Bitmap) Match(q Query) *roaring.Bitmap {
	bms := make([]*roaring.Bitmap, 0, len(q.Tags)+1)
	for _, tag := range q.Tags {
		bm, ok := b.tags[tag]
		if !ok {
			return roaring.New()
		}
		bms = append(bms, bm)
	}
	if q.Region >= 0 {
		bm, ok := b.regions[q.Region]
		if !ok {
			return roaring.New()
		}
		bms = append(bms, bm)
	}
	// Start from the smallest bitmap: every AND can only shrink the result,
	// so the intermediate results stay as small as possible.
	slices.SortFunc(bms, func(x, y *roaring.Bitmap) int {
		return compareUint64(x.GetCardinality(), y.GetCardinality())
	})
	res := bms[0].Clone()
	for _, bm := range bms[1:] {
		if res.IsEmpty() {
			break
		}
		res.And(bm)
	}
	return res
}

func (b *Bitmap) Run(ctx context.Context, q Query) (Result, error) {
	m := b.Match(q)
	ids := make([]string, 0, m.GetCardinality())
	it := m.Iterator()
	for it.HasNext() {
		ids = append(ids, formatUUID(b.ids[it.Next()]))
	}
	return Result{UserIDs: ids}, nil
}

// Stats describes the in-memory index size.
func (b *Bitmap) Stats() string {
	var bytes uint64
	for _, bm := range b.tags {
		bytes += bm.GetSizeInBytes()
	}
	for _, bm := range b.regions {
		bytes += bm.GetSizeInBytes()
	}
	return fmt.Sprintf("%d users, %d tag bitmaps, %d region bitmaps; bitmaps %.1f MB, id table %.1f MB",
		len(b.ids), len(b.tags), len(b.regions), float64(bytes)/(1<<20), float64(len(b.ids)*16)/(1<<20))
}

func bitmapFor(m map[int]*roaring.Bitmap, key int) *roaring.Bitmap {
	bm, ok := m[key]
	if !ok {
		bm = roaring.New()
		m[key] = bm
	}
	return bm
}

func mergeInto(dst, src map[int]*roaring.Bitmap) {
	for k, bm := range src {
		if d, ok := dst[k]; ok {
			d.Or(bm)
		} else {
			dst[k] = bm
		}
	}
}

func compareUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func parseUUID(s string) ([16]byte, error) {
	var out [16]byte
	if len(s) != 36 {
		return out, fmt.Errorf("bad uuid %q", s)
	}
	var hexDigits [32]byte
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '-' {
			continue
		}
		if n == len(hexDigits) {
			return out, fmt.Errorf("bad uuid %q", s)
		}
		hexDigits[n] = s[i]
		n++
	}
	if _, err := hex.Decode(out[:], hexDigits[:n]); err != nil || n != 32 {
		return out, fmt.Errorf("bad uuid %q", s)
	}
	return out, nil
}

func formatUUID(b [16]byte) string {
	var buf [36]byte
	hex.Encode(buf[0:8], b[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], b[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], b[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], b[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], b[10:16])
	return string(buf[:])
}
