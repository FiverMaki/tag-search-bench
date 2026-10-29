package strategy

import (
	"slices"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
)

func TestUUIDRoundTrip(t *testing.T) {
	const s = "3f2a9c1e-7b4d-4e8a-9c3f-0123456789ab"
	b, err := parseUUID(s)
	if err != nil {
		t.Fatal(err)
	}
	if got := formatUUID(b); got != s {
		t.Errorf("round trip = %q, want %q", got, s)
	}
	for _, bad := range []string{"", "3f2a9c1e", "3f2a9c1e-7b4d-4e8a-9c3f-0123456789zz"} {
		if _, err := parseUUID(bad); err == nil {
			t.Errorf("parseUUID(%q) should fail", bad)
		}
	}
}

func TestMatch(t *testing.T) {
	b := &Bitmap{
		tags: map[int]*roaring.Bitmap{
			1: roaring.BitmapOf(0, 1, 2, 3, 4),
			2: roaring.BitmapOf(1, 3, 5),
			3: roaring.BitmapOf(3),
		},
		regions: map[int]*roaring.Bitmap{
			0: roaring.BitmapOf(0, 1),
			1: roaring.BitmapOf(2, 3, 4, 5),
		},
	}
	tests := []struct {
		q    Query
		want []uint32
	}{
		{Query{Tags: []int{1}, Region: -1}, []uint32{0, 1, 2, 3, 4}},
		{Query{Tags: []int{1, 2}, Region: -1}, []uint32{1, 3}},
		{Query{Tags: []int{1, 2, 3}, Region: -1}, []uint32{3}},
		{Query{Tags: []int{1, 2}, Region: 0}, []uint32{1}},
		{Query{Tags: []int{1, 99}, Region: -1}, nil},
		{Query{Tags: []int{1}, Region: 7}, nil},
	}
	for _, tt := range tests {
		got := b.Match(tt.q).ToArray()
		if !slices.Equal(got, tt.want) && !(len(got) == 0 && len(tt.want) == 0) {
			t.Errorf("Match(%+v) = %v, want %v", tt.q, got, tt.want)
		}
	}
	// Match must not modify the index.
	if b.tags[3].GetCardinality() != 1 || b.tags[1].GetCardinality() != 5 {
		t.Error("Match mutated an index bitmap")
	}
}
