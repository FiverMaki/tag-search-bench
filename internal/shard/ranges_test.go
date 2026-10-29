package shard

import "testing"

func TestRanges16(t *testing.T) {
	got, err := Ranges(16)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 16 {
		t.Fatalf("len = %d, want 16", len(got))
	}
	if got[0].Lo != "" || got[15].Hi != "" {
		t.Errorf("outer bounds should be open: first=%+v last=%+v", got[0], got[15])
	}
	if got[0].Hi != "1000" || got[1].Lo != "1000" || got[15].Lo != "f000" {
		t.Errorf("unexpected bounds: %+v", got)
	}
}

func TestRangesContiguous(t *testing.T) {
	for _, n := range []int{1, 3, 7, 16, 100, 256, 1000} {
		got, err := Ranges(n)
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i < len(got); i++ {
			if got[i-1].Hi != got[i].Lo {
				t.Fatalf("n=%d: gap between %d and %d: %+v %+v", n, i-1, i, got[i-1], got[i])
			}
			if got[i].Lo != "" && got[i].Hi != "" && got[i].Lo >= got[i].Hi {
				t.Fatalf("n=%d: empty range %d: %+v", n, i, got[i])
			}
		}
	}
}

func TestRangesInvalid(t *testing.T) {
	for _, n := range []int{0, -1, keySpace + 1} {
		if _, err := Ranges(n); err == nil {
			t.Errorf("Ranges(%d) should fail", n)
		}
	}
}
