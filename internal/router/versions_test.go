package router

import "testing"

// The fourth element of a versionArray is a build counter and says nothing
// about what the server understands, so an archive that differs only there is
// not older.
func TestCompareVersions(t *testing.T) {
	cases := []struct {
		name string
		a, b []int32
		want int
	}{
		{"the same", []int32{8, 0, 4, 0}, []int32{8, 0, 4, 0}, 0},
		{"differing only in the build counter", []int32{7, 0, 5, 1}, []int32{7, 0, 5, 9}, 0},
		{"an older patch", []int32{7, 0, 2, 0}, []int32{7, 0, 5, 0}, -1},
		{"an older minor", []int32{7, 0, 9, 0}, []int32{7, 1, 0, 0}, -1},
		{"an older major", []int32{6, 3, 0, 0}, []int32{8, 0, 0, 0}, -1},
		{"a newer major", []int32{8, 0, 0, 0}, []int32{7, 0, 0, 0}, 1},
		{"a short array", []int32{8}, []int32{8, 0, 0, 0}, 0},
		{"a short older array", []int32{7}, []int32{8, 0, 0, 0}, -1},
		{"both empty", nil, nil, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := compareVersions(c.a, c.b); got != c.want {
				t.Errorf("compareVersions(%v, %v) = %d, want %d", c.a, c.b, got, c.want)
			}
		})
	}
}
