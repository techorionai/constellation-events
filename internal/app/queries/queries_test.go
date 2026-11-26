package queries_test

import (
	"testing"

	"github.com/techorionai/constellation-events/internal/app/queries"
)

func ident(n int) int { return n }

func repeat(n, size int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = size
	}
	return out
}

// U12: the page builder ends a page before the item that makes the body larger
// than 1 MiB, and always has the first item.
func TestU12_BuildPage(t *testing.T) {
	cases := []struct {
		name     string
		items    []int
		pageSize int
		want     int
		more     bool
	}{
		{"items of 400 KiB", repeat(51, 400<<10), 50, 2, true},
		{"one item of 2 MiB", append([]int{2 << 20}, repeat(50, 1<<10)...), 50, 1, true},
		{"60 items of 1 KiB", repeat(51, 1<<10), 50, 50, true},
		{"last page", repeat(3, 1<<10), 50, 3, false},
		{"exactly the page size", repeat(50, 1<<10), 50, 50, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			page, more := queries.BuildPage(tc.items, tc.pageSize, ident)
			if len(page) != tc.want || more != tc.more {
				t.Fatalf("page = %d items, more = %v, want %d and %v", len(page), more, tc.want, tc.more)
			}
		})
	}
}
