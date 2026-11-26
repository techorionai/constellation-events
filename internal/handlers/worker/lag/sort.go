package lag

import "slices"

// sortKeys sorts the keys in the order of stream and group.
func sortKeys(keys []groupKey) {
	slices.SortFunc(keys, func(a, b groupKey) int {
		if a.stream != b.stream {
			if a.stream < b.stream {
				return -1
			}
			return 1
		}
		switch {
		case a.group < b.group:
			return -1
		case a.group > b.group:
			return 1
		}
		return 0
	})
}
