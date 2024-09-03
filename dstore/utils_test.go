package dstore

import (
	"fmt"
	"testing"
)

func TestDeduplicateKeys(t *testing.T) {
	test := []string{"a", "b", "c", "d", "a"}
	dtk := deduplicateKeys(test)
	if (len(dtk) != 4) {
		t.Errorf("string slice should be deduplicated: %s", dtk)
	}
	test2 := []string{"a", "n"}
	dtk2 := deduplicateKeys(test2)
	if (len(dtk2) != 2) {
		t.Errorf("string slice %s has no duplications", test2)
	}
	t.Logf("after dedup: %s | %s", dtk, dtk2)
}

func BenchmarkDeduplicateKeys(b *testing.B) {
	test := []string{
		"/frodo_feed/title_vecs/3055:4601087161",
		"/frodo_feed/title_vecs/3055:4601087162",
		"/frodo_feed/title_vecs/3055:4601087161",
		"/frodo_feed/title_vecs/3055:4601087165",
		"/frodo_feed/title_vecs/3055:4601087161",
	}

	for j := 0; j < 200; j++ {
		test = append(test, fmt.Sprintf("/frodo_feed/title_vecs/3055:460108716%d", j))
	}

	for i := 0; i < b.N; i++ {
		deduplicateKeys(test)
	}
}
