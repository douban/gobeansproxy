package dstore

func deduplicateKeys(keys []string) []string {
	dedup := make(map[string]struct{}, len(keys))

	for _, k := range keys {
		if _, ok := dedup[k]; ok {
			continue
		} else {
			dedup[k] = struct{}{}
		}
	}

	dedupKs := make([]string, len(dedup))
	i := 0
	for k := range dedup {
		dedupKs[i] = k
		i++
	}
	return dedupKs
}
