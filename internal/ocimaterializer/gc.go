package ocimaterializer

import (
	"sort"
	"time"
)

type ContentFact struct {
	Digest, Namespace string
	UpdatedAt         time.Time
}

func SelectUnreferenced(facts []ContentFact, references map[string]bool, limit int) []ContentFact {
	unique := make(map[string]ContentFact)
	for _, fact := range facts {
		if fact.Digest == "" || references[fact.Digest] {
			continue
		}
		if current, ok := unique[fact.Digest]; !ok || fact.UpdatedAt.Before(current.UpdatedAt) {
			unique[fact.Digest] = fact
		}
	}
	result := make([]ContentFact, 0, len(unique))
	for _, fact := range unique {
		result = append(result, fact)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].UpdatedAt.Before(result[j].UpdatedAt) })
	if limit >= 0 && len(result) > limit {
		result = result[:limit]
	}
	return result
}
