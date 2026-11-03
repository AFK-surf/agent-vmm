package remoteconnector

func containsAllFeatures(enabled, required []string) bool {
	available := make(map[string]struct{}, len(enabled))
	for _, feature := range enabled {
		available[feature] = struct{}{}
	}
	for _, feature := range required {
		if _, ok := available[feature]; !ok {
			return false
		}
	}
	return true
}

func boundedLabel(value string) string {
	if len(value) > MaxLabelBytes {
		return value[:MaxLabelBytes]
	}
	return value
}
