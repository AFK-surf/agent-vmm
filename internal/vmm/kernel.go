package vmm

import "strings"

func kernelArgument(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || strings.ContainsRune("._+-", character) {
			continue
		}
		return false
	}
	return true
}

func parseKernelCommandLine(data string) map[string]string {
	result := map[string]string{}
	for _, field := range strings.Fields(data) {
		key, value, ok := strings.Cut(field, "=")
		if ok {
			result[key] = value
		}
	}
	return result
}
