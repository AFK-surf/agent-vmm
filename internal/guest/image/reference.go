package image

import (
	"errors"
	"strings"

	"github.com/distribution/reference"
)

func NormalizeReference(value string) (string, error) {
	named, err := reference.ParseNormalizedNamed(value)
	if err != nil || strings.Contains(value, "://") || strings.Contains(value, "@http") || strings.ToLower(named.Name()) != named.Name() {
		return "", errors.New("image reference must name a public HTTPS registry")
	}
	return reference.TagNameOnly(named).String(), nil
}
