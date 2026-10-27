package imageref

import (
	"errors"
	"github.com/distribution/reference"
)

func NormalizeReference(value string) (string, error) {
	named, err := reference.ParseNormalizedNamed(value)
	if err != nil {
		return "", errors.New("invalid image reference")
	}
	return reference.TagNameOnly(named).String(), nil
}
