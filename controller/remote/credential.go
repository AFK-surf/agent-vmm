package remote

import (
	"encoding/base64"
	"errors"
	"strings"
)

const bearerPrefix = "Bearer "

// EncodeBearer is the one remote-controller credential wire encoding.
func EncodeBearer(credential []byte) string {
	return bearerPrefix + base64.StdEncoding.EncodeToString(credential)
}

// DecodeBearer rejects alternate alphabets, missing padding, and non-Bearer
// schemes so connector and gateway cannot drift to different credential wire
// formats.
func DecodeBearer(value string) ([]byte, error) {
	if !strings.HasPrefix(value, bearerPrefix) {
		return nil, errors.New("bearer credential required")
	}
	return base64.StdEncoding.Strict().DecodeString(strings.TrimPrefix(value, bearerPrefix))
}
