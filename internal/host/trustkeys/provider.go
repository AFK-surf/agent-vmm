package trustkeys

import (
	"context"
	"errors"
)

var ErrKeyUnavailable = errors.New("device root key is unavailable")

type Provider interface {
	Ensure(context.Context, uint64) (publicKey []byte, custody string, err error)
	Sign(context.Context, uint64, []byte) ([]byte, error)
	Delete(context.Context, uint64) error
}

func Tag(revision uint64) string {
	return "surf.cue.agent-vmm.device-root.v1/" + formatRevision(revision)
}

func formatRevision(revision uint64) string {
	if revision == 0 {
		return "0"
	}
	var buffer [20]byte
	index := len(buffer)
	for revision != 0 {
		index--
		buffer[index] = byte('0' + revision%10)
		revision /= 10
	}
	return string(buffer[index:])
}
