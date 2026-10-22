// Portions derived from tinyrange/cc, commit eed53ffb414a8bebcbb1d7934fab00558d78df00.
// Licensed under the Apache License, Version 2.0; see ../LICENSE-cc.txt.
package virtio

import (
	"crypto/rand"
	"fmt"
)

type RNG struct{ *transport }

func NewRNG(mem GuestMemory, irq func(bool) error) *RNG {
	r := &RNG{newTransport(mem, irq, 4, 0, 1, nil)}
	r.process = func(_ int, c *chain) (response, bool, error) {
		if len(c.request) != 0 {
			return response{}, false, fmt.Errorf("rng descriptor is not writable")
		}
		data := make([]byte, c.writableBytes())
		rand.Read(data)
		return response{data: data, used: uint32(len(data))}, true, nil
	}
	r.start()
	return r
}
