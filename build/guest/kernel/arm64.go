package main

// Derived from tinyrange/cc eed53ffb414a8bebcbb1d7934fab00558d78df00.
// Modified for agent-vmm; see ../../../internal/hypervisor/LICENSE-cc.txt.

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
)

// decodeARM64 decodes a raw Image or the gzip payload in an EFI-stub kernel.
func decodeARM64(kernel []byte) ([]byte, error) {
	valid := func(b []byte) bool { return len(b) >= 64 && binary.LittleEndian.Uint32(b[56:60]) == 0x644d5241 }
	if valid(kernel) {
		return kernel, nil
	}
	// Alpine's EFI-stub kernel contains a gzip-compressed Image in its prefix.
	scan := kernel[:min(len(kernel), 1<<20)]
	for offset := 0; offset < len(scan); offset++ {
		n := bytes.Index(scan[offset:], []byte{0x1f, 0x8b, 0x08})
		if n < 0 {
			break
		}
		offset += n
		gz, err := gzip.NewReader(bytes.NewReader(kernel[offset:]))
		if err != nil {
			continue
		}
		gz.Multistream(false)
		image, err := io.ReadAll(gz)
		if err == nil && valid(image) {
			return image, nil
		}
	}
	return nil, fmt.Errorf("kernel contains no supported ARM64 Image")
}
