package main

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"testing"
)

func TestDecodeARM64(t *testing.T) {
	image := make([]byte, 64)
	binary.LittleEndian.PutUint32(image[56:60], 0x644d5241)
	for _, format := range []string{"raw", "gzip", "efi-stub", "invalid"} {
		t.Run(format, func(t *testing.T) {
			kernel := image
			if format != "raw" {
				var b bytes.Buffer
				if format == "efi-stub" {
					b.Write(make([]byte, 4096))
				}
				payload := image
				if format == "invalid" {
					payload = image[:32]
				}
				z := gzip.NewWriter(&b)
				_, _ = z.Write(payload)
				_ = z.Close()
				kernel = b.Bytes()
			}
			got, err := decodeARM64(kernel)
			if format == "invalid" {
				if err == nil {
					t.Fatal("accepted payload without ARM64 magic")
				}
				return
			}
			if err != nil || !bytes.Equal(got, image) {
				t.Fatalf("image=%x err=%v", got, err)
			}
		})
	}
}
