package rfc4contract

import (
	"bytes"
	"io"
	"testing"
)

const benchmarkFrameBytes = 32 << 10

func BenchmarkDirectBaseline(b *testing.B) {
	payload := bytes.Repeat([]byte("d"), 8<<20)
	buffer := make([]byte, 128<<10)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for range b.N {
		reader := bytes.NewReader(payload)
		if _, err := io.CopyBuffer(io.Discard, reader, buffer); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFramedByteBridge(b *testing.B) {
	payload := bytes.Repeat([]byte("f"), 8<<20)
	frame := make([]byte, benchmarkFrameBytes)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for range b.N {
		reader := bytes.NewReader(payload)
		for {
			count, err := io.ReadFull(reader, frame)
			if count > 0 {
				if _, writeErr := io.Discard.Write(frame[:count]); writeErr != nil {
					b.Fatal(writeErr)
				}
			}
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
