package virtio

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	blockRead        = 0
	blockWrite       = 1
	blockFlush       = 4
	blockGetID       = 8
	blockOK          = 0
	blockIOError     = 1
	blockUnsupported = 2
)

type Block struct {
	*transport
	file     *os.File
	capacity uint64
	readOnly bool
	id       [20]byte
}

// NewBlock borrows disk. The caller must close the device before closing disk or RAM.
func NewBlock(mem GuestMemory, irq func(bool) error, disk *os.File, readOnly bool, id string) (*Block, error) {
	if disk == nil {
		return nil, fmt.Errorf("block device has no disk")
	}
	info, err := disk.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < 512 {
		return nil, fmt.Errorf("block disk is smaller than one sector")
	}
	if len(id) > 20 {
		return nil, fmt.Errorf("block ID exceeds 20 bytes")
	}
	b := &Block{file: disk, capacity: uint64(info.Size()/512) * 512, readOnly: readOnly}
	copy(b.id[:], id)
	config := make([]byte, 24)
	binary.LittleEndian.PutUint64(config, b.capacity/512)
	// One data segment bounds a request without advertising unsupported indirect chains.
	binary.LittleEndian.PutUint32(config[8:], 128<<10)
	binary.LittleEndian.PutUint32(config[12:], 1)
	features := uint64(1<<1 | 1<<2 | 1<<9)
	if readOnly {
		features |= 1 << 5
	}
	b.transport = newTransport(mem, irq, 2, features, 1, config)
	b.process = b.processRequest
	b.start()
	return b, nil
}

func (b *Block) processRequest(_ int, c *chain) (response, bool, error) {
	if len(c.buffers) < 2 {
		return response{}, false, fmt.Errorf("block request has no status descriptor")
	}
	last := c.buffers[len(c.buffers)-1]
	if !last.write || last.length < 1 {
		return response{}, false, fmt.Errorf("block status descriptor is not writable")
	}
	status := byte(blockIOError)
	r := response{status: &status, used: 1}
	if len(c.request) < 16 {
		return r, true, nil
	}
	typ := binary.LittleEndian.Uint32(c.request)
	sector := binary.LittleEndian.Uint64(c.request[8:])
	dataSize := c.writableBytes() - int(last.length)
	var err error
	switch typ {
	case blockRead:
		if len(c.request) != 16 || dataSize%512 != 0 || !b.inRange(sector, dataSize) {
			return r, true, nil
		}
		r.data = make([]byte, dataSize)
		_, err = io.ReadFull(io.NewSectionReader(b.file, int64(sector*512), int64(dataSize)), r.data)
		if err == nil {
			r.used += uint32(dataSize)
		} else {
			r.data = nil
		}
	case blockWrite:
		data := c.request[16:]
		if b.readOnly || dataSize != 0 || len(data)%512 != 0 || !b.inRange(sector, len(data)) {
			return r, true, nil
		}
		var n int
		n, err = b.file.WriteAt(data, int64(sector*512))
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
	case blockFlush:
		if len(c.request) != 16 || dataSize != 0 {
			return r, true, nil
		}
		err = b.file.Sync()
	case blockGetID:
		if len(c.request) != 16 || dataSize < len(b.id) {
			return r, true, nil
		}
		r.data = append([]byte(nil), b.id[:]...)
		r.used += 20
	default:
		status = blockUnsupported
		return r, true, nil
	}
	if err == nil {
		status = blockOK
	}
	return r, true, nil
}

func (b *Block) inRange(sector uint64, size int) bool {
	return sector <= b.capacity/512 && uint64(size) <= b.capacity-sector*512
}
