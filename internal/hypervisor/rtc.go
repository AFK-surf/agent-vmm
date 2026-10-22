package hypervisor

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

const arm64RTCBase = 0x09010000

// pl031 provides the boot clock. The device tree omits an IRQ, so Linux disables alarms.
type pl031 struct {
	mu            sync.Mutex
	now           func() time.Time
	offset        int64
	match, frozen uint32
	stopped       bool
}

func (r *pl031) hostSeconds() int64 {
	if r.now != nil {
		return r.now().Unix()
	}
	return time.Now().Unix()
}
func (r *pl031) seconds() uint32 {
	if r.stopped {
		return r.frozen
	}
	return uint32(r.hostSeconds() + r.offset)
}

func (r *pl031) access(offset uint64, data []byte, write bool) error {
	if len(data) != 4 || offset%4 != 0 {
		return fmt.Errorf("PL031 register access %#x size %d", offset, len(data))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if write {
		value := binary.LittleEndian.Uint32(data)
		switch offset {
		case 4:
			r.match = value
		case 8:
			if r.stopped {
				r.frozen = value
			} else {
				r.offset = int64(value) - r.hostSeconds()
			}
		case 0xc:
			if value&1 == 0 && !r.stopped {
				r.frozen = r.seconds()
				r.stopped = true
			} else if value&1 != 0 && r.stopped {
				r.offset = int64(r.frozen) - r.hostSeconds()
				r.stopped = false
			}
		}
		return nil
	}
	var value uint32
	switch offset {
	case 0:
		value = r.seconds()
	case 4:
		value = r.match
	case 0xc:
		if !r.stopped {
			value = 1
		}
	case 0xfe0:
		value = 0x31
	case 0xfe4:
		value = 0x10
	case 0xfe8:
		value = 0x04
	case 0xff0:
		value = 0x0d
	case 0xff4:
		value = 0xf0
	case 0xff8:
		value = 0x05
	case 0xffc:
		value = 0xb1
	}
	binary.LittleEndian.PutUint32(data, value)
	return nil
}
