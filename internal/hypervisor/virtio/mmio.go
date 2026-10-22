// Portions derived from tinyrange/cc, commit eed53ffb414a8bebcbb1d7934fab00558d78df00.
// Licensed under the Apache License, Version 2.0; see ../LICENSE-cc.txt.
package virtio

import (
	"encoding/binary"
	"fmt"
	"sync"
)

const (
	regMagicValue      = 0x000
	regVersion         = 0x004
	regDeviceID        = 0x008
	regVendorID        = 0x00c
	regDeviceFeatures  = 0x010
	regDeviceFeatSel   = 0x014
	regDriverFeatures  = 0x020
	regDriverFeatSel   = 0x024
	regQueueSel        = 0x030
	regQueueNumMax     = 0x034
	regQueueNum        = 0x038
	regQueueReady      = 0x044
	regQueueNotify     = 0x050
	regInterruptStatus = 0x060
	regInterruptAck    = 0x064
	regStatus          = 0x070
	regQueueDescLow    = 0x080
	regQueueDescHigh   = 0x084
	regQueueAvailLow   = 0x090
	regQueueAvailHigh  = 0x094
	regQueueUsedLow    = 0x0a0
	regQueueUsedHigh   = 0x0a4
	regConfigGen       = 0x0fc
	regConfig          = 0x100
	featureVersion1    = uint64(1) << 32
	statusDriverOK     = 4
	statusFeaturesOK   = 8
	statusNeedsReset   = 64
)

type response struct {
	data   []byte
	status *byte
	used   uint32
}

type transport struct {
	mu                                                              sync.Mutex
	mem                                                             GuestMemory
	irq                                                             func(bool) error
	deviceID                                                        uint32
	features, driverFeatures                                        uint64
	deviceFeatureSel, driverFeatureSel, queueSel, status, interrupt uint32
	generation                                                      uint64
	queues                                                          []queue
	config                                                          []byte
	process                                                         func(int, *chain) (response, bool, error)
	reset                                                           func()
	wake                                                            chan struct{}
	done                                                            chan struct{}
	finished                                                        chan struct{}
	closed                                                          bool
	err                                                             error
}

func newTransport(mem GuestMemory, irq func(bool) error, id uint32, features uint64, queues int, config []byte) *transport {
	return &transport{mem: mem, irq: irq, deviceID: id, features: features | featureVersion1, queues: make([]queue, queues), config: config, wake: make(chan struct{}, 1), done: make(chan struct{}), finished: make(chan struct{})}
}

func (t *transport) start() { go t.run() }
func (t *transport) kick() {
	select {
	case t.wake <- struct{}{}:
	default:
	}
}
func (t *transport) Close() error {
	t.mu.Lock()
	if !t.closed {
		t.closed = true
		close(t.done)
	}
	t.mu.Unlock()
	<-t.finished
	return nil
}
func (t *transport) Err() error { t.mu.Lock(); defer t.mu.Unlock(); return t.err }

func (t *transport) Read(offset uint64, data []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if offset >= regConfig {
		p := offset - regConfig
		if p > uint64(len(t.config)) || uint64(len(data)) > uint64(len(t.config))-p {
			clear(data)
			return nil
		}
		copy(data, t.config[p:])
		return nil
	}
	if len(data) != 4 || offset%4 != 0 {
		return fmt.Errorf("virtio register read %#x size %d", offset, len(data))
	}
	var v uint32
	q := t.selectedQueue()
	switch offset {
	case regMagicValue:
		v = 0x74726976
	case regVersion:
		v = 2
	case regDeviceID:
		v = t.deviceID
	case regVendorID:
		v = 0x554d4551
	case regDeviceFeatures:
		if t.deviceFeatureSel < 2 {
			v = uint32(t.features >> (t.deviceFeatureSel * 32))
		}
	case regQueueNumMax:
		if q != nil {
			v = maxQueueSize
		}
	case regQueueNum:
		if q != nil {
			v = uint32(q.size)
		}
	case regQueueReady:
		if q != nil && q.ready {
			v = 1
		}
	case regInterruptStatus:
		v = t.interrupt
	case regStatus:
		v = t.status
	case regConfigGen:
		v = uint32(t.generation)
	}
	binary.LittleEndian.PutUint32(data, v)
	return nil
}

func (t *transport) Write(offset uint64, data []byte) error {
	if len(data) != 4 || offset%4 != 0 {
		return fmt.Errorf("virtio register write %#x size %d", offset, len(data))
	}
	v := binary.LittleEndian.Uint32(data)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	q := t.selectedQueue()
	switch offset {
	case regDeviceFeatSel:
		t.deviceFeatureSel = v
	case regDriverFeatSel:
		t.driverFeatureSel = v
	case regDriverFeatures:
		if t.driverFeatureSel < 2 {
			shift := t.driverFeatureSel * 32
			t.driverFeatures = (t.driverFeatures &^ (uint64(0xffffffff) << shift)) | uint64(v)<<shift
		}
	case regQueueSel:
		t.queueSel = v
	case regQueueNum:
		if q != nil && !q.ready {
			if v == 0 || v > maxQueueSize || v&(v-1) != 0 {
				return t.fail(fmt.Errorf("invalid queue size %d", v))
			}
			q.size = uint16(v)
		}
	case regQueueReady:
		if q != nil {
			if v == 1 && !q.ready {
				if err := q.validate(t.mem); err != nil {
					return t.fail(err)
				}
				q.ready = true
			} else if v == 0 && q.ready {
				return t.fail(fmt.Errorf("queue reset feature not negotiated"))
			}
		}
	case regQueueDescLow, regQueueDescHigh, regQueueAvailLow, regQueueAvailHigh, regQueueUsedLow, regQueueUsedHigh:
		if q != nil && !q.ready {
			var addr *uint64
			switch offset {
			case regQueueDescLow, regQueueDescHigh:
				addr = &q.desc
			case regQueueAvailLow, regQueueAvailHigh:
				addr = &q.avail
			default:
				addr = &q.used
			}
			if offset&4 == 0 {
				*addr = (*addr &^ 0xffffffff) | uint64(v)
			} else {
				*addr = (*addr & 0xffffffff) | (uint64(v) << 32)
			}
		}
	case regInterruptAck:
		t.interrupt &^= v
		return t.updateIRQ()
	case regStatus:
		if v == 0 {
			t.generation++
			t.status = 0
			t.interrupt = 0
			t.driverFeatures = 0
			t.deviceFeatureSel = 0
			t.driverFeatureSel = 0
			t.queueSel = 0
			clear(t.queues)
			t.err = nil
			if t.reset != nil {
				t.reset()
			}
			return t.updateIRQ()
		}
		t.status = v
		if v&statusFeaturesOK != 0 && (t.driverFeatures & ^t.features != 0 || t.driverFeatures&featureVersion1 == 0) {
			t.status &^= statusFeaturesOK
		}
		if v&statusDriverOK != 0 && t.status&statusFeaturesOK == 0 {
			return t.fail(fmt.Errorf("driver ready before feature negotiation"))
		}
		t.kick()
	case regQueueNotify:
		if v < uint32(len(t.queues)) {
			t.kick()
		}
	}
	return nil
}

func (t *transport) selectedQueue() *queue {
	if t.queueSel >= uint32(len(t.queues)) {
		return nil
	}
	return &t.queues[t.queueSel]
}
func (t *transport) updateIRQ() error {
	if t.irq != nil {
		return t.irq(t.interrupt != 0)
	}
	return nil
}
func (t *transport) fail(err error) error {
	t.err = err
	t.status |= statusNeedsReset
	t.interrupt |= 2
	return t.updateIRQ()
}

func (t *transport) run() {
	defer close(t.finished)
	for {
		select {
		case <-t.done:
			return
		case <-t.wake:
		}
		for i := range t.queues {
			for processed := 0; processed < maxQueueSize; processed++ {
				t.mu.Lock()
				if t.closed {
					t.mu.Unlock()
					return
				}
				q := &t.queues[i]
				if t.status&statusDriverOK == 0 || t.status&statusNeedsReset != 0 || !q.ready {
					t.mu.Unlock()
					break
				}
				c, err := q.next(t.mem)
				if err == nil && c != nil {
					c.request, err = c.readable(t.mem)
				}
				if err != nil {
					_ = t.fail(err)
					t.mu.Unlock()
					break
				}
				if c == nil {
					t.mu.Unlock()
					break
				}
				generation := t.generation
				c.generation = generation
				t.mu.Unlock()
				r, consume, err := t.process(i, c)
				t.mu.Lock()
				if t.closed {
					t.mu.Unlock()
					return
				}
				if generation != t.generation {
					t.mu.Unlock()
					break
				}
				if err == nil && consume {
					if len(r.data) > 0 {
						err = c.write(t.mem, r.data)
					}
					if err == nil && r.status != nil {
						err = t.mem.WriteIPA(c.buffers[len(c.buffers)-1].addr, []byte{*r.status})
					}
					if err == nil {
						err = q.complete(t.mem, c, r.used)
					}
					if err == nil {
						t.interrupt |= 1
						err = t.updateIRQ()
					}
				}
				if err != nil {
					_ = t.fail(err)
				}
				t.mu.Unlock()
				if err != nil || !consume {
					break
				}
				if processed == maxQueueSize-1 {
					t.kick()
				}
			}
		}
	}
}
