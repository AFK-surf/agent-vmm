package vmmprocess

// Virtio MMIO windows follow the arm64 platform's device range; IRQs are SPIs.
const mmioBase = 0x0a000000

var deviceIRQs = [...]uint32{32, 33, 34, 35, 36}
