package vmmprocess

// IRQs are free ISA GSIs: 4 is the UART, 8 the RTC, and 13 the FPU.
const mmioBase = 0xc0000000

var deviceIRQs = [...]uint32{5, 6, 7, 9, 10}
