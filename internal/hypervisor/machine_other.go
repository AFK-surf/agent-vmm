//go:build !linux || (!arm64 && !amd64)

package hypervisor

import "context"

type machine struct{}

func newMachine(Config) (*machine, error)      { return nil, ErrUnsupported }
func (*machine) setDevices([]MMIODevice) error { return ErrUnsupported }
func (*machine) memory() []byte                { return nil }
func (*machine) memoryBase() uint64            { return 0 }
func (*machine) setIRQ(uint32, bool) error     { return ErrUnsupported }
func (*machine) run(context.Context) error     { return ErrUnsupported }
func (*machine) close() error                  { return nil }
func (*machine) serialLoss() (uint64, uint64)  { return 0, 0 }
