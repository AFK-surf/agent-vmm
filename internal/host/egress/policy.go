package egress

import (
	"context"
	"errors"
	"net/netip"
	"sort"
	"sync/atomic"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/netprotocol"
)

type Mode uint8

const (
	DenyAll Mode = iota
	PublicInternet
)

type Source struct {
	EnvironmentID string
	NamespaceID   string
	Subnet        netip.Prefix
	Mode          Mode
}

type Snapshot struct {
	BootID   string
	Revision uint64
	Sources  []Source
}

type policyState struct {
	bootID   string
	revision uint64
	sources  []Source
}

type Policies struct {
	state atomic.Pointer[policyState]
}

func (policies *Policies) Publish(snapshot Snapshot) error {
	if snapshot.BootID == "" || snapshot.Revision == 0 {
		return errors.New("boot ID and policy revision are required")
	}
	sources := append([]Source(nil), snapshot.Sources...)
	sort.Slice(sources, func(i, j int) bool { return sources[i].Subnet.Addr().Less(sources[j].Subnet.Addr()) })
	for index, source := range sources {
		if source.EnvironmentID == "" || source.NamespaceID == "" || !source.Subnet.IsValid() || !source.Subnet.Addr().Is4() || source.Subnet.Bits() != 24 || source.Subnet != source.Subnet.Masked() {
			return errors.New("invalid source policy")
		}
		if index > 0 && sources[index-1].Subnet.Overlaps(source.Subnet) {
			return errors.New("overlapping source policies")
		}
	}
	current := policies.state.Load()
	if current != nil && current.bootID == snapshot.BootID && snapshot.Revision <= current.revision {
		return errors.New("stale policy revision")
	}
	policies.state.Store(&policyState{bootID: snapshot.BootID, revision: snapshot.Revision, sources: sources})
	return nil
}

func (policies *Policies) Clear() { policies.state.Store(nil) }

func (policies *Policies) Resolve(bootID string, address netip.Addr) (Source, bool) {
	state := policies.state.Load()
	if state == nil || state.bootID != bootID {
		return Source{}, false
	}
	var result Source
	found := false
	for _, source := range state.sources {
		if source.Subnet.Contains(address) {
			if found {
				return Source{}, false
			}
			result, found = source, true
		}
	}
	return result, found && result.Mode == PublicInternet
}

var deniedDestinations = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"), netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.31.196.0/24"), netip.MustParsePrefix("192.52.193.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("192.175.48.0/24"), netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
}

var transparentTest = netip.MustParsePrefix("198.18.0.0/15")

func DestinationAllowed(address netip.Addr, namespaceSubnets []netip.Prefix, allowTransparentTest bool) bool {
	if !address.Is4() || address.IsUnspecified() || address.IsMulticast() {
		return false
	}
	guest, host, prefix := netprotocol.Addresses()
	if address == guest || address == host || prefix.Contains(address) {
		return false
	}
	if transparentTest.Contains(address) {
		return allowTransparentTest
	}
	for _, blocked := range deniedDestinations {
		if blocked.Contains(address) {
			return false
		}
	}
	for _, subnet := range namespaceSubnets {
		if subnet.Contains(address) {
			return false
		}
	}
	return true
}

type Lease struct {
	id       uint64
	deadline time.Time
	owner    *Leases
}

func (lease *Lease) Release() {
	if lease != nil && lease.owner != nil {
		lease.owner.release(lease.id)
		lease.owner = nil
	}
}

type Leases struct {
	state atomic.Pointer[leaseState]
	next  atomic.Uint64
}

type leaseState struct {
	deadlines map[uint64]time.Time
	changed   chan struct{}
}

func (leases *Leases) Acquire(deadline time.Time) (*Lease, error) {
	if deadline.IsZero() || !deadline.After(time.Now()) {
		return nil, errors.New("lease deadline must be in the future")
	}
	for {
		current := leases.state.Load()
		deadlines := make(map[uint64]time.Time)
		if current != nil {
			for id, value := range current.deadlines {
				if value.After(time.Now()) {
					deadlines[id] = value
				}
			}
		}
		if len(deadlines) >= MaxInfrastructureLeases {
			return nil, errors.New("infrastructure lease limit reached")
		}
		id := leases.next.Add(1)
		deadlines[id] = deadline
		next := &leaseState{deadlines: deadlines, changed: make(chan struct{})}
		if leases.state.CompareAndSwap(current, next) {
			if current != nil {
				close(current.changed)
			}
			return &Lease{id: id, deadline: deadline, owner: leases}, nil
		}
	}
}

func (leases *Leases) Context(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	state := leases.state.Load()
	var leaseID uint64
	var deadline time.Time
	if state != nil {
		for id, candidate := range state.deadlines {
			if candidate.After(time.Now()) && (leaseID == 0 || id < leaseID) {
				leaseID = id
				deadline = candidate
			}
		}
	}
	if leaseID == 0 {
		cancel()
		return ctx, cancel
	}
	go func() {
		for {
			state = leases.state.Load()
			current, active := time.Time{}, false
			if state != nil {
				current, active = state.deadlines[leaseID]
			}
			if !active || current != deadline || !deadline.After(time.Now()) {
				cancel()
				return
			}
			timer := time.NewTimer(time.Until(deadline))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-state.changed:
				timer.Stop()
			case <-timer.C:
			}
		}
	}()
	return ctx, cancel
}

func (leases *Leases) Active(now time.Time) bool {
	current := leases.state.Load()
	if current == nil {
		return false
	}
	for _, deadline := range current.deadlines {
		if deadline.After(now) {
			return true
		}
	}
	return false
}

func (leases *Leases) Clear() {
	if current := leases.state.Swap(nil); current != nil {
		close(current.changed)
	}
}

func (leases *Leases) release(id uint64) {
	for {
		current := leases.state.Load()
		if current == nil {
			return
		}
		deadlines := make(map[uint64]time.Time, len(current.deadlines))
		for key, value := range current.deadlines {
			if key != id {
				deadlines[key] = value
			}
		}
		next := &leaseState{deadlines: deadlines, changed: make(chan struct{})}
		if leases.state.CompareAndSwap(current, next) {
			close(current.changed)
			return
		}
	}
}

const (
	MaxTCP                  = 256
	MaxTCPPerEnvironment    = 64
	MaxUDP                  = 256
	MaxUDPPerEnvironment    = 64
	MaxDNS                  = 128
	MaxDNSPerEnvironment    = 32
	MaxInfrastructureLeases = 4
	MaxInfrastructureTCP    = 16
	MaxInfrastructureUDP    = 16
	MaxInfrastructureDNS    = 16
)
