package egress

import (
	"context"
	"errors"
	"log/slog"
	"net/netip"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/egresslink"
)

type Mode uint8

const (
	DenyAll Mode = iota
	PublicInternet
)

type Source struct {
	EnvironmentID    string
	NamespaceID      string
	Subnet           netip.Prefix
	Mode             Mode
	LANEgress        bool
	InstanceID       string
	GrantRevision    uint64
	HostTCPEndpoints []netip.AddrPort
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
	changed  chan struct{}
}

type Policies struct {
	state atomic.Pointer[policyState]
	mu    sync.Mutex
}

func (policies *Policies) Publish(snapshot Snapshot) error {
	policies.mu.Lock()
	defer policies.mu.Unlock()
	if snapshot.BootID == "" || snapshot.Revision == 0 {
		return errors.New("boot ID and policy revision are required")
	}
	sources := cloneSources(snapshot.Sources)
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
	policies.state.Store(&policyState{bootID: snapshot.BootID, revision: snapshot.Revision, sources: sources, changed: make(chan struct{})})
	if current != nil {
		close(current.changed)
	}
	return nil
}

func (policies *Policies) Clear() {
	policies.mu.Lock()
	defer policies.mu.Unlock()
	if current := policies.state.Swap(nil); current != nil {
		close(current.changed)
	}
}

func (policies *Policies) Resolve(bootID string, address netip.Addr) (Source, bool) {
	result, found := policies.Lookup(bootID, address)
	return result, found && (result.Mode == PublicInternet || result.LANEgress || len(result.HostTCPEndpoints) > 0)
}

func (policies *Policies) Lookup(bootID string, address netip.Addr) (Source, bool) {
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
	return result.Clone(), found
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

var namespacePool = netip.MustParsePrefix("10.64.0.0/10")

var transparentTest = netip.MustParsePrefix("198.18.0.0/15")

func DestinationAllowed(address netip.Addr, namespaceSubnets []netip.Prefix, allowTransparentTest bool) bool {
	if !address.Is4() || address.IsUnspecified() || address.IsMulticast() {
		return false
	}
	guest, host, prefix := egresslink.Addresses()
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

func (policies *Policies) Context(parent context.Context, bootID string, address netip.Addr, expected Source) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	expected = expected.Clone()
	current, ok := policies.Lookup(bootID, address)
	if !ok || !current.Equal(expected) {
		slog.Warn("egress flow authorization changed before attachment", "boot_id", bootID, "source", address, "environment_id", expected.EnvironmentID, "namespace_id", expected.NamespaceID, "expected_instance", expected.InstanceID, "observed_instance", current.InstanceID, "source_present", ok)
		cancel()
		return ctx, cancel
	}
	go func() {
		for {
			state := policies.state.Load()
			current, exists := policies.Lookup(bootID, address)
			if state == nil || !exists || !current.Equal(expected) {
				reason, observedBoot := "source_changed", ""
				if state == nil {
					reason = "policy_removed"
				} else {
					observedBoot = state.bootID
					if state.bootID != bootID {
						reason = "boot_changed"
					} else if !exists {
						reason = "source_missing"
					}
				}
				slog.Info("egress flow authorization ended", "reason", reason, "boot_id", bootID, "observed_boot", observedBoot, "source", address, "environment_id", expected.EnvironmentID, "namespace_id", expected.NamespaceID, "expected_instance", expected.InstanceID, "observed_instance", current.InstanceID)
				cancel()
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-state.changed:
			}
		}
	}()
	return ctx, cancel
}

func LANDestinationAllowed(address netip.Addr, namespaceSubnets []netip.Prefix, hostAddresses []netip.Addr) bool {
	if !address.Is4() || !address.IsPrivate() {
		return false
	}
	_, _, transport := egresslink.Addresses()
	if transport.Contains(address) || namespacePool.Contains(address) {
		return false
	}
	for _, subnet := range namespaceSubnets {
		if subnet.Contains(address) {
			return false
		}
	}
	for _, host := range hostAddresses {
		if address == host.Unmap() {
			return false
		}
	}
	return true
}

func (policies *Policies) RevokeEnvironment(environmentID string) {
	policies.mu.Lock()
	defer policies.mu.Unlock()
	current := policies.state.Load()
	if current == nil {
		return
	}
	sources := cloneSources(current.sources)
	for index := range sources {
		if sources[index].EnvironmentID == environmentID {
			sources[index].Mode = DenyAll
			sources[index].LANEgress = false
			sources[index].HostTCPEndpoints = nil
		}
	}
	policies.state.Store(&policyState{bootID: current.bootID, revision: current.revision, sources: sources, changed: make(chan struct{})})
	close(current.changed)
}

func (source Source) Clone() Source {
	source.HostTCPEndpoints = slices.Clone(source.HostTCPEndpoints)
	return source
}
func (source Source) Equal(other Source) bool {
	return source.EnvironmentID == other.EnvironmentID && source.NamespaceID == other.NamespaceID && source.Subnet == other.Subnet && source.Mode == other.Mode && source.LANEgress == other.LANEgress && source.InstanceID == other.InstanceID && source.GrantRevision == other.GrantRevision && slices.Equal(source.HostTCPEndpoints, other.HostTCPEndpoints)
}
func cloneSources(sources []Source) []Source {
	result := make([]Source, len(sources))
	for index, source := range sources {
		result[index] = source.Clone()
	}
	return result
}
