package egress

import "sync"

type resourceKind uint8

const (
	tcpResource resourceKind = iota
	udpResource
	dnsResource
)

type counters struct {
	mu             sync.Mutex
	global         [3]int
	environment    map[string][3]int
	infrastructure [3]int
}

func (counters *counters) acquire(kind resourceKind, environment string, infrastructure bool) bool {
	counters.mu.Lock()
	defer counters.mu.Unlock()
	globalLimits := [3]int{MaxTCP, MaxUDP, MaxDNS}
	environmentLimits := [3]int{MaxTCPPerEnvironment, MaxUDPPerEnvironment, MaxDNSPerEnvironment}
	infrastructureLimits := [3]int{MaxInfrastructureTCP, MaxInfrastructureUDP, MaxInfrastructureDNS}
	if counters.global[kind] >= globalLimits[kind] {
		return false
	}
	if infrastructure {
		if counters.infrastructure[kind] >= infrastructureLimits[kind] {
			return false
		}
		counters.infrastructure[kind]++
	} else {
		current := counters.environment[environment]
		if current[kind] >= environmentLimits[kind] {
			return false
		}
		current[kind]++
		counters.environment[environment] = current
	}
	counters.global[kind]++
	return true
}

func (counters *counters) release(kind resourceKind, environment string, infrastructure bool) {
	counters.mu.Lock()
	defer counters.mu.Unlock()
	counters.global[kind]--
	if infrastructure {
		counters.infrastructure[kind]--
		return
	}
	current := counters.environment[environment]
	current[kind]--
	if current == [3]int{} {
		delete(counters.environment, environment)
	} else {
		counters.environment[environment] = current
	}
}
