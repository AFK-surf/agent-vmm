//go:build darwin && cgo

package remoteconnector

/*
#cgo LDFLAGS: -framework CoreFoundation -framework SystemConfiguration
#include <CoreFoundation/CoreFoundation.h>
#include <SystemConfiguration/SystemConfiguration.h>
#include <stdlib.h>

typedef struct {
 char interface_name[64];
 char servers[2][128];
 char domains[16][256];
 int restricted;
 int domain_count;
} gateway_dns_service;

static int copy_string(CFTypeRef value, char *out, int size) {
 if (value && CFGetTypeID(value) == CFStringGetTypeID() &&
     CFStringGetCString((CFStringRef)value, out, size, kCFStringEncodingUTF8)) return 1;
 out[0] = 0;
 return 0;
}
static int copy_array(CFTypeRef value, char *out, int width, int capacity) {
 if (!value || CFGetTypeID(value) != CFArrayGetTypeID()) return 0;
 CFIndex count = CFArrayGetCount((CFArrayRef)value);
 int copied = 0;
 for (int i = 0; i < count && i < capacity; i++)
  if (copy_string(CFArrayGetValueAtIndex((CFArrayRef)value, i), out + copied * width, width)) copied++;
 return copied;
}
static int gateway_dns_services(gateway_dns_service *out, int capacity) {
 SCDynamicStoreRef store = SCDynamicStoreCreate(NULL, CFSTR("AgentVMM Gateway"), NULL, NULL);
 if (!store) return 0;
 CFArrayRef keys = SCDynamicStoreCopyKeyList(store, CFSTR("State:/Network/Service/[^/]+/DNS"));
 int written = 0;
 if (keys) {
  CFIndex count = CFArrayGetCount(keys);
  for (CFIndex i = 0; i < count && i < 128 && written < capacity; i++) {
   CFStringRef key = (CFStringRef)CFArrayGetValueAtIndex(keys, i);
   CFPropertyListRef value = SCDynamicStoreCopyValue(store, key);
   if (!value) continue;
   if (CFGetTypeID(value) == CFDictionaryGetTypeID()) {
    gateway_dns_service *service = &out[written];
    CFDictionaryRef dns = (CFDictionaryRef)value;
    copy_array(CFDictionaryGetValue(dns, CFSTR("ServerAddresses")), &service->servers[0][0], 128, 2);
    CFTypeRef domains = CFDictionaryGetValue(dns, CFSTR("SupplementalMatchDomains"));
    service->restricted = domains != NULL;
    service->domain_count = copy_array(domains, &service->domains[0][0], 256, 16);
    for (int family = 4; family <= 6 && !service->interface_name[0]; family += 2) {
     CFMutableStringRef interface_key = CFStringCreateMutableCopy(NULL, 0, key);
     CFStringReplace(interface_key, CFRangeMake(CFStringGetLength(key)-3, 3), family == 4 ? CFSTR("IPv4") : CFSTR("IPv6"));
     CFPropertyListRef config = SCDynamicStoreCopyValue(store, interface_key);
     CFRelease(interface_key);
     if (config) {
      if (CFGetTypeID(config) == CFDictionaryGetTypeID())
       copy_string(CFDictionaryGetValue((CFDictionaryRef)config, CFSTR("InterfaceName")), service->interface_name, 64);
      CFRelease(config);
     }
    }
    if (service->interface_name[0] && service->servers[0][0]) written++;
    else *service = (gateway_dns_service){0};
   }
   CFRelease(value);
  }
  CFRelease(keys);
 }
 CFRelease(store);
 return written;
}
*/
import "C"

import (
	"context"
	"net"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

var gatewayDiscoverySlot = make(chan struct{}, 1)

func gatewayRoutes(ctx context.Context, host string) []gatewayRoute {
	return boundedGatewayRoutes(ctx, gatewayDiscoverySlot, func() []gatewayRoute { return readGatewayRoutes(host) })
}

func readGatewayRoutes(host string) []gatewayRoute {
	var services [32]C.gateway_dns_service
	count := int(C.gateway_dns_services(&services[0], C.int(len(services))))
	configs := make([]gatewayDNSConfig, 0, count)
	for i := 0; i < count; i++ {
		service := &services[i]
		config := gatewayDNSConfig{name: C.GoString(&service.interface_name[0]), restricted: service.restricted != 0}
		for j := range service.servers {
			config.servers = append(config.servers, C.GoString(&service.servers[j][0]))
		}
		for j := 0; j < int(service.domain_count); j++ {
			config.domains = append(config.domains, C.GoString(&service.domains[j][0]))
		}
		configs = append(configs, config)
	}
	return selectGatewayRoutes(host, configs, net.InterfaceByName)
}

func gatewaySocketControl(index int) func(string, string, syscall.RawConn) error {
	return func(network, _ string, raw syscall.RawConn) error {
		var bindErr error
		err := raw.Control(func(fd uintptr) {
			if strings.HasSuffix(network, "6") {
				bindErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_BOUND_IF, index)
			} else {
				bindErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_BOUND_IF, index)
			}
		})
		if err != nil {
			return err
		}
		return bindErr
	}
}
