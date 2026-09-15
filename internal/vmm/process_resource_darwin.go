//go:build darwin

package vmm

/*
#cgo LDFLAGS: -lproc
#include <libproc.h>
#include <stdint.h>
#include <string.h>
#include <sys/resource.h>
#include <unistd.h>

static int current_process_resident_bytes(uint64_t *resident) {
	struct rusage_info_v2 info;
	memset(&info, 0, sizeof(info));
	if (proc_pid_rusage(getpid(), RUSAGE_INFO_V2, (rusage_info_t *)&info) != 0) {
		return -1;
	}
	*resident = info.ri_resident_size;
	return 0;
}
*/
import "C"

import "errors"

func currentProcessMemoryBytes() (uint64, error) {
	var resident C.uint64_t
	if C.current_process_resident_bytes(&resident) != 0 {
		return 0, errors.New("current Host process memory is unavailable")
	}
	return uint64(resident), nil
}
