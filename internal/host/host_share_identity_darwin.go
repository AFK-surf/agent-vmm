//go:build darwin

package host

/*
#cgo LDFLAGS: -framework CoreFoundation
#include <CoreFoundation/CoreFoundation.h>
#include <errno.h>
#include <stdint.h>
#include <sys/attr.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <string.h>
#include <unistd.h>
#include <uuid/uuid.h>

static void hostShareVolumeFlag(CFURLRef url, CFStringRef key, int *value) {
    *value = -1;
    CFTypeRef property = NULL;
    CFErrorRef error = NULL;
    if (!CFURLCopyResourcePropertyForKey(url, key, &property, &error)) {
        if (error) CFRelease(error);
        return;
    }
    if (!property || CFGetTypeID(property) != CFBooleanGetTypeID()) {
        if (property) CFRelease(property);
        return;
    }
    *value = CFBooleanGetValue((CFBooleanRef)property);
    CFRelease(property);
}

static int hostShareIdentity(int fd, char *volume, uint64_t *fileID, int *flags) {
    struct stat source;
    struct statfs fs, observed;
    if (fstat(fd, &source) != 0 || fstatfs(fd, &fs) != 0) return errno;
    struct attrlist requested = {0};
    requested.bitmapcount = ATTR_BIT_MAP_COUNT;
    requested.volattr = ATTR_VOL_INFO | ATTR_VOL_UUID;
    struct __attribute__((packed, aligned(4))) { uint32_t length; uuid_t volume; } value;
    if (getattrlist(fs.f_mntonname, &requested, &value, sizeof(value), 0) != 0) return errno;
    if (statfs(fs.f_mntonname, &observed) != 0) return errno;
    if (memcmp(&fs.f_fsid, &observed.f_fsid, sizeof(fs.f_fsid)) != 0) return ESTALE;
    if (uuid_is_null(value.volume)) return ENOTSUP;
    CFURLRef url = CFURLCreateFromFileSystemRepresentation(NULL, (const UInt8 *)fs.f_mntonname, strlen(fs.f_mntonname), true);
    if (!url) return ENOTSUP;
    CFStringRef keys[] = {kCFURLVolumeIsInternalKey, kCFURLVolumeIsLocalKey, kCFURLVolumeIsRemovableKey, kCFURLVolumeIsEjectableKey};
    for (int i = 0; i < 4; i++) hostShareVolumeFlag(url, keys[i], &flags[i]);
    CFRelease(url);
    uuid_unparse_lower(value.volume, volume);
    *fileID = source.st_ino;
    return 0;
}
*/
import "C"

import (
	"fmt"
	"os"
	"runtime"
	"syscall"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func identifyHostShareSource(source *os.File) (store.HostShareSource, error) {
	var volume [37]C.char
	var fileID C.uint64_t
	var flags [4]C.int
	code := C.hostShareIdentity(C.int(source.Fd()), &volume[0], &fileID, &flags[0])
	runtime.KeepAlive(source)
	if code != 0 {
		return store.HostShareSource{}, os.NewSyscallError("share source identity", syscall.Errno(code))
	}
	if !hostShareVolumeSupported(flags[0] > 0, flags[1] > 0, flags[2] != 0, flags[3] != 0) {
		return store.HostShareSource{}, fmt.Errorf("host shares require an internal local volume that is neither removable nor ejectable (observed internal=%d local=%d removable=%d ejectable=%d)", flags[0], flags[1], flags[2], flags[3])
	}
	return store.HostShareSource{Path: source.Name(), FileSystemUUID: C.GoString(&volume[0]), FileID: uint64(fileID)}, nil
}

func hostShareVolumeSupported(internal, local, removable, ejectable bool) bool {
	return internal && local && !removable && !ejectable
}

func hostSharesSupported() error { return nil }
