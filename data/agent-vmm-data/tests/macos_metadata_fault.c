#include <CoreFoundation/CoreFoundation.h>
#include <SystemConfiguration/SystemConfiguration.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int metadata_open(const char *path, int flags, ...) {
    if (strcmp(path, "/Library/Preferences/SystemConfiguration/NetworkInterfaces.plist") == 0 ||
        strcmp(path, "/Library/Preferences/SystemConfiguration/preferences.plist") == 0) {
        errno = ENOENT;
        return -1;
    }
    mode_t mode = 0;
    if (flags & O_CREAT) {
        va_list args;
        va_start(args, flags);
        mode = va_arg(args, int);
        va_end(args);
    }
    return open(path, flags, mode);
}

static CFArrayRef metadata_interfaces(void) {
    _exit(86);
}

static CFStringRef metadata_display_name(SCNetworkInterfaceRef interface) {
    (void)interface;
    _exit(86);
}

#define INTERPOSE(replacement, original) \
    __attribute__((used)) static struct { const void *replacement; const void *original; } \
    interpose_##original __attribute__((section("__DATA,__interpose"))) = { \
        (const void *)&replacement, (const void *)&original \
    }

INTERPOSE(metadata_open, open);
INTERPOSE(metadata_interfaces, SCNetworkInterfaceCopyAll);
INTERPOSE(metadata_display_name, SCNetworkInterfaceGetLocalizedDisplayName);
