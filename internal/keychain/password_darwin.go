//go:build darwin && keychain && cgo

package keychain

/*
#cgo CFLAGS: -x objective-c
#cgo LDFLAGS: -framework Security -framework CoreFoundation -framework LocalAuthentication -framework Foundation
#include "security_darwin.h"

static OSStatus av_password_write(const char *service, const char *group, const char *account, void *bytes, long length, int replace) {
    CFMutableDictionaryRef query = av_query(service, group, account, 0);
    CFDataRef data = CFDataCreate(NULL, bytes, length);
    OSStatus status = errSecItemNotFound;
    if (replace) {
        const void *keys[] = { kSecValueData };
        const void *values[] = { data };
        CFDictionaryRef changes = CFDictionaryCreate(NULL, keys, values, 1, &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
        status = SecItemUpdate(query, changes);
        CFRelease(changes);
    }
    if (status == errSecItemNotFound) {
        CFDictionarySetValue(query, kSecValueData, data);
        CFDictionarySetValue(query, kSecAttrAccessible, kSecAttrAccessibleWhenUnlockedThisDeviceOnly);
        status = SecItemAdd(query, NULL);
    }
    CFRelease(data);
    CFRelease(query);
    return status;
}

static OSStatus av_password_get(const char *service, const char *group, const char *account, void **bytes, long *length) {
    CFMutableDictionaryRef query = av_query(service, group, account, 0);
    CFDictionarySetValue(query, kSecReturnData, kCFBooleanTrue);
    CFDataRef data = NULL;
    OSStatus status = SecItemCopyMatching(query, (CFTypeRef *)&data);
    CFRelease(query);
    if (status == errSecSuccess) {
        *bytes = av_copy_data(data, length);
        CFRelease(data);
    }
    return status;
}

static OSStatus av_delete(const char *service, const char *group, const char *account, int key) {
    CFMutableDictionaryRef query = av_query(service, group, account, key);
    OSStatus status = SecItemDelete(query);
    CFRelease(query);
    return status;
}

static OSStatus av_accounts(const char *service, const char *group, CFArrayRef *accounts) {
    CFMutableDictionaryRef query = av_query(service, group, NULL, 0);
    CFDictionarySetValue(query, kSecReturnAttributes, kCFBooleanTrue);
    CFDictionarySetValue(query, kSecMatchLimit, kSecMatchLimitAll);
    OSStatus status = SecItemCopyMatching(query, (CFTypeRef *)accounts);
    CFRelease(query);
    return status;
}

static char *av_account_at(CFArrayRef accounts, long index) {
    CFDictionaryRef attributes = CFArrayGetValueAtIndex(accounts, index);
    CFStringRef account = CFDictionaryGetValue(attributes, kSecAttrAccount);
    CFIndex size = CFStringGetMaximumSizeForEncoding(CFStringGetLength(account), kCFStringEncodingUTF8) + 1;
    char *value = malloc(size);
    if (!CFStringGetCString(account, value, size, kCFStringEncodingUTF8)) { free(value); return NULL; }
    return value;
}
*/
import "C"

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"unsafe"
)

func Available() bool { return true }

func statusError(operation string, code C.OSStatus) error {
	if code == C.errSecSuccess {
		return nil
	}
	if code == C.errSecItemNotFound {
		return os.ErrNotExist
	}
	return fmt.Errorf("%w: keychain %s status %d", ErrUnavailable, operation, int32(code))
}

func (backend *Backend) names(account string) (*C.char, *C.char, *C.char, func()) {
	service, group, name := C.CString(Service), C.CString(AccessGroup), C.CString(backend.account(account))
	return service, group, name, func() { C.free(unsafe.Pointer(service)); C.free(unsafe.Pointer(group)); C.free(unsafe.Pointer(name)) }
}

func (backend *Backend) Probe(ctx context.Context) error {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return err
	}
	account := "probe/" + hex.EncodeToString(value[:16])
	if err := backend.Put(ctx, account, value[:]); err != nil {
		return err
	}
	loaded, err := backend.Get(ctx, account)
	defer clear(loaded)
	if err == nil && !bytes.Equal(loaded, value[:]) {
		err = errors.New("keychain probe returned different bytes")
	}
	return errors.Join(err, backend.Delete(ctx, account))
}

func (backend *Backend) Put(ctx context.Context, account string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	service, group, name, release := backend.names(account)
	defer release()
	value := C.CBytes(data)
	defer func() { C.memset(value, 0, C.size_t(len(data))); C.free(value) }()
	code := C.av_password_write(service, group, name, value, C.long(len(data)), 0)
	if code == C.errSecDuplicateItem {
		current, err := backend.Get(ctx, account)
		if err != nil {
			return err
		}
		defer clear(current)
		if bytes.Equal(current, data) {
			return nil
		}
		return fmt.Errorf("keychain contents differ: %w", os.ErrExist)
	}
	return statusError("put", code)
}

func (backend *Backend) Replace(ctx context.Context, account string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	service, group, name, release := backend.names(account)
	defer release()
	value := C.CBytes(data)
	defer func() { C.memset(value, 0, C.size_t(len(data))); C.free(value) }()
	return statusError("replace", C.av_password_write(service, group, name, value, C.long(len(data)), 1))
}

func (backend *Backend) Get(ctx context.Context, account string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	service, group, name, release := backend.names(account)
	defer release()
	var value unsafe.Pointer
	var length C.long
	if err := statusError("get", C.av_password_get(service, group, name, &value, &length)); err != nil {
		return nil, err
	}
	defer func() { C.memset(value, 0, C.size_t(length)); C.free(value) }()
	return C.GoBytes(value, C.int(length)), nil
}

func (backend *Backend) Delete(ctx context.Context, account string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	service, group, name, release := backend.names(account)
	defer release()
	err := statusError("delete", C.av_delete(service, group, name, 0))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (backend *Backend) DeletePrefix(ctx context.Context, prefix string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	service, group, name, release := backend.names("")
	defer release()
	_ = name
	var accounts C.CFArrayRef
	if err := statusError("list", C.av_accounts(service, group, &accounts)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer C.CFRelease(C.CFTypeRef(accounts))
	fullPrefix := backend.account(prefix)
	var result error
	for index := C.CFIndex(0); index < C.CFArrayGetCount(accounts); index++ {
		value := C.av_account_at(accounts, C.long(index))
		account := C.GoString(value)
		C.free(unsafe.Pointer(value))
		if strings.HasPrefix(account, fullPrefix) {
			result = errors.Join(result, backend.Delete(ctx, strings.TrimPrefix(account, backend.account(""))))
		}
	}
	return result
}
