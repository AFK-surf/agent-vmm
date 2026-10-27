//go:build darwin && keychain && cgo

package keychain

/*
#cgo LDFLAGS: -framework Security -framework CoreFoundation
#include "security_darwin.h"

static SecKeyRef av_find_key(const char *service, const char *group, const char *account, OSStatus *status) {
    CFMutableDictionaryRef query = av_query(service, group, account, 1);
    CFDictionarySetValue(query, kSecReturnRef, kCFBooleanTrue);
    SecKeyRef key = NULL;
    *status = SecItemCopyMatching(query, (CFTypeRef *)&key);
    CFRelease(query);
    return key;
}

static SecKeyRef av_create_key(const char *group, const char *account, int enclave, OSStatus *status) {
    CFMutableDictionaryRef attributes = CFDictionaryCreateMutable(NULL, 0, &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
    CFMutableDictionaryRef privateAttributes = CFDictionaryCreateMutable(NULL, 0, &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
    CFDictionarySetValue(attributes, kSecAttrKeyType, kSecAttrKeyTypeECSECPrimeRandom);
    int bits = 256;
    CFNumberRef size = CFNumberCreate(NULL, kCFNumberIntType, &bits);
    CFDictionarySetValue(attributes, kSecAttrKeySizeInBits, size);
    CFRelease(size);
    CFDictionarySetValue(attributes, kSecUseDataProtectionKeychain, kCFBooleanTrue);
    av_no_interaction(attributes);
    if (enclave) CFDictionarySetValue(attributes, kSecAttrTokenID, kSecAttrTokenIDSecureEnclave);
    CFDictionarySetValue(privateAttributes, kSecAttrIsPermanent, kCFBooleanTrue);
    CFDataRef tag = CFDataCreate(NULL, (const UInt8 *)account, strlen(account));
    CFDictionarySetValue(privateAttributes, kSecAttrApplicationTag, tag);
    CFRelease(tag);
    CFStringRef accessGroup = av_string(group);
    CFDictionarySetValue(privateAttributes, kSecAttrAccessGroup, accessGroup);
    CFRelease(accessGroup);
    CFErrorRef error = NULL;
    SecAccessControlRef access = SecAccessControlCreateWithFlags(NULL, kSecAttrAccessibleWhenUnlockedThisDeviceOnly,
        enclave ? kSecAccessControlPrivateKeyUsage : 0, &error);
    if (!access) {
        *status = error ? (OSStatus)CFErrorGetCode(error) : errSecInternalComponent;
        if (error) CFRelease(error);
        CFRelease(privateAttributes); CFRelease(attributes);
        return NULL;
    }
    CFDictionarySetValue(privateAttributes, kSecAttrAccessControl, access);
    CFRelease(access);
    CFDictionarySetValue(attributes, kSecPrivateKeyAttrs, privateAttributes);
    SecKeyRef key = SecKeyCreateRandomKey(attributes, &error);
    *status = key ? errSecSuccess : (error ? (OSStatus)CFErrorGetCode(error) : errSecInternalComponent);
    if (error) CFRelease(error);
    CFRelease(privateAttributes);
    CFRelease(attributes);
    return key;
}

static int av_enclave_key(SecKeyRef key) {
    CFDictionaryRef attributes = SecKeyCopyAttributes(key);
    CFTypeRef token = CFDictionaryGetValue(attributes, kSecAttrTokenID);
    int enclave = token && CFEqual(token, kSecAttrTokenIDSecureEnclave);
    CFRelease(attributes);
    return enclave;
}

static OSStatus av_public_key(SecKeyRef key, void **bytes, long *length) {
    SecKeyRef publicKey = SecKeyCopyPublicKey(key);
    if (!publicKey) return errSecInternalComponent;
    CFErrorRef error = NULL;
    CFDataRef data = SecKeyCopyExternalRepresentation(publicKey, &error);
    CFRelease(publicKey);
    if (!data) {
        OSStatus status = error ? (OSStatus)CFErrorGetCode(error) : errSecInternalComponent;
        if (error) CFRelease(error);
        return status;
    }
    *bytes = av_copy_data(data, length);
    CFRelease(data);
    return errSecSuccess;
}

static OSStatus av_signature(SecKeyRef key, void *digest, long length, void **bytes, long *signatureLength) {
    CFDataRef input = CFDataCreate(NULL, digest, length);
    CFErrorRef error = NULL;
    CFDataRef signature = SecKeyCreateSignature(key, kSecKeyAlgorithmECDSASignatureDigestX962SHA256, input, &error);
    CFRelease(input);
    if (!signature) {
        OSStatus status = error ? (OSStatus)CFErrorGetCode(error) : errSecInternalComponent;
        if (error) CFRelease(error);
        return status;
    }
    *bytes = av_copy_data(signature, signatureLength);
    CFRelease(signature);
    return errSecSuccess;
}

static OSStatus av_key_delete(const char *service, const char *group, const char *account) {
    CFMutableDictionaryRef query = av_query(service, group, account, 1);
    OSStatus status = SecItemDelete(query);
    CFRelease(query);
    return status;
}
*/
import "C"

import (
	"context"
	"crypto/elliptic"
	"encoding/asn1"
	"errors"
	"fmt"
	"math/big"
	"os"
	"unsafe"
)

func (backend *Backend) EnsureKey(ctx context.Context, account, custody string) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	service, group, name, release := backend.names(account)
	defer release()
	var code C.OSStatus
	key := C.av_find_key(service, group, name, &code)
	if code == C.errSecItemNotFound {
		enclave := C.int(1)
		if custody == "keychain" {
			enclave = 0
		}
		key = C.av_create_key(group, name, enclave, &code)
		if custody == "" && code != C.errSecSuccess && code != C.errSecDuplicateItem {
			key = C.av_create_key(group, name, 0, &code)
		}
		if code == C.errSecDuplicateItem {
			key = C.av_find_key(service, group, name, &code)
		}
	}
	if err := statusError("ensure key", code); err != nil {
		return nil, "", err
	}
	defer C.CFRelease(C.CFTypeRef(key))
	actualCustody := "keychain"
	if C.av_enclave_key(key) != 0 {
		actualCustody = "secure-enclave"
	}
	if custody != "" && actualCustody != custody {
		return nil, "", fmt.Errorf("%w: device root custody changed", ErrUnavailable)
	}
	var data unsafe.Pointer
	var length C.long
	if err := statusError("public key", C.av_public_key(key, &data, &length)); err != nil {
		return nil, "", err
	}
	defer C.free(data)
	x, y := elliptic.Unmarshal(elliptic.P256(), C.GoBytes(data, C.int(length)))
	if x == nil {
		return nil, "", errors.New("keychain returned an invalid P-256 public key")
	}
	return elliptic.MarshalCompressed(elliptic.P256(), x, y), actualCustody, nil
}

func (backend *Backend) Sign(ctx context.Context, account string, digest []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(digest) != 32 {
		return nil, errors.New("P-256 signing requires a SHA-256 digest")
	}
	service, group, name, release := backend.names(account)
	defer release()
	var code C.OSStatus
	key := C.av_find_key(service, group, name, &code)
	if err := statusError("find signing key", code); err != nil {
		return nil, err
	}
	defer C.CFRelease(C.CFTypeRef(key))
	input := C.CBytes(digest)
	defer C.free(input)
	var data unsafe.Pointer
	var length C.long
	if err := statusError("sign", C.av_signature(key, input, C.long(len(digest)), &data, &length)); err != nil {
		return nil, err
	}
	defer C.free(data)
	var signature struct{ R, S *big.Int }
	if rest, err := asn1.Unmarshal(C.GoBytes(data, C.int(length)), &signature); err != nil || len(rest) != 0 || signature.R == nil || signature.S == nil {
		return nil, errors.New("keychain returned an invalid P-256 signature")
	}
	halfOrder := new(big.Int).Rsh(new(big.Int).Set(elliptic.P256().Params().N), 1)
	if signature.S.Cmp(halfOrder) > 0 {
		signature.S.Sub(elliptic.P256().Params().N, signature.S)
	}
	result := make([]byte, 64)
	signature.R.FillBytes(result[:32])
	signature.S.FillBytes(result[32:])
	return result, nil
}

func (backend *Backend) DeleteKey(ctx context.Context, account string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	service, group, name, release := backend.names(account)
	defer release()
	err := statusError("delete key", C.av_key_delete(service, group, name))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
