//go:build darwin && !development && !e2e

package trustkeys

/*
#cgo LDFLAGS: -framework Security -framework CoreFoundation
#include <CoreFoundation/CoreFoundation.h>
#include <Security/Security.h>
#include <stdlib.h>

static CFDataRef avmm_data(const unsigned char *bytes, long length) {
  return CFDataCreate(kCFAllocatorDefault, bytes, length);
}

static SecKeyRef avmm_find_key(const unsigned char *tag, long tag_len) {
  CFDataRef tag_data = avmm_data(tag, tag_len);
  const void *keys[] = {kSecClass, kSecAttrApplicationTag, kSecAttrKeyType, kSecReturnRef};
  const void *values[] = {kSecClassKey, tag_data, kSecAttrKeyTypeECSECPrimeRandom, kCFBooleanTrue};
  CFDictionaryRef query = CFDictionaryCreate(kCFAllocatorDefault, keys, values, 4,
      &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
  SecKeyRef key = NULL;
  OSStatus status = SecItemCopyMatching(query, (CFTypeRef *)&key);
  CFRelease(query);
  CFRelease(tag_data);
  if (status != errSecSuccess) return NULL;
  return key;
}

static SecKeyRef avmm_create_key(const unsigned char *tag, long tag_len, int enclave) {
  CFDataRef tag_data = avmm_data(tag, tag_len);
  const void *private_keys[] = {kSecAttrIsPermanent, kSecAttrApplicationTag, kSecAttrAccessible};
  const void *private_values[] = {kCFBooleanTrue, tag_data, kSecAttrAccessibleWhenUnlockedThisDeviceOnly};
  CFDictionaryRef private_attrs = CFDictionaryCreate(kCFAllocatorDefault, private_keys, private_values, 3,
      &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
  int key_size = 256;
  CFNumberRef key_bits = CFNumberCreate(kCFAllocatorDefault, kCFNumberIntType, &key_size);
  const void *keys[4] = {kSecAttrKeyType, kSecAttrKeySizeInBits, kSecPrivateKeyAttrs, kSecAttrTokenID};
  const void *values[4] = {kSecAttrKeyTypeECSECPrimeRandom, key_bits, private_attrs, kSecAttrTokenIDSecureEnclave};
  CFDictionaryRef attrs = CFDictionaryCreate(kCFAllocatorDefault, keys, values, enclave ? 4 : 3,
      &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
  CFErrorRef error = NULL;
  SecKeyRef key = SecKeyCreateRandomKey(attrs, &error);
  if (error) CFRelease(error);
  CFRelease(attrs);
  CFRelease(key_bits);
  CFRelease(private_attrs);
  CFRelease(tag_data);
  return key;
}

static int avmm_copy_bytes(CFDataRef data, unsigned char **output, long *output_len) {
  if (!data) return -1;
  CFIndex length = CFDataGetLength(data);
  unsigned char *bytes = malloc((size_t)length);
  if (!bytes) return -2;
  CFDataGetBytes(data, CFRangeMake(0, length), bytes);
  *output = bytes;
  *output_len = (long)length;
  return 0;
}

static int avmm_ensure_key(const unsigned char *tag, long tag_len, int *custody,
    unsigned char **output, long *output_len) {
  SecKeyRef key = avmm_find_key(tag, tag_len);
  if (!key) {
    key = avmm_create_key(tag, tag_len, 1);
    if (key) *custody = 1;
    else {
      key = avmm_create_key(tag, tag_len, 0);
      if (!key) return -3;
      *custody = 2;
    }
  } else {
    // Custody is persisted when the key is created. Existing-key discovery
    // deliberately avoids SecKeyCopyAttributes: ad-hoc/test signatures can
    // receive a non-inspectable key reference even though signing is allowed.
    *custody = 2;
  }
  SecKeyRef public_key = SecKeyCopyPublicKey(key);
  CFErrorRef error = NULL;
  CFDataRef external = SecKeyCopyExternalRepresentation(public_key, &error);
  int result = avmm_copy_bytes(external, output, output_len);
  if (external) CFRelease(external);
  if (error) CFRelease(error);
  CFRelease(public_key);
  CFRelease(key);
  return result;
}

static int avmm_sign(const unsigned char *tag, long tag_len,
    const unsigned char *digest, long digest_len, unsigned char **output, long *output_len) {
  SecKeyRef key = avmm_find_key(tag, tag_len);
  if (!key) return -4;
  CFDataRef digest_data = avmm_data(digest, digest_len);
  CFErrorRef error = NULL;
  CFDataRef signature = SecKeyCreateSignature(key,
      kSecKeyAlgorithmECDSASignatureDigestX962SHA256, digest_data, &error);
  int result = avmm_copy_bytes(signature, output, output_len);
  if (signature) CFRelease(signature);
  if (error) CFRelease(error);
  CFRelease(digest_data);
  CFRelease(key);
  return result;
}

static int avmm_delete_key(const unsigned char *tag, long tag_len) {
  CFDataRef tag_data = avmm_data(tag, tag_len);
  const void *keys[] = {kSecClass, kSecAttrApplicationTag, kSecAttrKeyType};
  const void *values[] = {kSecClassKey, tag_data, kSecAttrKeyTypeECSECPrimeRandom};
  CFDictionaryRef query = CFDictionaryCreate(kCFAllocatorDefault, keys, values, 3,
      &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
  OSStatus status = SecItemDelete(query);
  CFRelease(query);
  CFRelease(tag_data);
  return status == errSecSuccess || status == errSecItemNotFound ? 0 : (int)status;
}
*/
import "C"

import (
	"context"
	"crypto/elliptic"
	"encoding/asn1"
	"errors"
	"math/big"
	"unsafe"
)

type PlatformProvider struct{}

func NewPlatformProvider() Provider { return PlatformProvider{} }

func (PlatformProvider) Ensure(_ context.Context, revision uint64) ([]byte, string, error) {
	tag := []byte(Tag(revision))
	var output *C.uchar
	var outputLength C.long
	var custody C.int
	if result := C.avmm_ensure_key((*C.uchar)(unsafe.Pointer(&tag[0])), C.long(len(tag)), &custody, &output, &outputLength); result != 0 {
		return nil, "", ErrKeyUnavailable
	}
	defer C.free(unsafe.Pointer(output))
	public, err := compressPublicKey(C.GoBytes(unsafe.Pointer(output), C.int(outputLength)))
	if err != nil {
		return nil, "", err
	}
	if custody == 1 {
		return public, "secure_enclave", nil
	}
	return public, "keychain", nil
}

func (PlatformProvider) Sign(_ context.Context, revision uint64, digest []byte) ([]byte, error) {
	if len(digest) != 32 {
		return nil, errors.New("P-256 signing requires a SHA-256 digest")
	}
	tag := []byte(Tag(revision))
	var output *C.uchar
	var outputLength C.long
	if result := C.avmm_sign((*C.uchar)(unsafe.Pointer(&tag[0])), C.long(len(tag)), (*C.uchar)(unsafe.Pointer(&digest[0])), C.long(len(digest)), &output, &outputLength); result != 0 {
		return nil, ErrKeyUnavailable
	}
	defer C.free(unsafe.Pointer(output))
	var signature struct{ R, S *big.Int }
	if _, err := asn1.Unmarshal(C.GoBytes(unsafe.Pointer(output), C.int(outputLength)), &signature); err != nil || signature.R == nil || signature.S == nil {
		return nil, errors.New("platform returned an invalid ECDSA signature")
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

func (PlatformProvider) Delete(_ context.Context, revision uint64) error {
	tag := []byte(Tag(revision))
	if result := C.avmm_delete_key((*C.uchar)(unsafe.Pointer(&tag[0])), C.long(len(tag))); result != 0 {
		return ErrKeyUnavailable
	}
	return nil
}

func compressPublicKey(value []byte) ([]byte, error) {
	x, y := elliptic.Unmarshal(elliptic.P256(), value)
	if x == nil || y == nil {
		return nil, errors.New("platform returned an invalid P-256 public key")
	}
	return elliptic.MarshalCompressed(elliptic.P256(), x, y), nil
}
