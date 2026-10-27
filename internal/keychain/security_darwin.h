#include <CoreFoundation/CoreFoundation.h>
#import <LocalAuthentication/LocalAuthentication.h>
#include <Security/Security.h>
#include <stdlib.h>
#include <string.h>

static void av_no_interaction(CFMutableDictionaryRef query) {
    LAContext *context = [[LAContext alloc] init];
    context.interactionNotAllowed = YES;
    CFDictionarySetValue(query, kSecUseAuthenticationContext, (CFTypeRef)context);
    [context release];
}

static CFStringRef av_string(const char *value) {
    return CFStringCreateWithCString(NULL, value, kCFStringEncodingUTF8);
}

static CFMutableDictionaryRef av_query(const char *service, const char *group, const char *account, int key) {
    CFMutableDictionaryRef query = CFDictionaryCreateMutable(NULL, 0, &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
    CFDictionarySetValue(query, kSecClass, key ? kSecClassKey : kSecClassGenericPassword);
    CFDictionarySetValue(query, kSecUseDataProtectionKeychain, kCFBooleanTrue);
    av_no_interaction(query);
    CFStringRef access = av_string(group);
    CFDictionarySetValue(query, kSecAttrAccessGroup, access);
    CFRelease(access);
    if (key) {
        CFDictionarySetValue(query, kSecAttrKeyType, kSecAttrKeyTypeECSECPrimeRandom);
        CFDictionarySetValue(query, kSecAttrKeyClass, kSecAttrKeyClassPrivate);
        CFDataRef tag = CFDataCreate(NULL, (const UInt8 *)account, strlen(account));
        CFDictionarySetValue(query, kSecAttrApplicationTag, tag);
        CFRelease(tag);
    } else {
        CFStringRef name = av_string(service);
        CFDictionarySetValue(query, kSecAttrService, name);
        CFRelease(name);
        if (account) {
            CFStringRef name = av_string(account);
            CFDictionarySetValue(query, kSecAttrAccount, name);
            CFRelease(name);
        }
    }
    return query;
}

static void *av_copy_data(CFDataRef value, long *length) {
    *length = CFDataGetLength(value);
    void *result = malloc(*length ? *length : 1);
    if (*length) memcpy(result, CFDataGetBytePtr(value), *length);
    return result;
}
