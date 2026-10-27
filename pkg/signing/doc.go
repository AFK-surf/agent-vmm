// Package signing defines the V1 canonical inputs for signed trust and appliance messages.
// It fixes the signature domain, rejects unknown fields and duplicate set elements,
// and clears each message's signature before deterministic protobuf encoding.
// Shared vectors bind other implementations to the same input bytes.
package signing
