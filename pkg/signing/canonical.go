package signing

import (
	"bytes"
	"errors"
	"fmt"

	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const domain = "agent-vmm/signature/v1\x00"

// CanonicalInput returns the only byte representation accepted by V1
// signers and verifiers. Signed messages contain no maps; callers must reject
// non-canonical repeated-field order before calling this function.
func CanonicalInput(message proto.Message) ([]byte, error) {
	if message == nil {
		return nil, errors.New("canonical signing input is nil")
	}
	if err := rejectUnknownFields(message.ProtoReflect()); err != nil {
		return nil, err
	}

	cloned := proto.Clone(message)
	typeName, err := clearSignature(cloned)
	if err != nil {
		return nil, err
	}
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(cloned)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical signing input: %w", err)
	}

	result := make([]byte, 0, len(domain)+len(typeName)+1+len(payload))
	result = append(result, domain...)
	result = append(result, typeName...)
	result = append(result, 0)
	result = append(result, payload...)
	return result, nil
}

func clearSignature(message proto.Message) (string, error) {
	switch value := message.(type) {
	case *trustv1.DeviceRootRotation:
		value.OldSignsNew = nil
		value.NewSignsOld = nil
		return "DeviceRootRotation", nil
	case *trustv1.PersonalMeshDescriptor:
		value.GenesisSignature = nil
		return "PersonalMeshDescriptor", nil
	case *trustv1.MembershipCredential:
		value.Signature = nil
		return "MembershipCredential", nil
	case *trustv1.EndpointObservation:
		value.DeviceSignature = nil
		return "EndpointObservation", nil
	case *trustv1.PairingProof:
		value.Signature = nil
		return "PairingProof", nil
	case *trustv1.RouteCapability:
		value.Signature = nil
		return "RouteCapability", nil
	case *trustv1.RegistryOperation:
		value.Signature = nil
		return "RegistryOperation", nil
	case *trustv1.RegistryReceipt:
		value.Signature = nil
		return "RegistryReceipt", nil
	case *servicev1.PersonalRouteRequest:
		value.SourceSignature = nil
		return "PersonalRouteRequest", nil
	default:
		return "", fmt.Errorf("unsupported signed message %T", message)
	}
}

func rejectUnknownFields(message protoreflect.Message) error {
	if len(message.GetUnknown()) != 0 {
		return fmt.Errorf("unknown fields in %s", message.Descriptor().FullName())
	}
	var nestedErr error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Kind() != protoreflect.MessageKind {
			return true
		}
		if field.IsList() {
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				if err := rejectUnknownFields(list.Get(i).Message()); err != nil {
					nestedErr = err
					return false
				}
			}
			return true
		}
		nestedErr = rejectUnknownFields(value.Message())
		return nestedErr == nil
	})
	return nestedErr
}

// IsStrictlySortedBytes reports whether a repeated bytes field has the
// canonical ascending order and contains no duplicate value.
func IsStrictlySortedBytes(values [][]byte) bool {
	for i := 1; i < len(values); i++ {
		if bytes.Compare(values[i-1], values[i]) >= 0 {
			return false
		}
	}
	return true
}
