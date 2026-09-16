package conformance

import (
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
	"testing"
)

func TestReferenceContractRejectsMalformedControllerInput(t *testing.T) {
	valid := func() *remotev1.HostHello {
		return &remotev1.HostHello{ProtocolVersion: controllerremote.ProtocolVersion, HostApiVersion: "host.v1", RegistrationId: "reference", ConnectionEpoch: 1,
			InventoryWatermark: 1, SupportedFeatures: append([]string(nil), controllerremote.RequiredFeatures...), RequiredFeatures: append([]string(nil), controllerremote.RequiredFeatures...)}
	}
	tests := []struct {
		name   string
		mutate func(*remotev1.HostHello)
	}{
		{name: "missing watermark", mutate: func(value *remotev1.HostHello) { value.InventoryWatermark = 0 }},
		{name: "missing connection epoch", mutate: func(value *remotev1.HostHello) { value.ConnectionEpoch = 0 }},
		{name: "feature downgrade", mutate: func(value *remotev1.HostHello) { value.RequiredFeatures = nil }},
		{name: "duplicate allocation", mutate: func(value *remotev1.HostHello) {
			value.Inventory = []*remotev1.Allocation{{AllocationId: "same"}, {AllocationId: "same"}}
		}},
		{name: "cross registration", mutate: func(value *remotev1.HostHello) { value.RegistrationId = "other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := valid()
			test.mutate(value)
			if err := controllerremote.ValidateHello(value, "reference"); err == nil {
				t.Fatal("malformed controller hello was accepted")
			}
		})
	}
}
