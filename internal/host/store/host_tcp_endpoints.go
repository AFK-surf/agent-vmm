package store

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/netip"
)

type HostTCPEndpoints []netip.AddrPort

func (values HostTCPEndpoints) Value() (driver.Value, error) {
	if values == nil {
		return "[]", nil
	}
	encoded, err := json.Marshal(values)
	return string(encoded), err
}

func (values *HostTCPEndpoints) Scan(value any) error {
	text, ok := value.(string)
	if !ok {
		return fmt.Errorf("invalid Host TCP endpoint storage type %T", value)
	}
	return json.Unmarshal([]byte(text), values)
}
