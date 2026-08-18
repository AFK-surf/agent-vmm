//go:build linux

package control

import "testing"

func TestValidateEnvironmentAllowsMultilineValues(t *testing.T) {
	if err := validateEnvironment([]string{"CONFIG=first line\nsecond line\rthird"}); err != nil {
		t.Fatalf("multiline environment was rejected: %v", err)
	}
	if err := validateEnvironment([]string{"CONFIG=contains\x00nul"}); err == nil {
		t.Fatal("NUL environment value was accepted")
	}
}
