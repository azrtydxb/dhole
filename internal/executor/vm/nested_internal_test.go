package vm

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A /dev/kvm this engine cannot open is not a hypervisor it can hand a guest.
//
// The device existing was taken to be enough, and on a GitHub runner it is
// not: /dev/kvm is there, owned by root with no group access, the kvm module
// says nested=Y, and NESTED_VIRT was advertised by an engine that could not
// boot a VM at all, let alone one with a hypervisor inside it.
func TestNestedVirtIsNotAdvertisedWhenTheDeviceCannotBeOpened(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root opens a mode-0000 file, so an unopenable device cannot be simulated")
	}
	dir := t.TempDir()
	kvm := filepath.Join(dir, "kvm")
	require.NoError(t, os.WriteFile(kvm, nil, 0o000))
	nested := filepath.Join(dir, "nested")
	require.NoError(t, os.WriteFile(nested, []byte("Y\n"), 0o600))

	require.False(t, nestedVirtAvailableAt(kvm, []string{nested}),
		"NESTED_VIRT advertised on a host whose /dev/kvm this process cannot open")
}

// The other half: an openable device with nesting enabled is advertised, so
// the check above is not a blanket refusal.
func TestNestedVirtIsAdvertisedWhenTheDeviceOpensAndNestingIsOn(t *testing.T) {
	dir := t.TempDir()
	kvm := filepath.Join(dir, "kvm")
	require.NoError(t, os.WriteFile(kvm, nil, 0o600))
	nested := filepath.Join(dir, "nested")
	require.NoError(t, os.WriteFile(nested, []byte("Y\n"), 0o600))

	require.True(t, nestedVirtAvailableAt(kvm, []string{nested}))
}
