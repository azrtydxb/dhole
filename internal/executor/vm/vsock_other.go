//go:build !linux

package vm

import (
	"context"
	"fmt"
	"io"
	"runtime"
)

// dialVsock has no implementation off Linux, because vsock is a Linux address
// family and KVM is a Linux hypervisor. The QEMU backend therefore does not
// run here — and it says so, rather than failing later with a socket error
// that reads like a network problem.
func dialVsock(context.Context, uint32, uint32) (io.ReadWriteCloser, error) {
	return nil, fmt.Errorf("vm executor: the qemu backend needs the vsock address family, which %s does not have",
		runtime.GOOS)
}
