//go:build linux

package vm

import (
	"context"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// dialVsock connects to a guest over the host's vsock address family.
//
// This is the QEMU path and it exists because QEMU has no equivalent of
// Firecracker's host-initiated tunnel over a unix socket: the host talks to a
// vhost-vsock guest through AF_VSOCK itself. There is no Go standard library
// support for that family, so the socket is made by hand — and then handed to
// os.NewFile in non-blocking mode, so the runtime poller drives it and a
// sandbox with several concurrent streams does not park an OS thread each.
func dialVsock(_ context.Context, cid, port uint32) (io.ReadWriteCloser, error) {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("vsock socket: %w", err)
	}
	if err := unix.Connect(fd, &unix.SockaddrVM{CID: cid, Port: port}); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("connect to guest cid %d port %d: %w", cid, port, err)
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("set the vsock connection non-blocking: %w", err)
	}
	return os.NewFile(uintptr(fd), "vsock"), nil
}
