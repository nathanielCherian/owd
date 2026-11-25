//go:build linux

package main

import (
	"net"
	"syscall"
	"unsafe"
)

// enableRxTimestamps asks the kernel to stamp every received datagram
// (SO_TIMESTAMPNS), which keeps scheduler latency out of the OWD numbers.
func enableRxTimestamps(c *net.UDPConn) {
	raw, err := c.SyscallConn()
	if err != nil {
		return
	}
	raw.Control(func(fd uintptr) {
		syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_TIMESTAMPNS, 1)
	})
}

// rxTimestamp extracts the kernel receive time (unix ns) from the control
// message buffer, or returns 0 if there is none.
func rxTimestamp(oob []byte) int64 {
	msgs, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return 0
	}
	for _, m := range msgs {
		if m.Header.Level == syscall.SOL_SOCKET && m.Header.Type == syscall.SO_TIMESTAMPNS &&
			len(m.Data) >= int(unsafe.Sizeof(syscall.Timespec{})) {
			ts := (*syscall.Timespec)(unsafe.Pointer(&m.Data[0]))
			return ts.Nano()
		}
	}
	return 0
}
