//go:build !linux

package main

import "net"

// Kernel receive timestamps are only wired up on Linux; elsewhere the
// userspace receive time is used.
func enableRxTimestamps(c *net.UDPConn) {}

func rxTimestamp(oob []byte) int64 { return 0 }
