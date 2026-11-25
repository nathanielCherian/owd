// owd measures one-way delay between a client and a long-running server.
//
// The client sends timestamped UDP probes. The server reports what it saw
// back over a TCP control channel (per-probe OWD plus periodic stats) and
// also echoes each probe over UDP with its own receive/send timestamps, so
// the client gets uplink OWD, downlink OWD, RTT and per-direction loss.
package main

import (
	"fmt"
	"os"
)

var version = "1.0.0"

func usage() {
	fmt.Fprintf(os.Stderr, `owd %s - one-way delay measurement

usage:
  owd server [-port 9910] [-log-dir DIR] [...]   run the measurement server
  owd client -host HOST [-port 9910] [-i 10ms] [-t 10s] [...]
  owd version

Run "owd server -h" or "owd client -h" for all options.
`, version)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "server":
		runServer(os.Args[2:])
	case "client":
		runClient(os.Args[2:])
	case "version", "-v", "--version":
		fmt.Println(version)
	default:
		usage()
		os.Exit(2)
	}
}
