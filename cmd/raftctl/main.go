// Command raftctl is the operator's view of a raftlite cluster.
//
//	raftctl --endpoints 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 status
//	raftctl --endpoints 127.0.0.1:8001 put greeting "hello"
//	raftctl --endpoints 127.0.0.1:8001 get greeting
//	raftctl --endpoints 127.0.0.1:8001 member-add 4 127.0.0.1:9004 127.0.0.1:8004
package main

import (
	"os"

	"github.com/sahilkalgutkar/raftlite/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}
