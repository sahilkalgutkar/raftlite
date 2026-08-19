// Command raftlited runs one raftlite server: a consensus node plus the HTTP
// API in front of it.
//
// Three founding members, each in its own terminal:
//
//	raftlited --id 1 --raft-addr 127.0.0.1:9001 --http-addr 127.0.0.1:8001 \
//	  --peer 1,127.0.0.1:9001,127.0.0.1:8001 \
//	  --peer 2,127.0.0.1:9002,127.0.0.1:8002 \
//	  --peer 3,127.0.0.1:9003,127.0.0.1:8003 --bootstrap
//
// A fourth server joining later needs only one address to find the rest:
//
//	raftlited --id 4 --raft-addr 127.0.0.1:9004 --http-addr 127.0.0.1:8004 \
//	  --join 127.0.0.1:8001
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sahilkalgutkar/raftlite/internal/server"
)

func main() {
	cfg, err := server.ParseFlags(os.Args[1:], os.Stderr)
	if err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintf(os.Stderr, "raftlited: %v\n", err)
		}
		os.Exit(2)
	}

	// A second signal is deliberately left to the default handler, so an
	// operator can always kill a shutdown that is taking too long.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := server.Run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "raftlited: %v\n", err)
		os.Exit(1)
	}
}
