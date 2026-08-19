package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

const usage = `raftctl drives a raftlite cluster.

Usage:
  raftctl [flags] <command> [arguments]

Commands:
  put <key> <value>        write a value; use - as the value to read stdin
  get <key>                read a value
  del <key>                delete a key
  status                   what each endpoint thinks the cluster looks like
  members                  list the cluster configuration
  member-add <id> <raft-addr> <http-addr>
                           add a server, as a learner unless --voting is set
  member-promote <id>      turn a learner into a voter
  member-remove <id>       remove a server

Flags:
  --endpoints  comma separated node addresses (default 127.0.0.1:8001)
  --timeout    per request timeout (default 5s)
  --stale      on get, accept a possibly out of date value from any node
  --prev       on put, only write if the key currently holds this value
  --absent     on put, only write if the key does not exist
  --voting     on member-add, join as a voter rather than a learner
  --json       print raw JSON instead of a table
`

// Run executes one raftctl invocation and returns a process exit code.
//
// It takes its arguments and streams as parameters rather than reading globals
// so the whole command surface can be tested as a function call.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("raftctl", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }

	var (
		endpoints = fs.String("endpoints", "127.0.0.1:8001", "comma separated node addresses")
		timeout   = fs.Duration("timeout", 5*time.Second, "per request timeout")
		stale     = fs.Bool("stale", false, "accept a possibly out of date value")
		prev      = fs.String("prev", "", "only write if the key currently holds this value")
		absent    = fs.Bool("absent", false, "only write if the key does not exist")
		voting    = fs.Bool("voting", false, "add the member as a voter")
		asJSON    = fs.Bool("json", false, "print raw JSON")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}

	rest := fs.Args()
	if len(rest) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}

	client, err := NewClient(strings.Split(*endpoints, ","), *timeout)
	if err != nil {
		fmt.Fprintf(stderr, "raftctl: %v\n", err)
		return 2
	}

	opts := options{
		stale:  *stale,
		prev:   prev,
		absent: *absent,
		voting: *voting,
		json:   *asJSON,
		prevSet: func() bool {
			set := false
			fs.Visit(func(f *flag.Flag) {
				if f.Name == "prev" {
					set = true
				}
			})
			return set
		}(),
	}

	if err := dispatch(client, rest, opts, stdin, stdout); err != nil {
		fmt.Fprintf(stderr, "raftctl: %v\n", err)
		return 1
	}
	return 0
}

type options struct {
	stale   bool
	prev    *string
	prevSet bool
	absent  bool
	voting  bool
	json    bool
}

func dispatch(c *Client, args []string, opts options, stdin io.Reader, out io.Writer) error {
	switch args[0] {
	case "put":
		return cmdPut(c, args[1:], opts, stdin, out)
	case "get":
		return cmdGet(c, args[1:], opts, out)
	case "del", "delete":
		return cmdDelete(c, args[1:], opts, out)
	case "status":
		return cmdStatus(c, opts, out)
	case "members":
		return cmdMembers(c, opts, out)
	case "member-add":
		return cmdMemberAdd(c, args[1:], opts, out)
	case "member-promote":
		return cmdMemberChange(c, args[1:], opts, out, c.PromoteMember)
	case "member-remove":
		return cmdMemberChange(c, args[1:], opts, out, c.RemoveMember)
	default:
		return fmt.Errorf("unknown command %q; run with no arguments for usage", args[0])
	}
}

func cmdPut(c *Client, args []string, opts options, stdin io.Reader, out io.Writer) error {
	if len(args) != 2 {
		return errors.New("put takes a key and a value")
	}
	key, raw := args[0], args[1]

	value := []byte(raw)
	if raw == "-" {
		read, err := io.ReadAll(stdin)
		if err != nil {
			return fmt.Errorf("reading the value from stdin: %w", err)
		}
		value = read
	}

	cond := Condition{}
	switch {
	case opts.absent && opts.prevSet:
		return errors.New("--absent and --prev contradict each other")
	case opts.absent:
		cond.MustBeAbsent = true
	case opts.prevSet:
		cond.Prev = opts.prev
	}

	res, err := c.Put(key, value, cond)
	if err != nil {
		return err
	}
	if opts.json {
		return writeJSON(out, res)
	}
	verb := "wrote"
	if res.Existed {
		verb = "updated"
	}
	fmt.Fprintf(out, "%s %s at revision %d\n", verb, key, res.Revision)
	return nil
}

func cmdGet(c *Client, args []string, opts options, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("get takes exactly one key")
	}
	res, found, err := c.Get(args[0], opts.stale)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("key %q not found", args[0])
	}
	if opts.json {
		return writeJSON(out, map[string]any{
			"key": args[0], "value": string(res.Value),
			"revision": res.Revision, "consistency": res.Consistency,
		})
	}
	// The bare value goes to stdout so the output pipes into anything.
	fmt.Fprintf(out, "%s\n", res.Value)
	return nil
}

func cmdDelete(c *Client, args []string, opts options, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("del takes exactly one key")
	}
	res, existed, err := c.Delete(args[0])
	if err != nil {
		return err
	}
	if !existed {
		return fmt.Errorf("key %q not found", args[0])
	}
	if opts.json {
		return writeJSON(out, res)
	}
	fmt.Fprintf(out, "deleted %s at revision %d\n", args[0], res.Revision)
	return nil
}

func cmdStatus(c *Client, opts options, out io.Writer) error {
	type row struct {
		Endpoint string `json:"endpoint"`
		Status   Status `json:"status,omitzero"`
		Err      string `json:"error,omitempty"`
	}

	rows := make([]row, 0, len(c.Endpoints()))
	reachable := 0
	for _, endpoint := range c.Endpoints() {
		st, err := c.StatusOf(endpoint)
		if err != nil {
			rows = append(rows, row{Endpoint: endpoint, Err: err.Error()})
			continue
		}
		reachable++
		rows = append(rows, row{Endpoint: endpoint, Status: st})
	}

	if opts.json {
		return writeJSON(out, rows)
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ENDPOINT\tID\tROLE\tTERM\tLEADER\tCOMMIT\tAPPLIED\tSNAPSHOT\tKEYS")
	for _, r := range rows {
		if r.Err != "" {
			fmt.Fprintf(tw, "%s\t-\tunreachable\t-\t-\t-\t-\t-\t-\n", r.Endpoint)
			continue
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%d\n",
			r.Endpoint, r.Status.ID, r.Status.Role, r.Status.Term, r.Status.Leader,
			r.Status.Commit, r.Status.Applied, r.Status.Snapshot, r.Status.Keys)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if reachable == 0 {
		return errors.New("no endpoint answered")
	}
	return nil
}

func cmdMembers(c *Client, opts options, out io.Writer) error {
	members, err := c.Members()
	if err != nil {
		return err
	}
	if opts.json {
		return writeJSON(out, members)
	}
	return printMembers(out, members)
}

func cmdMemberAdd(c *Client, args []string, opts options, out io.Writer) error {
	if len(args) != 3 {
		return errors.New("member-add takes an id, a raft address and an http address")
	}
	id, err := parseID(args[0])
	if err != nil {
		return err
	}
	members, err := c.AddMember(id, args[1], args[2], opts.voting)
	if err != nil {
		return err
	}
	if opts.json {
		return writeJSON(out, members)
	}
	role := "learner"
	if opts.voting {
		role = "voter"
	}
	fmt.Fprintf(out, "added %d as a %s\n\n", id, role)
	return printMembers(out, members)
}

func cmdMemberChange(c *Client, args []string, opts options, out io.Writer, fn func(uint64) ([]Member, error)) error {
	if len(args) != 1 {
		return errors.New("this command takes exactly one member id")
	}
	id, err := parseID(args[0])
	if err != nil {
		return err
	}
	members, err := fn(id)
	if err != nil {
		return err
	}
	if opts.json {
		return writeJSON(out, members)
	}
	return printMembers(out, members)
}

func printMembers(out io.Writer, members []Member) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tROLE\tRAFT ADDRESS\tCLIENT ADDRESS")
	for _, m := range members {
		role := "voter"
		if m.Learner {
			role = "learner"
		}
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\n", m.ID, role, m.Addr, m.ClientAddr)
	}
	return tw.Flush()
}

func parseID(raw string) (uint64, error) {
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		return 0, fmt.Errorf("member id must be a positive integer, got %q", raw)
	}
	return id, nil
}

func writeJSON(out io.Writer, v any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
