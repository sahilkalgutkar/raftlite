// Package chaos holds raftlite's cluster-level failure tests.
//
// The tests elsewhere check that each piece behaves: the log truncates
// correctly, the codec round trips, a leader steps down when it should. These
// check the property the whole system exists to provide, under the conditions
// it exists to survive -- that a write which was acknowledged is never lost,
// no matter which nodes crash, restart or get cut off in the meantime.
//
// They run real nodes against real directories, writing real logs and
// snapshots to disk. Only the network is simulated, because partitioning a
// loopback socket on demand is awkward and the network is the one part with
// its own dedicated tests already.
package chaos
