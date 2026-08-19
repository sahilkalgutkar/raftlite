#!/usr/bin/env bash
#
# Brings up a three node raftlite cluster, writes to it, kills the leader, and
# shows the cluster electing a replacement and carrying on without losing the
# write. It is the thirty second version of what the tests assert in detail.
#
#   ./scripts/demo.sh
#
set -euo pipefail

cd "$(dirname "$0")/.."

ENDPOINTS="127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003"
RAFTCTL=${RAFTCTL:-./bin/raftctl}

say() { printf '\n\033[1m%s\033[0m\n' "$*"; }

if [ ! -x "$RAFTCTL" ]; then
  say "Building raftctl"
  go build -o bin/raftctl ./cmd/raftctl
fi

say "Starting three nodes"
docker compose up -d --build --wait

say "Cluster status"
"$RAFTCTL" --endpoints "$ENDPOINTS" status

say "Writing a few keys"
"$RAFTCTL" --endpoints "$ENDPOINTS" put greeting "hello from raftlite"
"$RAFTCTL" --endpoints "$ENDPOINTS" put colour green
"$RAFTCTL" --endpoints "$ENDPOINTS" get greeting

say "Taking a lock with compare-and-swap"
"$RAFTCTL" --endpoints "$ENDPOINTS" --absent put lock held-by-a
echo "a second holder should now be refused:"
"$RAFTCTL" --endpoints "$ENDPOINTS" --absent put lock held-by-b || true

leader_container() {
  for port in 8001 8002 8003; do
    role=$(curl -fsS "http://127.0.0.1:${port}/status" | sed -n 's/.*"role":"\([a-z]*\)".*/\1/p')
    if [ "$role" = "leader" ]; then
      id=$(curl -fsS "http://127.0.0.1:${port}/status" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
      echo "raftlite-${id}"
      return
    fi
  done
}

LEADER=$(leader_container)
say "Killing the leader (${LEADER})"
docker compose kill "$LEADER" >/dev/null

say "Waiting for a new leader"
for _ in $(seq 1 40); do
  sleep 0.5
  if "$RAFTCTL" --endpoints "$ENDPOINTS" --timeout 2s status 2>/dev/null | grep -q leader; then
    break
  fi
done
"$RAFTCTL" --endpoints "$ENDPOINTS" status

say "The committed write is still there, and the cluster still accepts new ones"
"$RAFTCTL" --endpoints "$ENDPOINTS" get greeting
"$RAFTCTL" --endpoints "$ENDPOINTS" put after-failover "still working"

say "Bringing the old leader back"
docker compose start "$LEADER" >/dev/null
sleep 3
"$RAFTCTL" --endpoints "$ENDPOINTS" status

say "Done. Tear down with: docker compose down -v"
