#!/usr/bin/env bash
# Watchtower pre-update lifecycle hook (see the qreceive service's labels in
# docker-compose.yaml). Runs inside the current container, via `docker exec`,
# before Watchtower stops it for an update. Blocks while cron-qreceive.sh's
# lock file is present, so a deploy waits out an in-progress run rather than
# killing it mid-flight (partial texts sent, DB writes left half-done).
#
# Watchtower still enforces its own pre-update-timeout on this hook, so the
# wait is bounded, not a hard guarantee: see that label's value for the cap.
LOCK_FILE=/tmp/qreceive.lock

while [ -f "$LOCK_FILE" ]; do
  echo "qreceive run in progress, waiting before update..."
  sleep 5
done

echo "No qreceive run in progress, proceeding with update."
