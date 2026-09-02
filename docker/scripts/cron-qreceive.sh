#!/usr/bin/env bash
echo
echo "-------------------------------------------------------------"
echo " Running QReceive: $(date)"
echo "-------------------------------------------------------------"

# Marks a run as in progress so Watchtower's pre-update lifecycle hook
# (wait-for-qreceive.sh) can hold off restarting this container until the
# run finishes, instead of killing it mid-flight.
LOCK_FILE=/tmp/qreceive.lock
touch "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"' EXIT

cd /app && uv run qreceive.py
