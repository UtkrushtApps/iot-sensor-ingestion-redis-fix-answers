#!/usr/bin/env bash
set -euo pipefail

echo "[kill.sh] Stopping and removing containers..."
docker compose down

echo "[kill.sh] Done."
