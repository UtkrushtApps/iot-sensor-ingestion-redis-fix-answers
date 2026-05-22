#!/usr/bin/env bash
set -euo pipefail

echo "[run.sh] Starting Redis via docker compose..."
docker compose up -d

echo "[run.sh] Waiting for Redis to be ready..."
MAX_ATTEMPTS=30
ATTEMPT=0
until docker exec redis-iot redis-cli ping 2>/dev/null | grep -q PONG; do
  ATTEMPT=$((ATTEMPT + 1))
  if [ "$ATTEMPT" -ge "$MAX_ATTEMPTS" ]; then
    echo "[run.sh] ERROR: Redis did not become ready in time."
    exit 1
  fi
  echo "[run.sh] Redis not ready yet, retrying (${ATTEMPT}/${MAX_ATTEMPTS})..."
  sleep 1
done

echo "[run.sh] Redis is ready."

if [ -f requirements.txt ]; then
  echo "[run.sh] Installing Python dependencies..."
  pip install -r requirements.txt --quiet
fi

echo "[run.sh] Setup complete. Run: python src/main.py"
