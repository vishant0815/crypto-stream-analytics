#!/usr/bin/env bash
# Deploys the latest code: pulls main, rebuilds what changed, restarts it.
#
#   cd /opt/crypto-stream-analytics && ./deploy/update.sh
#
# --no-pull skips `git pull` (setup-server.sh uses it for the first deploy).
# Extra arguments go to `docker compose up`, e.g. ./deploy/update.sh --no-pull api
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "${1:-}" = "--no-pull" ]; then
    shift
else
    git pull --ff-only
fi

[ -f .env ] || { echo "no .env: run deploy/setup-server.sh first" >&2; exit 1; }

compose() {
    docker compose --env-file .env -f docker/docker-compose.yml -f docker/docker-compose.prod.yml "$@"
}

compose config --quiet          # stops here if a required setting is missing
compose build --pull
compose up -d --remove-orphans "$@"
docker image prune -f >/dev/null

echo
compose ps --format 'table {{.Name}}\t{{.Status}}'
