#!/usr/bin/env bash
# Backs up TimescaleDB to a compressed dump and keeps the last KEEP_DAYS days.
# setup-server.sh installs it as a nightly cron job.
#
# Restore into a fresh stack (TimescaleDB's own procedure):
#   docker exec -i timescaledb psql -U crypto -d crypto -c "SELECT timescaledb_pre_restore();"
#   gunzip -c <file>.sql.gz | docker exec -i timescaledb psql -U crypto -d crypto
#   docker exec -i timescaledb psql -U crypto -d crypto -c "SELECT timescaledb_post_restore();"
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/crypto-stream-analytics}"
BACKUP_DIR="${BACKUP_DIR:-/var/backups/crypto-stream-analytics}"
KEEP_DAYS="${KEEP_DAYS:-7}"

user=$(grep '^POSTGRES_USER=' "$APP_DIR/.env" | cut -d= -f2-)
db=$(grep '^POSTGRES_DB=' "$APP_DIR/.env" | cut -d= -f2-)
user=${user:-crypto}
db=${db:-crypto}

mkdir -p "$BACKUP_DIR"
chmod 700 "$BACKUP_DIR"
file="$BACKUP_DIR/$db-$(date -u +%Y%m%d-%H%M).sql.gz"

# Plain SQL dump from inside the container; the Hive metastore schema comes along
docker exec timescaledb pg_dump -U "$user" -d "$db" --no-owner | gzip -6 > "$file.tmp"
mv "$file.tmp" "$file"
find "$BACKUP_DIR" -name "$db-*.sql.gz" -mtime +"$KEEP_DAYS" -delete

echo "$(date -u +%FT%TZ) backup written: $file ($(du -h "$file" | cut -f1))"
