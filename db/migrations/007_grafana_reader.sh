#!/bin/sh
# Login role for Grafana's TimescaleDB panels (Phase 7).
# Read-only: a member of dashboard_reader, and every session is read-only with a
# statement timeout, so a heavy dashboard query can't hurt the pipeline.
# A shell script rather than .sql so the password comes from the environment.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v pw="${GRAFANA_DB_PASSWORD:-grafana}" <<'EOSQL'
SELECT format('CREATE ROLE grafana_reader LOGIN PASSWORD %L IN ROLE dashboard_reader', :'pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_reader') \gexec
ALTER ROLE grafana_reader SET default_transaction_read_only = on;
ALTER ROLE grafana_reader SET statement_timeout = '5s';
EOSQL
