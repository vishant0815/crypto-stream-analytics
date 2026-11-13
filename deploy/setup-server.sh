#!/usr/bin/env bash
# One-time setup of a fresh Ubuntu 24.04 server, then the first deploy.
#
#   curl -fsSL https://raw.githubusercontent.com/vishant0815/crypto-stream-analytics/main/deploy/setup-server.sh -o setup-server.sh
#   sudo DOMAIN=crypto.example.com ACME_EMAIL=you@example.com bash setup-server.sh
#
# Point DOMAIN's DNS A record at this server first, so Caddy can get the HTTPS
# certificate. Safe to run again: every step checks what is already done.
#
# What it does:
#   1. Installs Docker, a firewall (SSH, HTTP, HTTPS only), automatic security
#      updates and a swap file
#   2. Clones the project into /opt/crypto-stream-analytics
#   3. Writes .env with strong random passwords (kept if .env already exists)
#   4. Builds and starts the production stack
#   5. Installs a nightly database backup
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/vishant0815/crypto-stream-analytics.git}"
APP_DIR="${APP_DIR:-/opt/crypto-stream-analytics}"
SWAP_GB="${SWAP_GB:-4}"

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
die() { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "run as root: sudo DOMAIN=... ACME_EMAIL=... bash $0"
[ -n "${DOMAIN:-}" ] || die "set DOMAIN, e.g. DOMAIN=crypto.example.com"
[ -n "${ACME_EMAIL:-}" ] || die "set ACME_EMAIL, for Let's Encrypt certificate notices"
grep -q 'Ubuntu' /etc/os-release || echo "warning: tested on Ubuntu 24.04 only"

mem_gb=$(awk '/MemTotal/ {printf "%d", $2/1024/1024 + 0.5}' /proc/meminfo)
[ "$mem_gb" -ge 7 ] || echo "warning: ${mem_gb} GB of memory; the full stack needs 8 GB or more"

say "Packages, automatic security updates"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq ca-certificates curl git openssl ufw unattended-upgrades dnsutils >/dev/null
dpkg-reconfigure -f noninteractive unattended-upgrades

say "Docker"
if ! command -v docker >/dev/null; then
    # Docker's own apt repository, as in docs.docker.com/engine/install/ubuntu
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
    . /etc/os-release
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${UBUNTU_CODENAME:-$VERSION_CODENAME} stable" \
        > /etc/apt/sources.list.d/docker.list
    apt-get update -qq
    apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin >/dev/null
fi
systemctl enable --now docker >/dev/null
docker compose version

say "Firewall: SSH, HTTP and HTTPS only"
ufw allow OpenSSH >/dev/null
ufw allow 80/tcp >/dev/null
ufw allow 443/tcp >/dev/null
ufw allow 443/udp >/dev/null   # HTTP/3
ufw --force enable >/dev/null
ufw status | sed -n '1,12p'
# Docker publishes ports past ufw; docker-compose.prod.yml binds every admin port
# to 127.0.0.1, so only Caddy (80/443) is reachable from outside.

say "Swap (${SWAP_GB} GB), so a memory spike slows things down instead of killing a service"
if ! swapon --show | grep -q .; then
    fallocate -l "${SWAP_GB}G" /swapfile
    chmod 600 /swapfile
    mkswap /swapfile >/dev/null
    swapon /swapfile
    grep -q '^/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
    sysctl -q vm.swappiness=10
    echo 'vm.swappiness=10' > /etc/sysctl.d/99-swappiness.conf
fi
swapon --show

say "Project in $APP_DIR"
if [ -d "$APP_DIR/.git" ]; then
    git -C "$APP_DIR" pull --ff-only
else
    git clone "$REPO_URL" "$APP_DIR"
fi
cd "$APP_DIR"

say "Settings (.env)"
if [ -f .env ]; then
    echo ".env exists, keeping it"
else
    secret() { openssl rand -base64 33 | tr -dc 'A-Za-z0-9' | head -c 32; }
    cp .env.example .env
    set_env() {  # set KEY=VALUE in .env, adding the line if it isn't there
        if grep -q "^$1=" .env; then sed -i "s|^$1=.*|$1=$2|" .env; else echo "$1=$2" >> .env; fi
    }
    set_env DOMAIN "$DOMAIN"
    set_env ACME_EMAIL "$ACME_EMAIL"
    set_env PUBLIC_URL "https://$DOMAIN"
    pg_password=$(secret)
    set_env POSTGRES_PASSWORD "$pg_password"
    set_env DATABASE_URL "postgresql://crypto:$pg_password@localhost:5432/crypto"
    set_env GRAFANA_DB_PASSWORD "$(secret)"
    set_env GRAFANA_ADMIN_PASSWORD "$(secret)"
    chmod 600 .env
    echo "wrote .env with new random passwords (only root can read it)"
fi

say "Checking .env has no default passwords"
for key in POSTGRES_PASSWORD GRAFANA_DB_PASSWORD GRAFANA_ADMIN_PASSWORD; do
    value=$(grep "^$key=" .env | cut -d= -f2- || true)
    case "$value" in
        ""|crypto|grafana|admin|changeme) die "$key in .env is empty or a default; set a strong one" ;;
    esac
done
echo "ok"

say "DNS"
public_ip=$(curl -fsS4 https://api.ipify.org || true)
dns_ip=$(dig +short A "$DOMAIN" | tail -1)
echo "this server: ${public_ip:-unknown}, $DOMAIN points to: ${dns_ip:-nothing}"
if [ -n "$public_ip" ] && [ "$dns_ip" != "$public_ip" ]; then
    echo "warning: $DOMAIN does not point here yet. Add an A record for it with $public_ip;"
    echo "         Caddy keeps retrying and gets the certificate once DNS is right."
fi

say "Building and starting the stack (the first build takes a while)"
./deploy/update.sh --no-pull

say "Nightly database backup"
install -m 0755 deploy/backup.sh /usr/local/bin/crypto-backup
cat > /etc/cron.d/crypto-backup <<EOF
# Nightly TimescaleDB backup, kept for 7 days (see deploy/backup.sh)
30 3 * * * root APP_DIR=$APP_DIR /usr/local/bin/crypto-backup >> /var/log/crypto-backup.log 2>&1
EOF
echo "backups go to /var/backups/crypto-stream-analytics at 03:30 every night"

cat <<EOF

Done. In a few minutes, once the services are up and the certificate is issued:

  Live dashboard   https://$DOMAIN
  Grafana          https://$DOMAIN/grafana/   (admin password: GRAFANA_ADMIN_PASSWORD in $APP_DIR/.env)

Admin pages are private to the server. Open them from your own computer with an
SSH tunnel, then browse to localhost:

  ssh -L 9090:localhost:9090 -L 9093:localhost:9093 -L 4040:localhost:4040 -L 9870:localhost:9870 root@$DOMAIN

  Prometheus 9090, Alertmanager 9093, Spark UI 4040, HDFS 9870

To get alert emails, add ALERT_EMAIL_TO, SMTP_HOST, SMTP_USER and SMTP_PASSWORD to
.env (see .env.example) and run ./deploy/update.sh.
EOF
