# Deploying Crypto Stream Analytics

This guide puts the full stack on one Linux server with HTTPS:

```
https://crypto.example.com            live dashboard
https://crypto.example.com/grafana/   pipeline health (Grafana)
```

It takes about 20 minutes, most of it the first image build.

## What you need

- **A server**: x86-64, Ubuntu 24.04, at least **4 vCPUs, 8 GB of memory and 80 GB of disk**. The stack runs 18 containers, three of them Spark, so smaller servers struggle. Use x86 rather than ARM: the Hadoop image is built for x86.
- **A domain or subdomain** you can add DNS records to, e.g. `crypto.yourdomain.com`.
- **SSH access** to the server as root (or a user with sudo).

### Choosing a server

Approximate monthly prices, checked November 2026. Prices change, so confirm them before you buy:

| Provider | Plan | vCPU / memory | About |
| --- | --- | --- | --- |
| Hetzner Cloud | CX33 (cost-optimised, EU) | 4 / 8 GB | €8.50, but often sold out |
| Hetzner Cloud | CPX32 (regular) | 4 / 8 GB | €35 |
| DigitalOcean | Basic Droplet | 4 / 8 GB | $48 |
| AWS Lightsail | Linux 8 GB bundle | 2 / 8 GB | $44 (only 2 vCPUs: Spark batches run slower) |

A 16 GB server gives the stack room to grow, but 8 GB with the swap file the setup script adds is enough.

## 1. Point the domain at the server

At your DNS provider, add an **A record** for the name you chose, with the server's public IPv4 address. Wait until it resolves:

```bash
dig +short crypto.yourdomain.com   # should print the server's IP
```

Caddy needs this to get the HTTPS certificate from Let's Encrypt.

## 2. Run the setup script

On the server:

```bash
curl -fsSL https://raw.githubusercontent.com/vishant0815/crypto-stream-analytics/main/deploy/setup-server.sh -o setup-server.sh
sudo DOMAIN=crypto.yourdomain.com ACME_EMAIL=you@example.com bash setup-server.sh
```

[`deploy/setup-server.sh`](../deploy/setup-server.sh) is safe to run again. It:

1. Installs Docker from Docker's own repository, automatic security updates, and a 4 GB swap file.
2. Turns on the firewall, allowing only SSH, HTTP and HTTPS.
3. Clones the project into `/opt/crypto-stream-analytics`.
4. Writes `.env` with random 32-character passwords, readable by root only. It refuses to start with default passwords.
5. Builds and starts the stack with [`docker/docker-compose.prod.yml`](../docker/docker-compose.prod.yml).
6. Installs a nightly database backup.

After a few minutes, open `https://crypto.yourdomain.com`. The Grafana admin password is `GRAFANA_ADMIN_PASSWORD` in `/opt/crypto-stream-analytics/.env`; anyone can view the dashboards without logging in.

## 3. Alert emails (optional)

Prometheus watches the pipeline with 10 alert rules, and Alertmanager emails you when one fires and again when it resolves. Add these to `.env`, then run `./deploy/update.sh`:

```bash
ALERT_EMAIL_TO=you@example.com
SMTP_HOST=smtp.gmail.com:587
SMTP_USER=yourname@gmail.com
SMTP_PASSWORD=your-app-password
```

For Gmail, `SMTP_PASSWORD` must be an [app password](https://support.google.com/accounts/answer/185833), not your normal password. Alerts that fire together arrive in one email, and a still-firing alert is repeated every 4 hours. When a whole service is down, the warnings it causes are held back so you get the cause, not a flood.

## How production differs from local

| | Local (`docker-compose.yml`) | Production (adds `docker-compose.prod.yml`) |
| --- | --- | --- |
| Open to the internet | Everything, on localhost | Only Caddy, on 80 and 443 |
| HTTPS | No | Let's Encrypt certificate, renewed automatically; HSTS and security headers |
| Admin pages (Prometheus, Alertmanager, Spark UI, HDFS, database) | Open | The server's `127.0.0.1` only, through an SSH tunnel |
| API, Kafka | Published | Internal only; the API is reached through `/api` and `/ws` |
| Kafka UI (no login) | Running | Off; `--profile debug` starts it on `127.0.0.1` |
| Passwords | Defaults | Required, random |
| Memory | Unlimited | A limit on every container |
| Logs | Unlimited | 3 × 10 MB per container |

The dashboard's nginx also limits each visitor to 10 API requests a second (bursts of 40) and 5 live connections, in development and production alike. Docker publishes ports past the `ufw` firewall, which is why every admin port is bound to `127.0.0.1` instead of relying on the firewall.

## Day to day

```bash
cd /opt/crypto-stream-analytics

./deploy/update.sh            # pull main, rebuild what changed, restart it

# Status and logs
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml ps
docker logs -f spark-streaming
docker stats --no-stream     # memory and CPU per container
```

**Admin pages.** From your own computer, open a tunnel, then browse to `localhost`:

```bash
ssh -L 9090:localhost:9090 -L 9093:localhost:9093 -L 4040:localhost:4040 -L 9870:localhost:9870 root@crypto.yourdomain.com
```

Prometheus is on 9090, Alertmanager on 9093, the Spark UI on 4040 and HDFS on 9870. For Kafka UI, start it with `--profile debug up -d kafka-ui` and tunnel 8080.

**Backups.** [`deploy/backup.sh`](../deploy/backup.sh) dumps TimescaleDB at 03:30 every night to `/var/backups/crypto-stream-analytics` and keeps 7 days. The restore steps are at the top of the script. The backups sit on the same server, so copy them elsewhere (for example `scp` or object storage) if the data matters to you. The HDFS archive can be rebuilt, but only from data still in Kafka (7 days).

**Cost.** Cloud servers bill while they exist, even when stopped. To pause the project for a while, take a snapshot and delete the server, then restore it from the snapshot later.

## Troubleshooting

- **No HTTPS certificate.** Check that the A record points at the server (`dig +short`), that ports 80 and 443 are open in your provider's firewall too, and read `docker logs caddy`. Caddy keeps retrying.
- **`required variable ... is missing a value`.** A required setting is missing from `.env`; the message names it.
- **A container keeps restarting.** Look at `docker logs <name>`. If it was killed for memory (`docker inspect <name> --format '{{.State.OOMKilled}}'`), use a bigger server or raise its `mem_limit` in `docker-compose.prod.yml`.
- **Grafana shows "No data".** Open Prometheus through the tunnel, go to Status → Targets, and check which service isn't being scraped.
