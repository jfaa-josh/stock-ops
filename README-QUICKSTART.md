# StockOps Local with Docker Compose

This directory provides a **Docker Compose** setup for running **StockOps** locally and in production.
It pulls pre-built, pinned images released to **GitHub Container Registry (GHCR)** for quick deployment or evaluation without cloning the repository.

---

## Overview

StockOps is a market data, trade execution, and strategy suite. Feature areas are configured through a shared Streamlit UI and orchestrated by Prefect, and their work can be triggered immediately or scheduled.

### Feature Areas

- **Data Pipeline** — concurrent real-time and historical market-data ingestion from multiple providers, buffered and written to SQLite. See [README-DATAPIPELINE.md](README-DATAPIPELINE.md).
- **Account / Trade Execution** — account queries, position/balance tracking, and order placement. See [README-TRADE-EXECUTION.md](README-TRADE-EXECUTION.md).
- **Models** — predictive/quantitative models consumed by strategies. See [README-MODELS.md](README-MODELS.md).
- **Strategies** — strategy definition, backtesting, and live signal generation. See [README-STRATEGIES.md](README-STRATEGIES.md).

### Execution Summary
- User creates `.env` from `.env.example` and fills in provider keys, and production domain and letsencrypt email (if deploying to production)
- User sets `PRODUCTION_DOMAIN` and `LETSENCRYPT_EMAIL` for automated TLS
- Launch with `stockops.sh`, then configure and trigger/schedule work for each active feature area from the Streamlit UI
- Prefect orchestrates concurrent flow run executions across feature areas

See each feature area's own guide (linked above) for how its deployments/flows work internally.

---

## Prerequisites

- Docker Engine and Docker Compose v2
  - Windows/macOS: [Docker Desktop](https://www.docker.com/products/docker-desktop/)
  - Linux: Docker Engine + the `docker compose` plugin

  Verify installation:
  ```bash
  docker --version
  docker compose version
  ```

  If you see `permission denied` on `/var/run/docker.sock`, add your user to the docker group:
  ```bash
  sudo groupadd docker
  sudo usermod -aG docker ubuntu
  newgrp docker
  ```

  Windows/WSL note: if you see line-ending warnings when running `git add` or `/bin/sh^M` errors, set Git to keep LF:
  ```bash
  git config --global core.autocrlf input
  ```

---

## Quickstart
Use `stockops.sh` to start the stack. It sets the correct nginx mode and Prefect UI URL based on `local` or `prod`, and passes through docker compose flags.
For any hosting platform, ensure inbound TCP ports 80 (HTTP) and 443 (HTTPS) are open for your instance before you start.
See [Appendix: AWS Ubuntu + GitHub CLI deployment](#appendix-aws-ubuntu--github-cli-deployment) for an example production deployment ssh call sequence.
1) Create an empty folder for deployment
2) Download `docker-compose.vx.y.z.yml`, `stockops.sh`, and `.env.example` from the GitHub repository [releases](https://github.com/jfaa-josh/stock-ops/releases) page
3) Make the script executable:
   ```bash
   chmod +x stockops.sh
   ```
4) Rename `.env.example` to `.env` and update placeholders ([see instructions](#2-configure-environment))
5) If using `prod`, set `PRODUCTION_DOMAIN` and `LETSENCRYPT_EMAIL` ([see instructions](#tls-certificates))
6) If using `prod`, create the nginx basic-auth file:
   ```bash
   mkdir -p secrets
   htpasswd -Bc ./secrets/prod.htpasswd produser
   ```
   Note: always start with `stockops.sh` (not raw `docker compose`) so preflight checks can catch missing/invalid auth file paths before containers start.
7) Launch full production datapipeline stack in detached mode:
   ```bash
   ./stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml --providers eodhd --profile datapipe-visualize-data up -d
   ```
   (Use `local` instead of `prod` to start the local nginx mode and self-signed `stockops.local` certs.)

   For host-managed nginx (recommended on IPv6-only hosts without Docker IPv6 NAT), use:
   ```bash
   PROXY_MODE=host ./stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml --providers eodhd --profile datapipe-visualize-data up -d
   ```
8) Access the UI:
   - Local: `https://stockops.local/`
   - Production: `https://your.production.domain/`

---

## Execution Details

### 1. Download a Pinned Compose File
From the GitHub repository [releases](https://github.com/jfaa-josh/stock-ops/releases) page, grab the compose file for the desired version, plus the wrapper and env example.

Example assets:
- `docker-compose.v1.2.3.yml`
- `stockops.sh`
- `.env.example`
- `nginx/host/stockops-host.conf.template` (for host nginx in IPv6-only deployments)

Place all files in the same directory. These files reference immutable image tags—no local build required.

### 2. Configure Environment
Rename `.env.example` to `.env` in the same directory as the compose file, then update the placeholder values:

`PRODUCTION_DOMAIN` is used by the `nginx-prod` mode to generate the `server_name` and certificate paths inside the baked prod nginx template, and certbot uses it to request and renew certificates under `/etc/letsencrypt/live/$PRODUCTION_DOMAIN` inside the production cert volume.
`LETSENCRYPT_EMAIL` is required for certbot to issue the initial production certificate. Local mode can keep placeholder values.
The wrapper sets `PREFECT_UI_SERVE_BASE`, `PREFECT_UI_API_URL`, and `PYTHONPATH` automatically.

Note: Your .env stays local and is never pushed to GitHub.

#### Providers:
Providers are part of the data pipeline feature area — see [README-DATAPIPELINE.md](README-DATAPIPELINE.md) for the currently available providers and how to select one.

### 3. Launch a Feature Area

| Feature | Description | Guide |
|---|---|---|
| Data Pipeline | ETL pipeline for streaming and historical stock price data, storage in a SQL database, and tools for reading this database. | [README-DATAPIPELINE.md](README-DATAPIPELINE.md) |

Each feature area's own guide covers its `stockops.sh` launch flags, containers, and profiles in full — see the table above for the current list.

Feature areas are turned on via `--providers`/`--profile` flags, and these are stackable: multiple providers and profiles can be combined in a single call (e.g. two data providers plus the DB browser at once), and future feature areas are expected to combine the same way.

Example — this launches the Data Pipeline feature area configured to use the EODHD provider:
```bash
./stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml --providers eodhd up -d
```
See [README-DATAPIPELINE.md](README-DATAPIPELINE.md) for this feature area's full set of provider/profile flags.

#### Project name (`-p`)
`-p <name>` is Docker Compose's own project-name flag — an arbitrary label grouping this run's containers, networks, and volumes together, independent of which feature areas or providers are active. It is not itself a feature selector: the example above uses `datapipe` purely as a clear, conventional label matching the feature area in use, not because `-p` activates it — that's what `--providers`/`--profile` do. Setting one is optional but recommended: it makes status/logs/stop/restart simpler to target and helps avoid accidentally missing containers grouped within a profile.

### 4. Additional Launch, Verify, and Access Details

#### Detatched mode (-d):
Running the wrapper with `-d` launches services in detached mode (background). Detached mode is ideal for long running service, where terminal closure will not interrupt service. Run without detached mode for short term debugging or for immediate access to streaming front-end container logs.

Note: regardless of detached mode, logs and other information is available for each container or volume in Docker Desktop if installed.

#### Check status:
```bash
./stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml ps
```

#### View logs (example: writer-service container):
```bash
./stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml logs -f writer-service
```

#### Stop / Restart / Remove:
Note: use the same `stockops.sh` wrapper and the same `-f` + `--providers`/`--profile` values you used for `up`, otherwise compose may not target the right stack.

*Stop (keep containers/data):*
```bash
./stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml stop
```

*Restart:*
```bash
./stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml restart
```

*Remove containers (preserve data):*
```bash
./stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml down
```

Note: On all `down` calls, do not postpend -v unless you intend to delete all storage volumes.

### Routing UI traffic through nginx

Responsibility for TLS is handled by the wrapper mode: `prod` renders the baked prod nginx template using `PRODUCTION_DOMAIN`, while `local` loads the baked local config (which already targets `stockops.local` and the bundled self-signed certs). Only the nginx container for the selected mode starts, so there is no port conflict.

The active nginx service listens only on host ports 80/443 and proxies:

A dedicated listener on port 80 immediately issues `301 https://$host$request_uri`, so all external traffic is funneled through the TLS listener on port 443 before reaching the proxy locations.

- `/` → Streamlit UI
- `/prefect/` → Prefect server API/UI
- `/sqlite/` → SQLite Browser (only available when `datapipe-visualize-data` is active)
- `/cadvisor/` → cAdvisor monitoring (available when `nginx-prod` is running)

The nginx mode is the only way to reach services from outside the Docker network; services remain internal by default. `nginx-local` uses a bundled htpasswd, and `nginx-prod` uses a host-managed htpasswd file at `./secrets/prod.htpasswd`.

#### Host-managed nginx (IPv6-friendly)
Some hosts cannot publish IPv6 ports through Docker due to missing kernel NAT targets. For those, run nginx on the host and proxy to the loopback ports exposed by the compose file.

Steps:
1. Start the stack in host-proxy mode:
   ```bash
   PROXY_MODE=host ./stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml --providers eodhd --profile datapipe-visualize-data up -d
   ```
2. Install nginx and certbot on the host.
3. Use `nginx/host/stockops-host.conf.template` as the server block template and replace `${PRODUCTION_DOMAIN}`.
4. Copy `./secrets/prod.htpasswd` to `/etc/nginx/prod.htpasswd`.
5. Request and renew certs with host certbot.

Notes:
1. The compose file binds Streamlit, Prefect, and SQLite Browser to `127.0.0.1`, so they are reachable only from the host. The host nginx proxies to:
   - `http://127.0.0.1:8501` for `/`
   - `http://127.0.0.1:4200` for `/api/` and `/prefect/`
   - `http://127.0.0.1:3000` for `/sqlite/` (only when `datapipe-visualize-data` is enabled)

#### Basic authorization

Local mode ships with a bundled `localadmin` user. It is seeded into a named volume on first start, so changes persist across container recreation. To update it:
```bash
./stockops.sh local -p datapipe -f docker-compose.vx.y.z.yml exec nginx-local \
  htpasswd -B /etc/nginx-local/htpasswd localadmin
```
For production with dockerized nginx (`nginx-prod`), it reads `./secrets/prod.htpasswd` from the host. Create this file before the first `prod` startup (the wrapper checks and fails fast if it is missing), and run the same command again any time you want to rotate credentials:
```bash
# Create or overwrite production credentials used by nginx-prod
mkdir -p secrets
htpasswd -Bc ./secrets/prod.htpasswd produser
```

For host-proxy mode (`PROXY_MODE=host`), the host nginx reads `/etc/nginx/prod.htpasswd`. Copy your generated file into place:
```bash
sudo cp ./secrets/prod.htpasswd /etc/nginx/prod.htpasswd
```

To reset local auth to the default `localadmin` user:
```bash
# Remove the existing htpasswd file from the local volume
docker run --rm -v datapipe_nginx_local_auth:/data alpine sh -c "rm -f /data/htpasswd"

# Re-seed from the baked default
./stockops.sh local -p datapipe -f docker-compose.vx.y.z.yml run --rm init-htpasswd-local
```

#### TLS certificates

TLS files are stored in the `certs_prod` named volume for production and are mounted straight into nginx at `/etc/letsencrypt`. The `nginx-prod` mode renders the baked prod template using `PRODUCTION_DOMAIN`.

##### Local
The `nginx-local` mode loads the baked local config, which already targets `stockops.local` and uses bundled self-signed certs. If desired, no modifications are required for local deployment.

The provided local pair has no CA; in order to avoid browser rejecting the certificate pair as untrustworthy, import into whatever trust store your OS/browser relies on. Example for WSL/Linux:
```bash
sudo mkdir -p /usr/local/share/ca-certificates
sudo cp ./certs/live/stockops.local/fullchain.pem /usr/local/share/ca-certificates/stockops.local.crt
cd ~ && sudo update-ca-certificates
```

After adding the certificate to your trust store, you must then resolve stockops.local to 127.0.0.1.

If you want to trust the bundled cert, you can extract it from the image:
```bash
./stockops.sh local -p datapipe -f docker-compose.vx.y.z.yml run --rm nginx-local \
  cat /etc/letsencrypt/fullchain.pem > stockops.local.crt
```

If you ever need to rebuild the local certificate (for example to change the subject), you will need a custom nginx image built from source with your new certs.

```bash
openssl req -x509 -nodes -newkey rsa:4096 \
  -keyout certs/live/stockops.local/privkey.pem \
  -out certs/live/stockops.local/fullchain.pem \
  -days 36500 -subj "/CN=stockops.local"
```

##### Production
For the `nginx-prod` mode, certificates are issued and renewed automatically by certbot. No certbot installation is required as docker services pull a certbot image. Simply set `PRODUCTION_DOMAIN` and `LETSENCRYPT_EMAIL` in `.env`, make sure your DNS points at the VM, and ensure ports 80/443 are open. On first run, the `certbot-init` service requests the initial certificate, then the `certbot` service renews on a 12-hour loop.

Use a real email you control for `LETSENCRYPT_EMAIL` (it receives expiration and recovery notices); see the Certbot/Let's Encrypt docs for account email guidance. `nginx-prod` reloads on a 12-hour loop (default `NGINX_RELOAD_INTERVAL_SECONDS=43200`) to pick up renewed certificates; override this in `.env` if needed.

### Upgrading to a New Version

1. Download the new pinned compose (e.g. docker-compose.v1.3.0.yml).
   - Reuse your existing .env (only update if release notes add variables).
2. For minimal interruption of service that is currently running:
  ```bash
  ./stockops.sh prod -p datapipe -f docker-compose.v1.3.0.yml --providers eodhd --profile datapipe-visualize-data up -d
   ```
   - Docker compares the images deployed in the current run to those in the new compose version, updates as needed, and restarts any updated containers.  All named volumes—and therefore your data—are preserved.

---

## Appendix: Example GitHub CLI deployment using AWS Lightsail Ubuntu

This appendix shows an example production ssh sequence to fetch release assets and launch.

### 1) Create deployment directory

```bash
mkdir -p ~/stock-ops
cd ~/stock-ops
```

### 2) Installs as needed

```bash
sudo apt-get update -y
sudo apt-get install -y curl ca-certificates gnupg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list >/dev/null

sudo apt-get install -y gh

sudo apt-get install -y apache2-utils
```

### 3) Set release/tag and asset names

```bash
export TAG="vx.y.z"
export OWNER="jfaa-josh"
export REPO="stock-ops"

export COMPOSE_ASSET="docker-compose.${TAG}.yml"
export ENV_ASSET=".env.example"
export RUN_ASSET="stockops.sh"
```

### 4) Authenticate and download release files

```bash
gh auth login -h github.com -p https -w
gh release download "$TAG" -R "$OWNER/$REPO" -p "$COMPOSE_ASSET" -p "$ENV_ASSET" -p "$RUN_ASSET"
ls -la
```

### 5) Create and edit `.env`

```bash
mv "$ENV_ASSET" .env
nano .env
```

Fill in at least your provider keys and production values `PRODUCTION_DOMAIN` and `LETSENCRYPT_EMAIL`.

### 6) Create production basic-auth credentials

```bash
mkdir -p secrets
htpasswd -Bc ./secrets/prod.htpasswd produser
```

### 7) AWS Lightsail firewall

Open these inbound ports in the Lightsail Networking tab:
- TCP 80 (HTTP)
- TCP 443 (HTTPS)
- TCP 22 (SSH) for administration

### 8) Configure Docker log rotation (recommended)

Limit container log growth to prevent disk/IO pressure:

```bash
sudo tee /etc/docker/daemon.json <<'EOF'
{
  "log-driver": "json-file",
  "log-opts": { "max-size": "10m", "max-file": "3" }
}
EOF

sudo systemctl restart docker
```

### 9) Disable fwupd-refresh (recommended)

```bash
sudo systemctl disable --now fwupd-refresh.timer
```

### 10) Limit journald size (recommended)

```bash
sudo mkdir -p /etc/systemd/journald.conf.d
printf "[Journal]\nSystemMaxUse=200M\nRuntimeMaxUse=50M\n" | sudo tee /etc/systemd/journald.conf.d/limits.conf
sudo systemctl restart systemd-journald
```

### 11) Launch (prod core profile)

```bash
chmod +x "$RUN_ASSET"
./"$RUN_ASSET" prod -p datapipe -f "$COMPOSE_ASSET" --providers eodhd up -d
```

If your VM has limited resources, recommend starting with just a provider (without also adding `--profile datapipe-visualize-data`).

### 12) Production login

Access `https://$PRODUCTION_DOMAIN/` and authenticate with:
- Username: `produser`
- Password: the one you set with `htpasswd` in step 6

---

## Appendix: Example IPv6-only VM deployment (host nginx)

This appendix shows a production ssh sequence for IPv6-only hosts where Docker cannot publish IPv6 ports. It uses host nginx to terminate TLS and proxy to loopback ports exposed by the compose file. When you run `PROXY_MODE=host`, the dockerized `nginx-prod`, `certbot-init`, and `certbot` services do not start, so you must initialize TLS certificates on the host.

### 1) Create deployment directory

```bash
mkdir -p ~/stock-ops
cd ~/stock-ops
```

### 2) Install dependencies

```bash
sudo apt-get update -y
sudo apt-get install -y curl ca-certificates gnupg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list >/dev/null
sudo apt-get install -y gh

sudo apt-get install -y apache2-utils nginx certbot
```

### 3) Set release/tag and asset names

```bash
export TAG="vx.y.z"
export OWNER="jfaa-josh"
export REPO="stock-ops"

export COMPOSE_ASSET="docker-compose.${TAG}.yml"
export ENV_ASSET=".env.example"
export RUN_ASSET="stockops.sh"
export HOST_NGINX_ASSET="stockops-host.conf.template"
```

### 4) Authenticate and download release files

```bash
gh auth login -h github.com -p https -w
gh release download "$TAG" -R "$OWNER/$REPO" -p "$COMPOSE_ASSET" -p "$ENV_ASSET" -p "$RUN_ASSET" -p "$HOST_NGINX_ASSET"
ls -la
```

### 5) Configure host and Docker DNS for IPv6

Set IPv6-capable DNS servers on the host, then configure Docker to use the same resolvers so containers can resolve AAAA records.

```bash
sudo resolvectl dns eth0 2a00:1098:2c::1 2a01:4f8:c2c:123f::1 2a00:1098:2b::1
sudo resolvectl domain eth0 "~."
sudo systemctl restart systemd-resolved

sudo tee /etc/docker/daemon.json >/dev/null <<'JSON'
{
  "dns": [
    "2a00:1098:2c::1",
    "2a01:4f8:c2c:123f::1",
    "2a00:1098:2b::1"
  ]
}
JSON

sudo systemctl restart docker
```

### 6) Create and edit `.env`

```bash
mv "$ENV_ASSET" .env
nano .env
```

Fill in at least your provider keys and production values `PRODUCTION_DOMAIN` and `LETSENCRYPT_EMAIL`.

### 7) Create production basic-auth credentials

```bash
mkdir -p secrets
htpasswd -Bc ./secrets/prod.htpasswd produser
```

### 8) Configure DNS and firewall

1. Create an AAAA record for `PRODUCTION_DOMAIN` pointing to your VM IPv6 address.
2. Ensure inbound IPv6 TCP ports 80 and 443 are allowed in your provider firewall.

### 9) Configure host nginx + TLS (first-time setup)

This step creates a temporary HTTP-only nginx site to complete the initial ACME challenge, issues a certificate with certbot using the webroot method, then swaps in the full TLS reverse-proxy config and enables WebSocket upgrades.

```bash
sudo tee /etc/nginx/sites-available/stockops-http >/dev/null <<'EOF'
server {
  listen 80;
  listen [::]:80;
  server_name ${PRODUCTION_DOMAIN};

  location /.well-known/acme-challenge/ {
    root /var/www/certbot;
  }

  location / {
    return 301 https://$host$request_uri;
  }
}
EOF

sudo mkdir -p /var/www/certbot
sudo cp ./secrets/prod.htpasswd /etc/nginx/prod.htpasswd
sudo ln -sf /etc/nginx/sites-available/stockops-http /etc/nginx/sites-enabled/stockops
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl start nginx

sudo certbot certonly --webroot -w /var/www/certbot \
  -d "$PRODUCTION_DOMAIN" --email "$LETSENCRYPT_EMAIL" \
  --agree-tos --no-eff-email --non-interactive

sudo tee /etc/nginx/conf.d/connection_upgrade.conf >/dev/null <<'EOF'
map $http_upgrade $connection_upgrade {
  default upgrade;
  ''      close;
}
EOF

set -a
. ./.env
set +a
envsubst '${PRODUCTION_DOMAIN}' < stockops-host.conf.template | sudo tee /etc/nginx/sites-available/stockops >/dev/null
sudo nginx -t
sudo systemctl reload nginx
```

### 10) Launch StockOps (host-proxy mode)

```bash
chmod +x "$RUN_ASSET"
PROXY_MODE=host ./"$RUN_ASSET" prod -p datapipe -f "$COMPOSE_ASSET" --providers eodhd up -d
```

### 11) Production login

Access `https://$PRODUCTION_DOMAIN/` and authenticate with:
- Username: `produser`
- Password: the one you set with `htpasswd` in step 7
