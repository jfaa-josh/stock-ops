# StockOps Local with Docker Compose

This directory provides a **Docker Compose** setup for running **StockOps** locally and in production.
It pulls pre-built, pinned images released to **GitHub Container Registry (GHCR)** for quick deployment or evaluation without cloning the repository.

---

## Overview

StockOps is a stock data pipeline orchestrator. StockOps facilitates parallel concurrent realtime and historical data provider API call deployments.  Deployments are specified using a Streamlit user interface, and can be triggered immediately or scheduled.  Data extracted by concurrent streams is buffered, and then extracted by a single SQLite writer which transforms and stores the data in a docker volume database within `.db` files.

### Execution Summary
- User creates `.env` from `.env.example` and fills in provider keys
- User sets `PRODUCTION_DOMAIN` and `LETSENCRYPT_EMAIL` for automated TLS
- Available providers set to Streamlit UI
- Streamlit UI sets historical or streaming data paradigms called "deployments"
- Prefect flow runs (executions of deployments) are either triggered (live) or scheduled by user via Streamlit UI
- Prefect orchestrates concurrent flow run executions
- Concurrent flow runs send API calls to provider, then transform returned data and set to Redis buffer DB1.  Multiple flow runs can write concurrently to the single buffer.
- Writer-service monitors and drains Redis buffer, sorting data into batches of common database file and table (ticker), and sends batches to be written to .db files contained within the `db_data` Docker storage volume.

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

---

## Quickstart
Use the wrapper script to start the stack. It sets the correct nginx mode and Prefect UI URL based on `local` or `prod`, and passes through any extra compose flags.
1) Create an empty folder for deployment
2) Download `docker-compose.vx.y.z.yml` from GitHub repository [releases](https://github.com/jfaa-josh/stock-ops/releases) page
3) Copy `.env.example` to `.env` and update placeholders ([see instructions](#2-configure-environment))
4) If using `prod`, set `PRODUCTION_DOMAIN` and `LETSENCRYPT_EMAIL` ([see instructions](#tls-certificates))
5) Launch full datapipeline stack in detached mode:
   ```bash
   ./run/stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml --profile datapipe-core --profile datapipe-visualize-data up -d
   ```
(Use `local` instead of `prod` to start the local nginx mode and self-signed `stockops.local` certs.)
6) Access the UI:
   - Local: `https://stockops.local/`
   - Production: `https://your.production.domain/`

---

## Execution Details

### 1. Download a Pinned Compose File
From the GitHub repository [releases](https://github.com/jfaa-josh/stock-ops/releases) page, grab the compose file for the desired version.

Example: `docker-compose.v1.2.3.yml`

These files reference immutable image tags—no local build required.

### 2. Configure Environment
Copy `.env.example` to `.env` in the same directory as the compose file, then update the placeholder values:
```bash
cp .env.example .env
```

`PRODUCTION_DOMAIN` is used by the `nginx-prod` mode to generate the `server_name` and certificate paths inside `nginx/conf.d/stockops-prod.conf.template`, and certbot uses it to request and renew certificates under `./certs/live/stockops.prod/live/$PRODUCTION_DOMAIN/`.
`LETSENCRYPT_EMAIL` is required for certbot to issue the initial production certificate. Local mode can keep placeholder values.
The wrapper sets `PREFECT_UI_SERVE_BASE`, `PREFECT_UI_API_URL`, and `PYTHONPATH` automatically.

Note: Your .env stays local and is never pushed to GitHub.

#### Providers:
StockOps is currently capable of interacting with the following providers:
1) EODHD

### 3. Launch Docker Compose Profiles

#### Core stack only (Docker profile: "datapipe-core"):
To start the core data-pipeline profile only in detached mode:

```bash
./run/stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml --profile datapipe-core up -d
```

Important container launches (nginx now reverse-proxies the UI services on ports 80/443):

- Streamlit UI — reachable through nginx:
  - Local: `https://stockops.local/`
  - Production: `https://your.production.domain/`
- Prefect-server orchestrator — reachable through nginx:
  - Local: `https://stockops.local/prefect/`
  - Production: `https://your.production.domain/prefect/`
- Prefect-serve – Prefect worker pool
- PostgreSQL – metadata DB for Prefect
- Redis – cache/queue for prefect (DB0) and memory buffer for SQLite writer-service (DB1)
- writer-service – Redis buffer monitoring, transform, batching, and SQLite writer

#### Core stack with optional data browser (Docker profile: "datapipe-visualize-data"):
Start the main + visualization data-pipeline profile in detached mode:

```bash
./run/stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml --profile datapipe-core --profile datapipe-visualize-data up -d
```

Additional container launches:

- SQLite Browser service for viewing `.db` files located at `Computer/data/` — reachable through nginx when the optional `datapipe-visualize-data` profile is active:
  - Local: `https://stockops.local/sqlite/`
  - Production: `https://your.production.domain/sqlite/`

### 4. Additional Launch, Verify, and Access Details

#### Detatched mode (-d):
Running the wrapper with `-d` launches services in detached mode (background). Detached mode is ideal for long running service, where terminal closure will not interrupt service. Run without detached mode for short term debugging or for immediate access to streaming front-end container logs.

Note: regardless of detached mode, logs and other information is available for each container or volume in Docker Desktop if installed.

#### Project name (-p):
Running with project name `-p datapipe` is optional. The benefit, shown here, is that containers set by profiles in the compose up command are set to the project name, which makes it simpler to perform other tasks and easier to avoid accidentally omitting containers grouped within a profile.

#### Check status:
```bash
./run/stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml ps
```

#### View logs (example: writer-service container):
```bash
./run/stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml logs -f writer-service
```

#### Stop / Restart / Remove:

*Stop (keep containers/data):*
```bash
./run/stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml stop
```

*Restart:*
```bash
./run/stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml restart
```

*Remove containers (preserve data):*
```bash
./run/stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml down
```

Note: On all `down` calls, do not postpend -v unless you intend to delete all storage volumes.

#### Data Persistence

Named volumes survive a `down` call:

- `db_data` – SQLite databases
- `postgres_data` – PostgreSQL metadata
- `redis_data` – Redis state & buffer

Note: Check actual volume names with `docker volume ls`.

#### Accessing Data Outside of Docker

A reader class exists, but automated access points are future work.

To copy database files to a local directory:
1) cd to desired local directory (example: localdir)
2) Copy files to localdir/data:
  ```bash
  docker cp "$(./run/stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml ps -q writer-service)":/app/data/raw ./data
  ```

Note: Container must be running.

### Routing UI traffic through nginx

Responsibility for TLS is handled by the wrapper mode: `prod` renders `nginx/conf.d/stockops-prod.conf.template` using `PRODUCTION_DOMAIN`, while `local` loads `nginx/conf.d/stockops-local.conf` (which already targets `stockops.local` and the local self-signed certs under `./certs/live/stockops.local`). Only the nginx container for the selected mode starts, so there is no port conflict.

The active nginx service listens only on host ports 80/443 and proxies:

A dedicated listener on port 80 immediately issues `301 https://$host$request_uri`, so all external traffic is funneled through the TLS listener on port 443 before reaching the proxy locations.

- `/` → Streamlit UI
- `/prefect/` → Prefect server API/UI
- `/sqlite/` → SQLite Browser (only available when `datapipe-visualize-data` is active)

The nginx mode is the only way to reach services from outside the Docker network; services remain internal by default. The nginx container mounts a local auth file for `nginx-local` and a separate production auth file for `nginx-prod`.

#### Basic authorization

We commit `./nginx/htpasswd-local` prepopulated with the `localadmin` user for the `nginx-local` mode. Change the password with `htpasswd -B nginx/htpasswd-local localadmin` or create additional users with `htpasswd -B nginx/htpasswd-local <new user>`.

For production, `nginx-prod` uses `./nginx/prod/htpasswd` (gitignored). The first time you start `nginx-prod`, an init container runs `scripts/init-htpasswd-prod.sh`, prompts you for a password, and creates the file for the `produser` account. Run the initial startup without `-d` so the prompt can appear. To rotate credentials later, delete `./nginx/prod/htpasswd` and restart `nginx-prod` without `-d` to re-prompt.

#### TLS certificates

TLS files are stored under `./certs/live/stockops.prod/live/$PRODUCTION_DOMAIN` for production and are mounted straight into nginx. The `nginx-prod` mode renders `nginx/conf.d/stockops-prod.conf.template` using `PRODUCTION_DOMAIN`.

##### Local
The `nginx-local` mode loads `nginx/conf.d/stockops-local.conf`, which already targets `stockops.local` and the self-signed files under `./certs/live/stockops.local`. That local pair is committed so the repo contains a 100-year placeholder cert you can use without re-generating. If desired, no modifications are required for local deployment.

The provided local pair has no CA; in order to avoid browser rejecting the certificate pair as untrustworthy, import into whatever trust store your OS/browser relies on. Example for WSL/Linux:
```bash
sudo mkdir -p /usr/local/share/ca-certificates
sudo cp ./certs/live/stockops.local/fullchain.pem /usr/local/share/ca-certificates/stockops.local.crt
cd ~ && sudo update-ca-certificates
```

After adding the certificates to your trust store, you must then resolve stockops.local to 127.0.0.1.

If you ever need to rebuild the local certificate (for example to change the subject), rerun the long-lived self-signed command:

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
  ./run/stockops.sh prod -p datapipe -f docker-compose.v1.3.0.yml --profile datapipe-core --profile datapipe-visualize-data up -d
   ```
   - Docker compares the images deployed in the current run to those in the new compose version, updates as needed, and restarts any updated containers.  All named volumes—and therefore your data—are preserved.
