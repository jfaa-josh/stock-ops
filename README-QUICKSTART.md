# StockOps Local with Docker Compose

This directory provides a **Docker Compose** setup for running **StockOps** locally and in production.
It pulls pre-built, pinned images released to **GitHub Container Registry (GHCR)** for quick deployment or evaluation without cloning the repository.

---

## Overview

StockOps is a stock data pipeline orchestrator. StockOps facilitates parallel concurrent realtime and historical data provider API call deployments.  Deployments are specified using a Streamlit user interface, and can be triggered immediately or scheduled.  Data extracted by concurrent streams is buffered, and then extracted by a single SQLite writer which transforms and stores the data in a docker volume database within `.db` files.

### Execution Summary
- User created .env file sets API provider keys
- User sets TLS certificates for production as needed
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

---

## Quickstart
Pass the desired nginx profile flag every time you run `docker compose` so nginx loads the correct certificate bundle: add `--profile nginx-prod` for the production config or `--profile nginx-local` for the self-signed `stockops.local` setup, in addition to the `datapipe-*` profiles. You must choose exactly one of the two nginx profiles so only the matching nginx container starts.
1) Create an empty folder for deployment
2) Download `docker-compose.vx.y.z.yml` from GitHub repository [releases](https://github.com/jfaa-josh/stock-ops/releases) page
3) Create .env and set API token ([see instructions](#2-configure-environment))
4) Set TLS certificates ([see instructions](#tls-certificates))
5) Launch full datapipeline stack in detached mode:
   ```bash
   docker compose -p datapipe -f docker-compose.vx.y.z.yml --profile datapipe-core --profile datapipe-visualize-data --profile nginx-prod up -d
   ```
  (Production launches assume `--profile nginx-prod` is included so that `nginx/conf.d/stockops-prod.conf` loads. Replace that flag with `--profile nginx-local` if you want the `nginx-local` service and self-signed `stockops.local` certs instead.)
6) Access the Streamlit UI via `http://localhost/` (or `https://localhost/` once TLS assets are placed in `./certs`; see the TLS certificates section below for how to generate them)
7) Access the Prefect UI at `http://localhost/prefect/` and, when the visualization profile is active, the SQLite Browser at `http://localhost/sqlite/`

---

## Execution Details

### 1. Download a Pinned Compose File
From the GitHub repository [releases](https://github.com/jfaa-josh/stock-ops/releases) page, grab the compose file for the desired version.

Example: `docker-compose.v1.2.3.yml`

These files reference immutable image tags—no local build required.

### 2. Configure Environment
Create an .env in the same directory as the compose file.  File should contain exactly as shown below, except fill in required info where indicated:
```bash
cat > .env << 'EOF'
# Set provider tokens here:
EODHD_API_TOKEN=__PUT_YOUR_TOKEN_HERE__
PRODUCTION_DOMAIN=your.production.domain
EOF
```

`PRODUCTION_DOMAIN` is used by the `nginx-prod` profile to generate the `server_name` and certificate paths inside `nginx/conf.d/stockops-prod.conf`, so keep it in sync with whatever certificate bundle you drop under `./certs/live/<your-domain>/`.

Note: Your .env stays local and is never pushed to GitHub.

#### Providers:
StockOps is currently capable of interacting with the following providers:
1) EODHD

### 3. Launch Docker Compose Profiles

#### Core stack only (Docker profile: "datapipe-core"):
To start the core data-pipeline profile only in detatched mode:

```bash
docker compose -p datapipe -f docker-compose.vx.y.z.yml --profile datapipe-core --profile nginx-prod up -d
```

Important container launches (nginx now reverse-proxies the UI services on ports 80/443):

- Streamlit UI — reachable through nginx at `http://localhost/` (or `https://localhost/` once TLS assets are configured)
- Prefect-server orchestrator — reachable through nginx at `http://localhost/prefect/`
- Prefect-serve – Prefect worker pool
- PostgreSQL – metadata DB for Prefect
- Redis – cache/queue for prefect (DB0) and memory buffer for SQLite writer-service (DB1)
- writer-service – Redis buffer monitoring, transform, batching, and SQLite writer

#### Core stack with optional data browser (Docker profile: "datapipe-visualize-data"):
Start the main + visualization data-pipeline profile in detatched mode:

```bash
docker compose -p datapipe -f docker-compose.vx.y.z.yml --profile datapipe-core --profile datapipe-visualize-data --profile nginx-prod up -d
```

Additional container launches:

- SQLite Browser service for viewing `.db` files located at `Computer/data/` — reachable through nginx at `http://localhost/sqlite/` when the optional `datapipe-visualize-data` profile is active

### 4. Additional Launch, Verify, and Access Details

#### Detatched mode (-d):
Running docker compose as described in basic launch ([above](#3-launch-docker-compose-profiles)) with postpend `-d` launches services in detached mode (background). Detached mode is ideal for long running service, where terminal closure will not interrupt service. Run without detached mode for short term debugging or for immediate access to streaming front-end container logs.

Note: regardless of detatched mode, logs and other information is available for each container or volume in Docker Desktop if installed.

#### Project name (-p):
Running docker compose with project name `-p datapipe` is optional.  The benefit, shown here, is that containers set by profiles in the docker compose up command are set to the project name, which makes it simpler to perform other tasks and easier to avoid accidentally omitting containers grouped within a profile.

#### Check status:
```bash
docker compose -p datapipe -f docker-compose.vx.y.z.yml ps
```

#### View logs (example: writer-service container):
```bash
docker compose -p datapipe -f docker-compose.vx.y.z.yml logs -f writer-service
```

#### Stop / Restart / Remove:

*Stop (keep containers/data):*
```bash
docker compose -p datapipe -f docker-compose.vx.y.z.yml stop
```

*Restart:*
```bash
docker compose -p datapipe -f docker-compose.vx.y.z.yml restart
```

*Remove containers (preserve data):*
```bash
docker compose -p datapipe -f docker-compose.vx.y.z.yml down
```

Note: On all `docker compose down` calls, do not postpend -v unless you intend to delete all storage volumes.

#### Data Persistence

Named volumes survive docker compose down:

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
  docker cp "$(docker compose -p datapipe -f docker-compose.vx.y.z.yml ps -q writer-service)":/app/data/raw ./data
  ```

Note: Container must be running.

### Routing UI traffic through nginx

Responsibility for TLS is handled by whichever nginx profile you include via `--profile`: `--profile nginx-prod` loads `nginx/conf.d/stockops-prod.conf` (update `server_name` and the `/etc/letsencrypt/live/<your-domain>` paths inside that file to match your production domain/Certbot bundle), while `--profile nginx-local` loads `nginx/conf.d/stockops-local.conf` (which already targets `stockops.local` and the local self-signed certs under `./certs/live/stockops.local`). Only the nginx container for the supplied profile starts, so there is no port conflict.

The active nginx service listens only on host ports 80/443 and proxies:

A dedicated listener on port 80 immediately issues `301 https://$host$request_uri`, so all external traffic is funneled through the TLS listener on port 443 before reaching the proxy locations.

- `/` → Streamlit UI
- `/prefect/` → Prefect server API/UI
- `/sqlite/` → SQLite Browser (only available when `datapipe-visualize-data` is active)

The nginx profile is the only way to reach services from outside the Docker network; services remain internal by default. The nginx container also mounts `./nginx/htpasswd` so you can add additional auth directives if needed.

#### Optional basic authorization

We commit a sample `./nginx/htpasswd` file prepopulated with the `localadmin` user so you can enable basic auth in either nginx profile without generating a password file first. The nginx service still mounts the same file regardless of the nginx local or production service run via docker profile. Change the password with `htpasswd -Bnginx/htpasswd localadmin` or create additional users with `htpasswd -b nginx/htpasswd <user> <password>` if you prefer.

#### TLS certificates

TLS files are stored under `./certs/live/<your-domain>` and are mounted straight into nginx. The `nginx-prod` profile loads `nginx/conf.d/stockops-prod.conf`, so update its `server_name` and `ssl_certificate`/`ssl_certificate_key` entries to reflect your public domain and certificate path.

##### Local
The `nginx-local` profile loads `nginx/conf.d/stockops-local.conf`, which already targets `stockops.local` and the self-signed files under `./certs/live/stockops.local`. That local pair is committed so the repo contains a 100-year placeholder cert you can use without re-generating. If desired, no modifications are required for local deployment.

If you ever need to rebuild the local certificate (for example to change the subject), rerun the long-lived self-signed command:

```bash
openssl req -x509 -nodes -newkey rsa:4096 \
  -keyout certs/live/stockops.local/privkey.pem \
  -out certs/live/stockops.local/fullchain.pem \
  -days 36500 -subj "/CN=stockops.local"
```

Then make `stockops.local` resolve to your machine:

```bash
echo "127.0.0.1 stockops.local" | sudo tee -a /etc/hosts
```

##### Production
For the `nginx-prod` profile, you need to generate and maintain TLS certificates. Since `.gitignore` still blocks all other paths under `certs/live/`, you can place production certs in `./certs/live/stockops.prod/<your.production.domain>` without them being picked up or committed.

Create `./certs/live/stockops.prod/<your.production.domain>`, then copy in the certs issued for your real domain (e.g., via Certbot). Set `PRODUCTION_DOMAIN` in `.env` to exactly match that subfolder name. Because the certs directory is ignored except for the tracked `stockops.local` pair, regenerate or re-sync the production certs on each clone and whenever the certs rotate.

### Upgrading to a New Version

1. Download the new pinned compose (e.g. docker-compose.v1.3.0.yml).
   - Reuse your existing .env (only update if release notes add variables).
2. For minimal interruption of service that is currently running:
  ```bash
  docker compose -p datapipe -f docker-compose.v1.3.0.yml --profile datapipe-core --profile datapipe-visualize-data --profile nginx-prod up -d
   ```
   - Docker compares the images deployed in the current run to those in the new compose version, updates as needed, and restarts any updated containers.  All named volumes—and therefore your data—are preserved.
