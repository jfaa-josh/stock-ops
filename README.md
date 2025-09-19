# StockOps Local with Docker Compose

This directory provides a **Docker Compose** setup for running **StockOps** locally.
It pulls pre-built, pinned images released to **GitHub Container Registry (GHCR)** for quick deployment or evaluation without cloning the repository.

---

## Overview

StockOps is a stock data pipeline orchestrator. StockOps facilitates parallel concurrent realtime and historical data provider API call deployments.  Deployments are specified using a Streamlit user interface, and can be triggered immediately or scheduled.  Data extracted by concurrent streams is buffered, and then extracted by a single **SQLite** writer which transforms and stores the data in a docker volume database within `.db` files.

- **Core Data Pipeline (Docker profile: "datapipe-core"):**
  - **Streamlit** UI — exposed on your host (default `http://localhost:8501`).
  - **Prefect**-based orchestration — exposed on your host (default `http://localhost:4200`)
  - **PostgreSQL** metadata DB
  - **Redis** cache/queue and **SQLite** writer service

- **Optional Visualization (Docker profile: "datapipe-visualize-data"):**
  - **SQLite Browser** service for viewing `.db` files. — exposed on your host (default `http://localhost:8081`)

---

## Prerequisites

- **Docker Engine** and **Docker Compose v2**
  - Windows/macOS: [Docker Desktop](https://www.docker.com/products/docker-desktop/)
  - Linux: Docker Engine + the `docker compose` plugin

  Verify installation:
  ```bash
  docker --version
  docker compose version
  ```

---

## One-Minute Quickstart
1) Create an empty folder for deployment
2) Download `docker-compose.vx.y.z.yml` from GitHub Releases
3) Create .env and set API token ([see instructions](#2-configure-environment))
4) Launch core stack:
   ```bash
   docker compose -f docker-compose.vx.y.z.yml --profile datapipe-core up -d
   ```
5) Open streamlit UI via web-browser at `http://localhost:8501`

---

## 1. Download a Pinned Compose File
From the GitHub Releases page, 🚨ghcr.io, grab the compose file for the desired version.

Example: `docker-compose.v1.2.3.yml`

These files reference immutable image tags—no local build required.

## 2. Configure Environment
Create an .env in the same directory as the compose file.  File should contain exactly as shown below, except fill in required info where indicated:
```bash
cat > .env << 'EOF'
# Set provider tokens here:
EODHD_API_TOKEN=__PUT_YOUR_TOKEN_HERE__
EOF
```

Note: Your .env stays local and is never pushed to GitHub.

### Providers:
StockOps is currently capable of interacting with the following providers:
1) EODHD

## 3. Basic Launch

### Core stack only:
Start the main data-pipeline profile:

```bash
docker compose -f docker-compose.vx.y.z.yml --profile datapipe-core up -d
```

Primary launches:

- streamlit – web UI
- prefect-server – Prefect API/UI backend
- prefect-serve – Prefect worker
- postgres – metadata DB for Prefect
- redis – messaging and caching (DB0: prefect/DB1: writer-service buffer)
- writer-service – buffering & SQLite writer

### Core stack with database visualization:
Start the main + visualization data-pipeline profile:

```bash
docker compose -f docker-compose.vx.y.z.yml --profile datapipe-core --profile datapipe-visualize-data up -d
```

Adds:

-  sqlitebrowser – for viewing database files located at `Computer/data/`.

## 4. Additional Launch, Verify, and Access
### Check status:
```bash
docker compose -f docker-compose.vx.y.z.yml ps
```

### View logs (example: writer service):
```bash
docker compose -f docker-compose.vx.y.z.yml logs -f writer-service
```

Note: basic launch ([above](#3-basic-launch)) postpend `-d` launches services in detached mode (background). Detached mode is ideal for long running service. Run without detached mode for short term debugging or for immediate access to streaming front-end container logs.

### Stop / Restart / Remove:

*Stop (keep containers/data):*
```bash
docker compose -f docker-compose.vx.y.z.yml stop
```

*Restart:*
```bash
docker compose -f docker-compose.vx.y.z.yml start
```

*Remove containers (preserve data):*
```bash
docker compose -f docker-compose.vx.y.z.yml down
```

Note: On all `docker compose down` calls, do not postpend -v unless you intend to delete all storage volumes.

### Data Persistence

Named volumes survive docker compose down:

- `db_data` – SQLite databases
- `postgres_data` – PostgreSQL metadata
- `redis_data` – Redis state & buffer

Note: Check actual volume names with `docker volume ls`.

### Accessing Data Outside of Docker

A reader class exists, but automated access points are future work.

To copy database files to a local directory:
1) cd to desired local directory (example: localdir)
2) Copy files to localdir/data:
  ```bash
  docker cp stockops-writer-service-1:/app/data/raw ./data
  ```

## Upgrading to a New Version

1. Stop current stack (optional but clean):
   ```bash
   docker compose -f docker-compose.vx.y.z.yml down
   ```
2. Download the new pinned compose (e.g. docker-compose.v1.3.0.yml).
   - Reuse your existing .env (only update if release notes add variables).
3. Start:
   ```bash
   docker compose -f docker-compose.v1.3.0.yml --profile datapipe-core up -d
   ```
   - All named volumes—and therefore your data—are preserved.

## Security Notes

*API tokens:*
- Remain in your local .env.
- Never commit .env to version control.

Protect exposed ports on shared hosts or proxy them behind authentication.
