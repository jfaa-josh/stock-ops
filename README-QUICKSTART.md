# StockOps Local with Docker Compose

This directory provides a **Docker Compose** setup for running **StockOps** locally.
It pulls pre-built, pinned images released to **GitHub Container Registry (GHCR)** for quick deployment or evaluation without cloning the repository.

---

## Overview

StockOps is a stock data pipeline orchestrator. StockOps facilitates parallel concurrent realtime and historical data provider API call deployments.  Deployments are specified using a Streamlit user interface, and can be triggered immediately or scheduled.  Data extracted by concurrent streams is buffered, and then extracted by a single SQLite writer which transforms and stores the data in a docker volume database within `.db` files.

### Execution Summary
- User created .env file sets API provider keys
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
1) Create an empty folder for deployment
2) Download `docker-compose.vx.y.z.yml` from GitHub repository [releases](https://github.com/jfaa-josh/stock-ops/releases) page
3) Create .env and set API token ([see instructions](#2-configure-environment))
4) Launch full datapipeline stack in detached mode:
   ```bash
   docker compose -p datapipe -f docker-compose.vx.y.z.yml --profile datapipe-core --profile datapipe-visualize-data up -d
   ```
5) Open streamlit UI via web-browser at `http://localhost:8501` to set and execute flow runs
6) Monitor data storage using SQLite Browser via web-browser at `http://localhost:8081`

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
EOF
```

Note: Your .env stays local and is never pushed to GitHub.

#### Providers:
StockOps is currently capable of interacting with the following providers:
1) EODHD

### 3. Launch Docker Compose Profiles

#### Core stack only (Docker profile: "datapipe-core"):
To start the core data-pipeline profile only in detatched mode:

```bash
docker compose -p datapipe -f docker-compose.vx.y.z.yml --profile datapipe-core up -d
```

Important container launches:

- Streamlit UI — exposed on your host (default `http://localhost:8501`)
- Prefect-server orchestrator — exposed on your host (default `http://localhost:4200`)
- Prefect-serve – Prefect worker pool
- PostgreSQL – metadata DB for Prefect
- Redis – cache/queue for prefect (DB0) and memory buffer for SQLite writer-service (DB1)
- writer-service – Redis buffer monitoring, transform, batching, and SQLite writer

#### Core stack with optional data browser (Docker profile: "datapipe-visualize-data"):
Start the main + visualization data-pipeline profile in detatched mode:

```bash
docker compose -p datapipe -f docker-compose.vx.y.z.yml --profile datapipe-core --profile datapipe-visualize-data up -d
```

Additional container launches:

- SQLite Browser service for viewing `.db` files located at `Computer/data/` — exposed on your host (default `http://localhost:8081`)

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

### Upgrading to a New Version

1. Download the new pinned compose (e.g. docker-compose.v1.3.0.yml).
   - Reuse your existing .env (only update if release notes add variables).
2. For minimal interruption of service that is currently running:
   ```bash
   docker compose -p datapipe -f docker-compose.v1.3.0.yml --profile datapipe-core --profile datapipe-visualize-data up -d
   ```
   - Docker compares the images deployed in the current run to those in the new compose version, updates as needed, and restarts any updated containers.  All named volumes—and therefore your data—are preserved.
