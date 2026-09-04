# StockOps — Data Pipeline

This guide covers the **Data Pipeline** feature area of StockOps: concurrent real-time and historical market-data ingestion from multiple providers. See [README-QUICKSTART.md](README-QUICKSTART.md) for installation, general `stockops.sh` usage, and deployment (Docker/nginx/TLS) setup shared across all feature areas.

---

## How It Works

- Streamlit UI sets historical or streaming data paradigms called "deployments"
- Prefect flow runs (executions of deployments) are either triggered (live) or scheduled by the user via the Streamlit UI
- Concurrent flow runs send API calls to the selected provider, then transform the returned data and post it to a Redis buffer (DB1). Multiple flow runs can write concurrently to the single buffer.
- `writer-service` monitors and drains the Redis buffer, sorting data into batches by database file and table (ticker), and writes batches to `.db` files in the `db_data` Docker volume.

---

## Running a Provider (`--providers`)

A provider is a commercial service StockOps connects to for credentialed access to market datastreams. The data pipeline only starts when you tell it which provider(s) to use, via `--providers` — a comma-separated list (no spaces after commas).

| Flag | Provider | Notes |
|---|---|---|
| `eodhd` | EODHD | Historical and streaming market data. |
| `ibkr_paper` | Interactive Brokers — paper account | Historical and streaming market data. Sandboxed/fake-money trading mode. |
| `ibkr_live` | Interactive Brokers — live account | Historical and streaming market data. Real trading account. |

Example selecting more than one at once: `--providers eodhd,ibkr_live`. Add `--profile datapipe-visualize-data` if you also want the DB browser. You never pass a raw `--profile datapipe-core` yourself; `--providers` turns it on automatically.

The Streamlit UI only shows a provider's section if that provider was included in `--providers` when the stack was started — e.g. start with `--providers eodhd` and only the EODHD section appears; add `ibkr_paper`/`ibkr_live` to also see IBKR's. When IBKR is active, the UI also shows a read-only label indicating whether it's running in paper or live mode.

```bash
./stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml --providers eodhd up -d
```

Important container launches (nginx reverse-proxies the UI services on ports 80/443):

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

---

## Provider Stack With the Data Browser (`--profile datapipe-visualize-data`)

Add the optional DB browser alongside a provider in detached mode:

```bash
./stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml --providers eodhd --profile datapipe-visualize-data up -d
```

Additional container launches:

- SQLite Browser service for viewing `.db` files — mounted at `/data` inside the container, backed by the same `db_data` named volume the writer-service writes to — reachable through nginx when the optional `datapipe-visualize-data` profile is active:
  - Local: `https://stockops.local/sqlite/`
  - Production: `https://your.production.domain/sqlite/`

---

## Reading Existing Data Without a Provider (`--profile reader`)

If you just want to read data already sitting in the `db_data` volume — no provider, no credentials, no core pipeline — use `--profile reader` instead. It starts a single idle container with read-only access to the stored `.db` files; combine with `--profile datapipe-visualize-data` if you also want the DB browser.

```bash
./stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml --profile reader up -d
./stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml exec reader-service python
```

---

## Accessing Data Outside of Docker

Use the `reader` profile (above) to query the `.db` files in place via Python without copying anything out, e.g.:
```bash
./stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml exec reader-service python
```
```python
from stockops.data.database.reader import ReadProcess
```

To copy database files to a local directory instead:
1) cd to desired local directory (example: localdir)
2) Copy files to localdir/data:
  ```bash
  docker cp "$(./stockops.sh prod -p datapipe -f docker-compose.vx.y.z.yml ps -q writer-service)":/app/data/raw ./data
  ```

Note: Container must be running.
