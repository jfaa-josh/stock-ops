# StockOps — Account / Trade Execution

Not yet implemented.

Intended scope: account queries (balances, positions, P&L), and order placement against connected trading providers. This is expected to build on the connection/pacing/contract-qualification layers already established for market-data access in the [Data Pipeline](README-DATAPIPELINE.md), rather than starting from scratch.

Order placement carries real safety requirements not yet built — a hard trading-enabled kill switch, dry-run/what-if support, and idempotent order references — since this touches real money, not just data collection.

See [README-QUICKSTART.md](README-QUICKSTART.md) for general installation and deployment, shared across all feature areas.
