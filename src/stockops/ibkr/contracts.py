"""Contract resolution and qualification (strategy doc Section 4). IBKR requires a fully
qualified Contract before requesting historical or streaming data -- a bare (symbol, exchange)
is ambiguous for anything listed on more than one venue (the classic MSFT/CSCO case), so every
contract is routed via SMART with an explicit `primaryExchange` and qualified through IBKR's
API before use.

Qualified contracts are cached in-process, keyed by (symbol, sec_type, exchange, currency):
qualification is itself a request, and repeating it for every window of a multi-window
historical pull would waste pacing budget for no reason. The cache is intentionally not shared
across processes -- conIds can change on corporate actions, and a stale one fails obscurely.
"""

from ib_async import Contract, Stock

from stockops.config import ibkr_config
from stockops.ibkr.connection import IBKRClient


class ContractResolutionError(Exception):
    """Raised when IBKR can't resolve a (symbol, exchange) pair to exactly one contract."""


_qualified_cache: dict[tuple[str, str, str, str], Contract] = {}


async def qualify_stock(client: IBKRClient, ticker: str, exchange: str, *, sec_type: str = "STK") -> Contract:
    """Build and qualify a Stock contract for `ticker` on `exchange`."""
    metadata = ibkr_config.EXCHANGE_METADATA[exchange]
    currency = str(metadata["Currency"])
    key = (ticker, sec_type, exchange, currency)

    cached = _qualified_cache.get(key)
    if cached is not None:
        return cached

    contract = Stock(ticker, str(metadata["exchange"]), currency, primaryExchange=str(metadata["primary_exchange"]))

    qualified = await client.qualifyContractsAsync(contract)
    result = qualified[0] if len(qualified) == 1 else None
    if not isinstance(result, Contract):
        raise ContractResolutionError(
            f"Could not uniquely qualify IBKR contract for ticker={ticker!r} exchange={exchange!r}: {result!r}"
        )

    _qualified_cache[key] = result
    return result
