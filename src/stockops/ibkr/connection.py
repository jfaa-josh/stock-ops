import asyncio
import logging
from collections.abc import AsyncGenerator, Awaitable
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Protocol

from ib_async import IB, BarDataList, Contract, TagValue

from stockops.config import ibkr_config

logger = logging.getLogger(__name__)

# IBKR "market data farm connection OK" style codes — informational, never a failure.
_INFO_ERROR_CODES = {2104, 2106, 2158}

_CONNECT_TIMEOUT_S = 10.0
_CONNECT_MAX_RETRIES = 3
_CONNECT_RETRY_BACKOFF_S = 2.0


class IBKRClient(Protocol):
    """The subset of ib_async.IB this module (and its callers) depend on, so nothing outside this
    file needs to import ib_async directly. Grows as later chunks need more (qualify, historical
    bars, subscribe_ticks) — not all defined up front."""

    def isConnected(self) -> bool: ...
    def disconnect(self) -> str | None: ...
    def reqMarketDataType(self, marketDataType: int) -> None: ...
    def reqCurrentTimeAsync(self) -> Awaitable[datetime]: ...
    async def qualifyContractsAsync(self, *contracts: Contract) -> list[Contract | list[Contract | None] | None]: ...
    async def reqHistoricalDataAsync(
        self,
        contract: Contract,
        endDateTime: datetime | date | str | None,
        durationStr: str,
        barSizeSetting: str,
        whatToShow: str,
        useRTH: bool,
        formatDate: int = 1,
        keepUpToDate: bool = False,
        chartOptions: list[TagValue] = [],  # noqa: B006 -- mirrors ib_async's real signature exactly (Protocol stub, never executed)
        timeout: float = 60,
    ) -> BarDataList: ...


def acquire_client_id(role: str) -> int:
    """clientId to use for this role. Static base for now — leasing (for concurrency > 1 per role)
    is deferred until something actually needs more than one connection per role at once."""
    bases = {
        "historical": ibkr_config.IBKR_CLIENT_ID_HISTORICAL,
        "streaming": ibkr_config.IBKR_CLIENT_ID_STREAMING,
        "account": ibkr_config.IBKR_CLIENT_ID_ACCOUNT,
    }
    if role not in bases:
        raise ValueError(f"Unknown IBKR client role: {role!r}; expected one of {sorted(bases)}")
    return bases[role]


def _on_error(reqId: int, errorCode: int, errorString: str, contract) -> None:
    if errorCode in _INFO_ERROR_CODES:
        logger.info("IBKR [%s]: %s", errorCode, errorString)
    else:
        logger.warning("IBKR error %s (reqId=%s, contract=%s): %s", errorCode, reqId, contract, errorString)


@asynccontextmanager
async def ibkr_session(role: str, *, readonly: bool = True) -> AsyncGenerator[IBKRClient]:
    """Connect to IB Gateway, yield a connected client, guarantee disconnect. readonly stays True
    for all market-data work in this phase — only Section 13 (account/order) work will ever pass
    False, and that's not built yet."""
    client_id = acquire_client_id(role)
    ib = IB()
    ib.errorEvent += _on_error

    last_exc: Exception | None = None
    for attempt in range(1, _CONNECT_MAX_RETRIES + 1):
        try:
            await ib.connectAsync(
                ibkr_config.IBKR_HOST,
                ibkr_config.IBKR_PORT,
                clientId=client_id,
                timeout=_CONNECT_TIMEOUT_S,
                readonly=readonly,
            )
            break
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "IBKR connect attempt %d/%d failed (role=%s, clientId=%s): %s",
                attempt,
                _CONNECT_MAX_RETRIES,
                role,
                client_id,
                exc,
            )
            if attempt < _CONNECT_MAX_RETRIES:
                await asyncio.sleep(_CONNECT_RETRY_BACKOFF_S * attempt)
    else:
        ib.errorEvent -= _on_error
        raise ConnectionError(
            f"Failed to connect to IB Gateway after {_CONNECT_MAX_RETRIES} attempts (role={role})"
        ) from last_exc

    try:
        ib.reqMarketDataType(ibkr_config.IBKR_MARKET_DATA_TYPE)
        yield ib
    finally:
        ib.errorEvent -= _on_error
        ib.disconnect()
