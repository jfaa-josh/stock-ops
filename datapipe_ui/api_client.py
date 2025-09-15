from __future__ import annotations

import json
import logging
from typing import Any, Dict, Iterable, Literal, Mapping, Optional, Tuple, Sequence, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

HttpMethod = Literal["GET", "POST", "DELETE", "PATCH", "PUT"]

JsonMapping = Mapping[str, Any]
JsonList    = Sequence[JsonMapping]
JsonPayload = Union[JsonMapping, JsonList]  # allow dict OR list[dict]

class ApiClient:
    def __init__(
        self,
        base_url: str,
        default_headers: Optional[Mapping[str, str]] = None,
        timeout: Tuple[float, float] = (5.0, 30.0),  # (connect, read)
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        retry_statuses: Iterable[int] = (429, 502, 503, 504),
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.default_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **(default_headers or {}),
        }
        self.timeout = timeout

        # Single session for connection pooling + retries (idempotent methods)
        self.session = requests.Session()
        retry = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=tuple(retry_statuses),
            allowed_methods=frozenset({"GET", "DELETE", "HEAD", "OPTIONS"}),  # avoid retrying non-idempotent writes
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=50)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _redact(self, obj: Any) -> Any:
        """Redact sensitive fields in logs; shallow by design."""
        if not isinstance(obj, dict):
            return obj
        SENSITIVE = {"authorization", "auth", "token", "password", "api_key", "x-api-key"}
        return {k: ("<redacted>" if k.lower() in SENSITIVE else v) for k, v in obj.items()}

    def send(
        self,
        path: str,
        payload: Optional[JsonPayload] = None,
        headers: Optional[Mapping[str, str]] = None,
        method: HttpMethod = "POST",
        timeout: Optional[Tuple[float, float]] = None,
        params: Optional[Mapping[str, Any]] = None,
        allow_redirects: bool = True,
        verbose_logging: bool = True,
    ) -> Dict[str, Any]:
        """
        Wraps Prefect 3 Server API calls with sane defaults.

        - GET uses `params` (not JSON body).
        - 204 returns a uniform dict.
        - Robust JSON parsing fallback.
        - Retries on 429/5xx for idempotent verbs only.
        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        hdrs = {**self.default_headers, **(headers or {})}
        to = timeout or self.timeout

        try:
            if method == "GET":
                resp = self.session.get(url, headers=hdrs, params=params, timeout=to, allow_redirects=allow_redirects)
            elif method == "DELETE":
                # Some endpoints accept a JSON body for DELETE; pass if provided
                resp = self.session.delete(url, headers=hdrs, json=payload, params=params, timeout=to, allow_redirects=allow_redirects)
            elif method in {"POST", "PATCH", "PUT"}:
                resp = self.session.request(method, url, headers=hdrs, json=payload, params=params, timeout=to, allow_redirects=allow_redirects)
            else:
                raise ValueError(f"Unsupported method: {method}")

            # Raise for 4xx/5xx *after* we have resp for logging
            try:
                resp.raise_for_status()
            except requests.exceptions.HTTPError as http_err:
                # Try to extract Prefect-style {"detail": "..."} for clarity
                detail = None
                body: Any
                try:
                    body = resp.json()
                    if isinstance(body, dict):
                        detail = body.get("detail")
                except Exception:
                    body = (resp.text or "")[:2000]

                logger.error(
                    "HTTP %s on %s %s\nparams=%r\npayload=%r\nresp=%s",
                    resp.status_code, method, url,
                    self._redact(params or {}),
                    self._redact(dict(payload) if isinstance(payload, dict) else {"_non_dict_payload": str(payload)[:500]} if payload is not None else {}),
                    body if isinstance(body, str) else json.dumps(body, indent=2)[:2000],
                )
                msg = f"HTTP {resp.status_code} {url}"
                if detail:
                    msg += f" — {detail}"
                # Re-raise with compact message (nice for Streamlit/UI surfaces)
                raise requests.exceptions.HTTPError(msg, response=resp) from None

            # Success path
            if resp.status_code == 204:
                logger.info("API %s %s -> 204 No Content", method, url)
                return {"status": 204, "ok": True, "no_content": True}

            if verbose_logging: logger.info("API %s %s -> %s", method, url, resp.status_code)

            # Prefer JSON if declared; fallback to text
            ctype = resp.headers.get("Content-Type", "")
            if "application/json" in ctype.lower():
                try:
                    data = resp.json()
                except ValueError:
                    raise

            return data

        except requests.exceptions.RequestException as e:
            logger.error(
                "Request error on %s %s: %s\nparams=%r\npayload=%r",
                method, url, e,
                self._redact(params or {}),
                self._redact(dict(payload) if isinstance(payload, dict) else {"_non_dict_payload": str(payload)[:500]} if payload is not None else {}),
            )
            raise
