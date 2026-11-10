"""A small, polite HTTP client: one User-Agent, a minimum interval between requests,
and bounded retries with exponential backoff for transient failures."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from typing import Any, Self

import httpx

log = logging.getLogger(__name__)

RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


class RetryExhausted(RuntimeError):
    """Raised when a request keeps failing after all retries."""


class PoliteClient:
    def __init__(
        self,
        user_agent: str,
        min_interval: float = 1.0,
        max_retries: int = 4,
        backoff_base: float = 2.0,
        backoff_cap: float = 60.0,
        timeout: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._client = httpx.Client(
            headers={"User-Agent": user_agent},
            timeout=timeout,
            follow_redirects=True,
            transport=transport,
        )
        self._min_interval = min_interval
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._backoff_cap = backoff_cap
        self._clock = clock
        self._sleep = sleep
        self._last_request_at: float | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def get(self, url: str, params: Mapping[str, Any] | None = None) -> httpx.Response:
        """GET with rate limiting and retries. Raises httpx.HTTPStatusError on 4xx
        (other than 429) and RetryExhausted when transient failures persist."""
        last_error: str = ""
        for attempt in range(self._max_retries + 1):
            self._wait_for_slot()
            try:
                response = self._client.get(url, params=params)
            except httpx.TransportError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                delay = self._backoff(attempt)
            else:
                if response.status_code not in RETRY_STATUSES:
                    response.raise_for_status()
                    return response
                last_error = f"HTTP {response.status_code}"
                delay = self._retry_after(response) or self._backoff(attempt)
            if attempt < self._max_retries:
                log.warning("GET %s failed (%s); retrying in %.1fs", url, last_error, delay)
                self._sleep(delay)
        raise RetryExhausted(
            f"GET {url} failed after {self._max_retries + 1} attempts: {last_error}"
        )

    def _wait_for_slot(self) -> None:
        now = self._clock()
        if self._last_request_at is not None:
            wait = self._min_interval - (now - self._last_request_at)
            if wait > 0:
                self._sleep(wait)
                now = self._clock()
        self._last_request_at = now

    def _backoff(self, attempt: int) -> float:
        return min(self._backoff_base * (2**attempt), self._backoff_cap)

    def _retry_after(self, response: httpx.Response) -> float | None:
        value = response.headers.get("Retry-After")
        if value is None:
            return None
        try:
            return min(max(float(value), 0.0), self._backoff_cap)
        except ValueError:
            return None  # HTTP-date form: fall back to exponential backoff
