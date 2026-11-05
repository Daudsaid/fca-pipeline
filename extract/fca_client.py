"""
Thin client for the FCA Financial Services Register API (free tier).

Docs / sign-up: https://register.fca.org.uk/Developer/s/
Auth: two headers, X-Auth-Email and X-Auth-Key.

The free API returns one entity per call and is rate limited, so this client:
  * spaces calls out (FCA_MIN_INTERVAL seconds between requests, default 0.5)
  * slows down (x1.5) whenever the API answers 429, then speeds back up
    gradually (5% per 100 successful calls) to the configured pace
  * retries on 429 / 5xx / network errors with exponential backoff
  * returns None for "not found", and raises FCAUnavailable when the API keeps
    failing, so callers never mistake an outage for "firm does not exist"
"""
from __future__ import annotations

import logging
import math
import os
import time
from typing import Iterator, Optional

import requests

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://register.fca.org.uk/services/V0.1"


class FCAUnavailable(RuntimeError):
    """The API kept failing after all retries (outage, network down, blocked)."""


class FCAClient:
    def __init__(
        self,
        email: str,
        api_key: str,
        base_url: Optional[str] = None,
        min_interval: Optional[float] = None,
        max_retries: int = 5,
        timeout: int = 30,
    ):
        if not email or not api_key:
            raise ValueError("FCA_API_EMAIL and FCA_API_KEY must be set (see .env.example)")
        self.base_url = (base_url or os.getenv("FCA_API_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.min_interval = min_interval if min_interval is not None else float(os.getenv("FCA_MIN_INTERVAL", "0.5"))
        self.base_interval = self.min_interval
        self.calls = 0
        self._ok_streak = 0
        self.max_retries = max_retries
        self.timeout = timeout
        self._last_call = 0.0
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Auth-Email": email,
                "X-Auth-Key": api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    # ------------------------------------------------------------------ core
    def _throttle(self) -> None:
        wait = self.min_interval - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def get(self, path: str, params: Optional[dict] = None) -> Optional[dict]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            self.calls += 1
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                self._backoff(attempt, f"network error: {exc}")
                continue

            if resp.status_code == 404:
                return None
            if resp.status_code in (401, 403):
                raise RuntimeError(
                    f"FCA API refused credentials ({resp.status_code}). "
                    "Check FCA_API_EMAIL / FCA_API_KEY."
                )
            if resp.status_code == 429:
                self._ok_streak = 0
                new = min(5.0, self.min_interval * 1.5)
                if new > self.min_interval:
                    log.warning("Rate limited (429): slowing to one call every %.2fs", new)
                self.min_interval = new
            if resp.status_code == 429 or resp.status_code >= 500:
                self._backoff(attempt, f"HTTP {resp.status_code}")
                continue

            resp.raise_for_status()
            self._ok_streak += 1
            if self._ok_streak >= 100 and self.min_interval > self.base_interval:
                self.min_interval = max(self.base_interval, self.min_interval * 0.95)
                self._ok_streak = 0
            try:
                return resp.json()
            except ValueError:
                self._backoff(attempt, "invalid JSON")
                continue

        raise FCAUnavailable(f"Giving up on {url} after {self.max_retries} attempts")

    @staticmethod
    def _backoff(attempt: int, reason: str) -> None:
        delay = min(120, 5 * 2 ** (attempt - 1))
        log.warning("Attempt %s failed (%s); sleeping %ss", attempt, reason, delay)
        time.sleep(delay)

    @staticmethod
    def data(payload: Optional[dict]) -> list:
        """The API wraps results in {"Data": [...]}; Data can be null."""
        if not payload:
            return []
        data = payload.get("Data")
        if data is None:
            return []
        return data if isinstance(data, list) else [data]

    # ------------------------------------------------------------- endpoints
    def search_firms(self, term: str, max_pages: int = 50) -> Iterator[dict]:
        """Yield firm search hits for a term, following pagination."""
        page = 1
        while page <= max_pages:
            payload = self.get("Search", {"q": term, "type": "firm", "pgnp": page})
            hits = self.data(payload)
            if not hits:
                return
            yield from hits

            info = (payload or {}).get("ResultInfo") or {}
            try:
                total = int(info.get("total_count") or 0)
                per_page = int(info.get("per_page") or len(hits))
            except (TypeError, ValueError):
                return
            if per_page <= 0 or page >= math.ceil(total / per_page):
                return
            page += 1

    def firm(self, frn: str) -> Optional[dict]:
        rows = self.data(self.get(f"Firm/{frn}"))
        return rows[0] if rows else None

    def firm_addresses(self, frn: str) -> list:
        return self.data(self.get(f"Firm/{frn}/Address"))
