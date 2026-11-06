"""Shared fixtures: a stand-in FCA Register API running on a local port."""
from __future__ import annotations

import json
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "extract"))


class FakeRegister:
    """In-memory Register. `firms` maps FRN -> firm dict; `mode` simulates failures."""

    def __init__(self):
        self.firms: dict[str, dict] = {}
        self.mode = "ok"            # "ok" | "down" (503 always) | "throttle" (429 above max_per_sec)
        self.max_per_sec = 20
        self.calls = 0
        self._recent: deque = deque()

    def add(self, frn: int | str, status: str = "Authorised", name: str | None = None):
        frn = str(frn)
        self.firms[frn] = {
            "FRN": frn,
            "Organisation Name": name or f"Firm {frn} Ltd",
            "Status": status,
            "Status Effective Date": "01/12/2001",
            "Business Type": "Regulated",
            "System Timestamp": "06/11/2026 10:00",
            "Name": f"https://register.fca.org.uk/services/V0.1/Firm/{frn}/Names",
        }


def _handler(reg: FakeRegister):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _send(self, code: int, body: dict):
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def do_GET(self):
            reg.calls += 1
            if self.headers.get("X-Auth-Key") != "test-key":
                return self._send(403, {})
            if reg.mode == "down":
                return self._send(503, {})
            if reg.mode == "throttle":
                now = time.monotonic()
                reg._recent.append(now)
                while reg._recent and now - reg._recent[0] > 1:
                    reg._recent.popleft()
                if len(reg._recent) > reg.max_per_sec:
                    return self._send(429, {})

            url = urlparse(self.path)
            parts = url.path.strip("/").split("/")
            if parts[-1] == "Search":
                q = parse_qs(url.query).get("q", [""])[0].lower()
                hits = [
                    {"Reference Number": f, "Name": d["Organisation Name"]}
                    for f, d in sorted(reg.firms.items())
                    if q in d["Organisation Name"].lower()
                ]
                return self._send(200, {"Data": hits or None, "ResultInfo": {
                    "page": "1", "per_page": str(max(1, len(hits))), "total_count": str(len(hits))}})
            if "Firm" in parts:
                frn = parts[parts.index("Firm") + 1]
                firm = reg.firms.get(frn)
                if firm is None:
                    return self._send(200, {"Message": "Firm not found", "Data": None})
                if parts[-1] == "Address":
                    return self._send(200, {"Data": [
                        {"Address Type": "Complaints Contact", "Town": "Elsewhere"},
                        {"Address Type": "Principal Place of Business", "Address Line 1": "1 Test St",
                         "Town": "London", "Postcode": "ec2v 8rf", "Country": "UNITED KINGDOM"},
                    ]})
                return self._send(200, {"Message": "Ok. Firm Found", "Data": [firm]})
            return self._send(404, {})

    return Handler


@pytest.fixture
def fake_api(monkeypatch):
    reg = FakeRegister()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler(reg))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("FCA_API_BASE_URL", f"http://127.0.0.1:{server.server_port}/services/V0.1")
    monkeypatch.setenv("FCA_API_EMAIL", "test@example.com")
    monkeypatch.setenv("FCA_API_KEY", "test-key")
    monkeypatch.setenv("FCA_MIN_INTERVAL", "0")

    import fca_client
    monkeypatch.setattr(fca_client.FCAClient, "_backoff", staticmethod(lambda attempt, reason: None))
    yield reg
    server.shutdown()
