"""
Extract firm data from the FCA Financial Services Register API.

The free API has no "list every firm" endpoint and returns one firm per call,
so getting as much data as possible is done in resumable modes:

  sweep    Try every Firm Reference Number (FRN) in a range (default 100000-1050000).
           Numbers that exist are fetched; misses are skipped. The position is
           checkpointed, so a multi-day backfill can be stopped and resumed.
  refresh  Re-fetch known firms, least-recently-fetched first, so every firm is
           revisited in rotation and status changes get picked up.
  frontier Check the newest FRN range (around the highest FRN found so far) for
           newly registered firms. FRNs are issued roughly in order, so this is
           how firms registered after the backfill get discovered.
  search   Discover FRNs through the Search endpoint (quick, but only finds what
           the search terms match).
  auto     (default) refresh up to --limit firms, run the frontier scan (once a
           day), then spend any remaining --max-minutes continuing the sweep.
           This is what the scheduled job runs.
  status   Show progress. No API calls.

Every fetched firm is appended as one JSON line to data/raw/firms_<YYYY-MM-DD>.jsonl
(firm record + addresses). Progress lives in data/raw/extract_state.json.

Examples:
    python extract/extract_fca.py sweep --max-minutes 600     # backfill for 10 hours
    python extract/extract_fca.py refresh --limit 500
    python extract/extract_fca.py frontier
    python extract/extract_fca.py search --limit 200
    python extract/extract_fca.py --limit 2000 --max-minutes 330
    python extract/extract_fca.py status
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import string
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fca_client import FCAClient, FCAUnavailable  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

log = logging.getLogger("extract_fca")

DEFAULT_SWEEP_START = 100_000
DEFAULT_SWEEP_END = 1_050_000

# Frontier scan: FRNs are allocated when a firm applies and can appear on the
# Register months later, so re-check a window below the highest FRN seen as
# well as a band above it.
FRONTIER_BELOW = 3_000
FRONTIER_ABOVE = 1_000

SEARCH_TERMS = [
    "plc", "bank", "capital", "finance", "financial", "insurance", "investment",
    "investments", "asset", "wealth", "mortgage", "mortgages", "credit", "partners",
    "group", "advisers", "advisory", "services", "management", "securities", "fund",
    "payments", "money", "lending", "loans", "brokers", "broker", "trust", "holdings",
    "associates", "consulting", "planning", "pensions", "life", "motor", "cars",
    "estate", "property", "homes", "solutions", "london", "international", "global",
    "limited", "ltd", "llp",
] + list(string.ascii_lowercase)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------- state
class State:
    """JSON state file: sweep position + when each known firm was last fetched."""

    def __init__(self, raw_dir: Path):
        self.raw_dir = raw_dir
        self.path = raw_dir / "extract_state.json"
        data = json.loads(self.path.read_text()) if self.path.exists() else {}
        self.sweep: dict = data.get("sweep", {})
        self.frontier: dict = data.get("frontier", {})
        self.firms: dict[str, str] = data.get("firms", {})  # frn -> last fetched ISO time
        if not data:
            self._seed_from_existing_files()

    def _seed_from_existing_files(self) -> None:
        """First run: learn known firms from JSONL already on disk (and an old frns.txt)."""
        for f in sorted(self.raw_dir.glob("firms_*.jsonl")):
            with f.open() as fh:
                for line in fh:
                    try:
                        rec = json.loads(line)
                    except ValueError:
                        continue
                    frn, ts = rec.get("frn"), rec.get("extracted_at", "")
                    if frn and ts >= self.firms.get(frn, ""):
                        self.firms[frn] = ts
        old = self.raw_dir / "frns.txt"
        if old.exists():
            for frn in old.read_text().split():
                self.firms.setdefault(frn, "")
        if self.firms:
            log.info("Seeded state with %s known firms from existing files", len(self.firms))
        self.save()

    def save(self) -> None:
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(
                {"sweep": self.sweep, "frontier": self.frontier, "firms": self.firms},
                indent=0, sort_keys=True,
            ))
        tmp.replace(self.path)

    def report(self) -> None:
        sw = self.sweep
        fetched = sum(1 for v in self.firms.values() if v)
        print(f"Known firms:     {len(self.firms):,} ({fetched:,} fetched at least once)")
        if sw:
            end, nxt = sw.get("end", DEFAULT_SWEEP_END), sw.get("next_frn", DEFAULT_SWEEP_START)
            pct = 100 * (nxt - DEFAULT_SWEEP_START) / max(1, end - DEFAULT_SWEEP_START)
            print(f"Sweep position:  FRN {nxt:,} of {end:,} ({min(100.0, pct):.1f}% done, {sw.get('found', 0):,} found)")
        else:
            print("Sweep position:  not started")
        oldest = min((v for v in self.firms.values() if v), default="")
        print(f"Oldest refresh:  {oldest[:19] or 'n/a'}")
        highest = max((int(f) for f in self.firms if f.isdigit()), default=0)
        fr = self.frontier
        print(
            f"Frontier:        highest FRN {highest:,}; last scan {fr.get('last_run', 'never')[:19]}"
            + (f" ({fr.get('last_found', 0)} new)" if fr else "")
        )


# ------------------------------------------------------------------------ fetching
class Extractor:
    def __init__(self, raw_dir: Path, max_minutes: float | None):
        self.raw_dir = raw_dir
        raw_dir.mkdir(parents=True, exist_ok=True)
        self.state = State(raw_dir)
        self.client = FCAClient(os.getenv("FCA_API_EMAIL", ""), os.getenv("FCA_API_KEY", ""))
        self.deadline = time.monotonic() + max_minutes * 60 if max_minutes else None
        self.written = 0
        self._since_save = 0

    def time_left(self) -> bool:
        return self.deadline is None or time.monotonic() < self.deadline

    def _out_file(self) -> Path:
        return self.raw_dir / f"firms_{utcnow():%Y-%m-%d}.jsonl"

    def fetch_and_write(self, frn: str) -> bool:
        """Fetch one firm (+ addresses). Returns False if the FRN doesn't exist."""
        firm = self.client.firm(frn)
        if firm is None:
            return False
        record = {
            "frn": frn,
            "extracted_at": utcnow().isoformat(),
            "firm": firm,
            "addresses": self.client.firm_addresses(frn),
        }
        with self._out_file().open("a") as fh:
            fh.write(json.dumps(record) + "\n")
        self.state.firms[frn] = record["extracted_at"]
        self.written += 1
        self._since_save += 1
        if self._since_save >= 25:
            self.state.save()
            self._since_save = 0
        return True

    # ---- modes
    def refresh(self, limit: int) -> int:
        """Re-fetch up to `limit` known firms, least recently fetched first."""
        queue = sorted(self.state.firms, key=lambda f: self.state.firms[f])[:limit]
        log.info("Refresh: %s of %s known firms (oldest first)", len(queue), len(self.state.firms))
        done = 0
        for i, frn in enumerate(queue, 1):
            if not self.time_left():
                log.info("Refresh: time limit reached")
                break
            if not self.fetch_and_write(frn):
                # no longer returned by the API: mark as checked so it rotates to the back
                self.state.firms[frn] = utcnow().isoformat()
            done += 1
            if i % 100 == 0:
                log.info("Refresh: %s/%s", i, len(queue))
        self.state.save()
        return done

    def sweep(self, start: int | None, end: int | None, limit: int | None = None) -> int:
        """Walk the FRN range from the checkpoint; fetch every FRN that exists."""
        sw = self.state.sweep
        if start is not None or "next_frn" not in sw:
            sw["next_frn"] = start if start is not None else DEFAULT_SWEEP_START
            sw["found"] = 0
        if end is not None or "end" not in sw:
            sw["end"] = end if end is not None else DEFAULT_SWEEP_END
        sw.setdefault("found", 0)

        frn, stop = sw["next_frn"], sw["end"]
        if frn > stop:
            log.info("Sweep: already complete up to %s. Use --start to sweep again.", stop)
            return 0
        log.info("Sweep: resuming at FRN %s (end %s, %s found so far)", frn, stop, sw["found"])

        found, checked, t0 = 0, 0, time.monotonic()
        while frn <= stop and self.time_left() and (limit is None or found < limit):
            key = str(frn)
            if not self.state.firms.get(key):  # skip firms we already have
                if self.fetch_and_write(key):
                    found += 1
                    sw["found"] += 1
            frn += 1
            checked += 1
            sw["next_frn"] = frn
            if checked % 200 == 0:
                self.state.save()
                rate = checked / max(1e-9, time.monotonic() - t0)
                log.info(
                    "Sweep: at FRN %s | %s new firms this run, %s total | %.2f FRNs/s | ~%.0f h left",
                    frn, found, sw["found"], rate, (stop - frn) / rate / 3600,
                )
        self.state.save()
        if frn > stop:
            log.info("Sweep: COMPLETE. %s firms found up to FRN %s", sw["found"], stop)
        return found

    def frontier_scan(self, force: bool = False) -> int:
        """Look for newly registered firms around the highest known FRN (once a day)."""
        today = f"{utcnow():%Y-%m-%d}"
        if not force and self.state.frontier.get("last_run", "").startswith(today):
            log.info("Frontier: already scanned today, skipping")
            return 0
        known = [int(f) for f in self.state.firms if f.isdigit()]
        if not known:
            log.info("Frontier: no known firms yet, skipping")
            return 0
        highest = max(known)
        lo, hi = max(DEFAULT_SWEEP_START, highest - FRONTIER_BELOW), highest + FRONTIER_ABOVE
        log.info("Frontier: checking FRNs %s-%s for new firms (highest known %s)", lo, hi, highest)

        found, frn = 0, lo
        while frn <= hi and self.time_left():
            key = str(frn)
            if not self.state.firms.get(key) and self.fetch_and_write(key):
                found += 1
                if frn > highest - 50:  # new firms near the top: keep scanning above them
                    highest = frn
                    hi = max(hi, frn + FRONTIER_ABOVE)
            frn += 1
        complete = frn > hi
        self.state.frontier = {
            "last_run": utcnow().isoformat() if complete else self.state.frontier.get("last_run", ""),
            "last_found": found,
            "highest": highest,
        }
        self.state.save()
        log.info("Frontier: %s new firms (scan %s)", found, "complete" if complete else "stopped early")
        return found

    def search(self, limit: int) -> int:
        """Discover new FRNs via search terms, fetching each new firm."""
        found = 0
        for term in SEARCH_TERMS:
            if found >= limit or not self.time_left():
                break
            hits, new = 0, 0
            for hit in self.client.search_firms(term):
                hits += 1
                frn = str(hit.get("Reference Number") or hit.get("FRN") or "").strip()
                if not frn or self.state.firms.get(frn):
                    continue
                if self.fetch_and_write(frn):
                    found += 1
                    new += 1
                if found >= limit or not self.time_left():
                    break
            log.info("Search '%s': %s hits, %s new firms (total new %s)", term, hits, new, found)
        self.state.save()
        return found


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mode", nargs="?", default="auto", choices=["auto", "sweep", "refresh", "frontier", "search", "status"])
    parser.add_argument("--limit", type=int, help="max firms for refresh/search/auto (default FCA_FIRM_LIMIT or 2000)")
    parser.add_argument("--max-minutes", type=float, help="stop cleanly after this many minutes")
    parser.add_argument("--start", type=int, help=f"sweep: first FRN (default {DEFAULT_SWEEP_START}, else resume)")
    parser.add_argument("--end", type=int, help=f"sweep: last FRN (default {DEFAULT_SWEEP_END})")
    parser.add_argument("--raw-dir", help="output folder (default data/raw)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raw_dir = Path(args.raw_dir or os.getenv("RAW_DATA_DIR") or PROJECT_ROOT / "data" / "raw")
    limit = args.limit or int(os.getenv("FCA_FIRM_LIMIT", "2000"))

    if args.mode == "status":
        State(raw_dir).report()
        return

    ex = Extractor(raw_dir, args.max_minutes)
    t0 = time.monotonic()

    def _on_sigterm(signum, frame):  # e.g. a CI time limit: stop like Ctrl+C so progress is saved
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, _on_sigterm)
    try:
        if args.mode == "refresh":
            ex.refresh(limit)
        elif args.mode == "search":
            ex.search(limit)
        elif args.mode == "frontier":
            ex.frontier_scan(force=True)
        elif args.mode == "sweep":
            ex.sweep(args.start, args.end, args.limit)
        else:  # auto
            ex.refresh(limit)
            if ex.time_left():
                ex.frontier_scan()
            if args.max_minutes and ex.time_left():
                ex.sweep(args.start, args.end)
    except FCAUnavailable as exc:
        ex.state.save()
        log.error("%s. Progress is saved; re-run to resume.", exc)
        sys.exit(2)
    except KeyboardInterrupt:
        ex.state.save()
        log.info("Stopped. Progress is saved; re-run to resume.")
        sys.exit(130)
    finally:
        log.info(
            "Run finished: %s firms written, %s API calls, %.1f min",
            ex.written, ex.client.calls, (time.monotonic() - t0) / 60,
        )


if __name__ == "__main__":
    main()
