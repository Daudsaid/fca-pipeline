"""
Extract firm data from the FCA Financial Services Register API.

The free API has no "list every firm" endpoint and returns one firm per call,
so getting as much data as possible is done in resumable modes:

  sweep    Try every Firm Reference Number (FRN) in a range (default 100000-1050000).
           Numbers that exist are fetched; misses are skipped. The position is
           checkpointed, so a multi-day backfill can be stopped and resumed.
  refresh  Re-fetch known firms, least-recently-fetched first, so every firm is
           revisited in rotation and status changes get picked up.
  frontier Walk the newest FRN range (around the highest FRN found so far) for
           newly registered firms. It has its own checkpoint and, in auto mode,
           a fixed share of the run. Once it has caught up with the top of the
           range it re-checks just a small window there, once a day.
  search   Discover FRNs through the Search endpoint (quick, but only finds what
           the search terms match).
  auto     (default) refresh up to --limit firms, give the frontier its share of
           the remaining time, then continue the sweep. The scheduled job runs this.
  status   Show progress. No API calls.

Requests run in parallel (FCA_WORKERS threads, default 4) while the client keeps
the overall pace at one request start every FCA_MIN_INTERVAL seconds. Work is
done in chunks of FRNs, and the checkpoint only moves past a chunk once every
FRN in it has been answered, so an outage or a crash can never skip a firm.

If FCA_CHECKPOINT_CMD is set, it is run every FCA_CHECKPOINT_MINUTES (default 45)
during a long extract. The scheduled job uses it to load what has been fetched
so far and upload the warehouse, so a lost runner costs minutes, not hours.

Every fetched firm is appended as one JSON line to data/raw/firms_<YYYY-MM-DD>.jsonl
(firm record + addresses). Progress lives in data/raw/extract_state.json.

Examples:
    python extract/extract_fca.py sweep --max-minutes 600     # backfill for 10 hours
    python extract/extract_fca.py refresh --limit 500
    python extract/extract_fca.py frontier --max-minutes 60
    python extract/extract_fca.py search --limit 200
    python extract/extract_fca.py --limit 2000 --max-minutes 290
    python extract/extract_fca.py status
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import string
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
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

# Frontier: FRNs are allocated when a firm applies and can appear on the
# Register months later, so once caught up we re-check a window below the
# highest FRN seen as well as a band above it.
FRONTIER_BELOW = 3_000
FRONTIER_ABOVE = 1_000

CHUNK = 100  # FRNs fetched in parallel before the checkpoint moves on

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


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except ValueError:
        return default


# --------------------------------------------------------------------------- state
class State:
    """JSON state file: sweep + frontier positions, and when each known firm was last fetched."""

    def __init__(self, raw_dir: Path):
        self.raw_dir = raw_dir
        self.path = raw_dir / "extract_state.json"
        self.lock = threading.RLock()
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

    def known(self, frn: str) -> bool:
        return bool(self.firms.get(frn))

    def mark(self, frn: str, when: str) -> None:
        with self.lock:
            self.firms[frn] = when

    def save(self) -> None:
        with self.lock:
            payload = json.dumps(
                {"sweep": self.sweep, "frontier": self.frontier, "firms": self.firms},
                indent=0, sort_keys=True,
            )
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(payload)
        tmp.replace(self.path)

    def report(self) -> None:
        sw, fr = self.sweep, self.frontier
        fetched = sum(1 for v in self.firms.values() if v)
        print(f"Known firms:     {len(self.firms):,} ({fetched:,} fetched at least once)")
        if sw:
            end, nxt = sw.get("end", DEFAULT_SWEEP_END), sw.get("next_frn", DEFAULT_SWEEP_START)
            pct = 100 * (nxt - DEFAULT_SWEEP_START) / max(1, end - DEFAULT_SWEEP_START)
            print(f"Sweep position:  FRN {nxt:,} of {end:,} ({min(100.0, pct):.1f}% done, "
                  f"{sw.get('found', 0):,} found)")
        else:
            print("Sweep position:  not started")
        oldest = min((v for v in self.firms.values() if v), default="")
        print(f"Oldest refresh:  {oldest[:19] or 'n/a'}")
        highest = max((int(f) for f in self.firms if f.isdigit()), default=0)
        if fr.get("caught_up"):
            where = f"caught up; daily check last ran {fr.get('last_run', 'never')[:19]}"
        elif fr.get("next_frn"):
            where = f"catching up, at FRN {fr['next_frn']:,}"
        else:
            where = "not started"
        print(f"Frontier:        highest FRN {highest:,}; {where}")


# ------------------------------------------------------------------------ fetching
class Extractor:
    def __init__(self, raw_dir: Path, max_minutes: float | None, workers: int | None = None):
        self.raw_dir = raw_dir
        raw_dir.mkdir(parents=True, exist_ok=True)
        self.state = State(raw_dir)
        self.client = FCAClient(os.getenv("FCA_API_EMAIL", ""), os.getenv("FCA_API_KEY", ""))
        self.deadline = time.monotonic() + max_minutes * 60 if max_minutes else None
        self.workers = max(1, workers or int(env_float("FCA_WORKERS", 4)))
        self.pool = ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="fca")
        self.written = 0
        self._write_lock = threading.Lock()
        self.checkpoint_cmd = os.getenv("FCA_CHECKPOINT_CMD", "").strip()
        self.checkpoint_every = env_float("FCA_CHECKPOINT_MINUTES", 45) * 60
        self._last_checkpoint = time.monotonic()
        self.checkpoints = 0

    def close(self) -> None:
        self.pool.shutdown(wait=False, cancel_futures=True)

    # ---- helpers
    def time_left(self, deadline: float | None = None) -> bool:
        limits = [d for d in (self.deadline, deadline) if d is not None]
        return not limits or time.monotonic() < min(limits)

    def _out_file(self) -> Path:
        return self.raw_dir / f"firms_{utcnow():%Y-%m-%d}.jsonl"

    def fetch_and_write(self, frn: str) -> bool:
        """Fetch one firm (+ addresses). Returns False if the FRN doesn't exist.
        Thread-safe: the network calls run in parallel, the write is serialised."""
        firm = self.client.firm(frn)
        if firm is None:
            return False
        record = {
            "frn": frn,
            "extracted_at": utcnow().isoformat(),
            "firm": firm,
            "addresses": self.client.firm_addresses(frn),
        }
        line = json.dumps(record) + "\n"
        with self._write_lock:
            with self._out_file().open("a") as fh:
                fh.write(line)
            self.state.mark(frn, record["extracted_at"])
            self.written += 1
        return True

    def fetch_many(self, frns: list[str]) -> dict[str, bool]:
        """Fetch FRNs in parallel. Raises FCAUnavailable if any of them can't be answered,
        so the caller doesn't move its checkpoint past this batch."""
        if not frns:
            return {}
        return dict(zip(frns, self.pool.map(self.fetch_and_write, frns)))

    def maybe_checkpoint(self, force: bool = False) -> None:
        """Every FCA_CHECKPOINT_MINUTES, save state and run FCA_CHECKPOINT_CMD (e.g. load + upload)."""
        if not self.checkpoint_cmd:
            return
        if not force and time.monotonic() - self._last_checkpoint < self.checkpoint_every:
            return
        self.state.save()
        log.info("Checkpoint: running %r", self.checkpoint_cmd)
        t0 = time.monotonic()
        try:
            rc = subprocess.run(self.checkpoint_cmd, shell=True, cwd=PROJECT_ROOT, timeout=1200).returncode
        except subprocess.TimeoutExpired:
            rc = "timeout"
        log.info("Checkpoint: finished (exit %s) in %.0fs", rc, time.monotonic() - t0)
        self.checkpoints += 1
        self._last_checkpoint = time.monotonic()

    # ---- modes
    def refresh(self, limit: int) -> int:
        """Re-fetch up to `limit` known firms, least recently fetched first."""
        queue = sorted(self.state.firms, key=lambda f: self.state.firms[f])[:limit]
        log.info("Refresh: %s of %s known firms (oldest first, %s workers)",
                 len(queue), len(self.state.firms), self.workers)
        done = 0
        for i in range(0, len(queue), CHUNK):
            if not self.time_left():
                log.info("Refresh: time limit reached")
                break
            batch = queue[i:i + CHUNK]
            now = utcnow().isoformat()
            for frn, found in self.fetch_many(batch).items():
                if not found:
                    # no longer returned by the API: mark as checked so it rotates to the back
                    self.state.mark(frn, now)
            done += len(batch)
            self.state.save()
            if (i // CHUNK) % 10 == 9:
                log.info("Refresh: %s/%s", done, len(queue))
            self.maybe_checkpoint()
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
        self.state.save()

        frn, stop = sw["next_frn"], sw["end"]
        if frn > stop:
            log.info("Sweep: already complete up to %s. Use --start to sweep again.", stop)
            return 0
        log.info("Sweep: resuming at FRN %s (end %s, %s found so far, %s workers)",
                 frn, stop, sw["found"], self.workers)

        found, chunks, t0, start_frn = 0, 0, time.monotonic(), frn
        while frn <= stop and self.time_left() and (limit is None or found < limit):
            hi = min(frn + CHUNK - 1, stop)
            candidates = [str(n) for n in range(frn, hi + 1) if not self.state.known(str(n))]
            hits = sum(self.fetch_many(candidates).values())
            found += hits
            sw["found"] += hits
            frn = hi + 1
            sw["next_frn"] = frn
            self.state.save()
            chunks += 1
            if chunks % 20 == 0:
                rate = (frn - start_frn) / max(1e-9, time.monotonic() - t0)
                log.info(
                    "Sweep: at FRN %s | %s new firms this run, %s total | %.2f FRNs/s | ~%.0f h left",
                    frn, found, sw["found"], rate, (stop - frn) / max(rate, 1e-9) / 3600,
                )
            self.maybe_checkpoint()
        self.state.save()
        if frn > stop:
            log.info("Sweep: COMPLETE. %s firms found up to FRN %s", sw["found"], stop)
        return found

    def frontier_scan(self, budget_minutes: float | None = None, force: bool = False) -> int:
        """Walk up from the frontier checkpoint to just past the highest known FRN.

        While catching up it resumes from its own checkpoint each run and only uses
        `budget_minutes`. Once caught up, it re-checks a window around the top
        (FRONTIER_BELOW under to FRONTIER_ABOVE over the highest FRN) once a day.
        """
        fr = self.state.frontier
        known = [int(f) for f in self.state.firms if f.isdigit()]
        if not known:
            log.info("Frontier: no known firms yet, skipping")
            return 0
        highest = max(max(known), int(fr.get("highest", 0)))
        today = f"{utcnow():%Y-%m-%d}"
        if fr.get("caught_up") and not force and str(fr.get("last_run", "")).startswith(today):
            log.info("Frontier: caught up and already checked today, skipping")
            return 0
        if fr.get("caught_up") or not fr.get("next_frn"):
            frn = max(DEFAULT_SWEEP_START, highest - FRONTIER_BELOW)
        else:
            frn = int(fr["next_frn"])
        top = highest + FRONTIER_ABOVE
        local_deadline = time.monotonic() + budget_minutes * 60 if budget_minutes else None
        log.info("Frontier: checking from FRN %s up to %s (highest known %s, budget %s min)",
                 frn, top, highest, f"{budget_minutes:.0f}" if budget_minutes else "none")

        found = 0
        while frn <= top and self.time_left(local_deadline):
            hi = min(frn + CHUNK - 1, top)
            candidates = [str(n) for n in range(frn, hi + 1) if not self.state.known(str(n))]
            for f, ok in self.fetch_many(candidates).items():
                if ok:
                    found += 1
                    highest = max(highest, int(f))
            top = max(top, highest + FRONTIER_ABOVE)  # new firms near the top extend the scan
            frn = hi + 1
            fr.update(next_frn=frn, highest=highest, caught_up=False)
            self.state.save()
            self.maybe_checkpoint()

        if frn > top:
            fr.update(caught_up=True, last_run=utcnow().isoformat(), highest=highest)
            fr.pop("next_frn", None)
            log.info("Frontier: caught up at FRN %s; %s new firms", highest, found)
        else:
            log.info("Frontier: paused at FRN %s (budget used); %s new firms", frn, found)
        fr["last_found"] = found
        self.state.save()
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
                if not frn or self.state.known(frn):
                    continue
                if self.fetch_and_write(frn):
                    found += 1
                    new += 1
                if found >= limit or not self.time_left():
                    break
            log.info("Search '%s': %s hits, %s new firms (total new %s)", term, hits, new, found)
        self.state.save()
        return found

    def auto(self, limit: int, max_minutes: float | None, start: int | None, end: int | None) -> None:
        """Scheduled run: refresh, then the frontier's share of the time, then the sweep."""
        self.refresh(limit)
        if not self.time_left():
            return
        budget = None
        if max_minutes and self.deadline:
            remaining = (self.deadline - time.monotonic()) / 60
            budget = max(1.0, remaining * env_float("FCA_FRONTIER_SHARE", 0.35))
        self.frontier_scan(budget_minutes=budget)
        if max_minutes and self.time_left():
            self.sweep(start, end)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mode", nargs="?", default="auto",
                        choices=["auto", "sweep", "refresh", "frontier", "search", "status"])
    parser.add_argument("--limit", type=int, help="max firms for refresh/search/auto (default FCA_FIRM_LIMIT or 2000)")
    parser.add_argument("--max-minutes", type=float, help="stop cleanly after this many minutes")
    parser.add_argument("--start", type=int, help=f"sweep: first FRN (default {DEFAULT_SWEEP_START}, else resume)")
    parser.add_argument("--end", type=int, help=f"sweep: last FRN (default {DEFAULT_SWEEP_END})")
    parser.add_argument("--workers", type=int, help="parallel requests (default FCA_WORKERS or 4)")
    parser.add_argument("--raw-dir", help="output folder (default data/raw)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raw_dir = Path(args.raw_dir or os.getenv("RAW_DATA_DIR") or PROJECT_ROOT / "data" / "raw")
    limit = args.limit or int(os.getenv("FCA_FIRM_LIMIT", "2000"))

    if args.mode == "status":
        State(raw_dir).report()
        return

    ex = Extractor(raw_dir, args.max_minutes, args.workers)
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
            ex.frontier_scan(budget_minutes=args.max_minutes, force=True)
        elif args.mode == "sweep":
            ex.sweep(args.start, args.end, args.limit)
        else:
            ex.auto(limit, args.max_minutes, args.start, args.end)
    except FCAUnavailable as exc:
        ex.state.save()
        log.error("%s. Progress is saved; re-run to resume.", exc)
        sys.exit(2)
    except KeyboardInterrupt:
        ex.state.save()
        log.info("Stopped. Progress is saved; re-run to resume.")
        sys.exit(130)
    finally:
        ex.close()
        minutes = (time.monotonic() - t0) / 60
        log.info(
            "Run finished: %s firms written, %s API calls (%.2f/s), %s checkpoints, %.1f min",
            ex.written, ex.client.calls, ex.client.calls / max(1e-9, minutes * 60), ex.checkpoints, minutes,
        )


if __name__ == "__main__":
    main()
