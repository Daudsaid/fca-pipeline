"""
Build a static HTML dashboard from the DuckDB warehouse.

    python dashboard/build_dashboard.py                    # fca.duckdb -> site/index.html
    python dashboard/build_dashboard.py --db fca.duckdb --out site/index.html

The page is self-contained (no external scripts): the numbers are queried here,
embedded as JSON, and drawn in the browser with plain SVG. The scheduled
workflow rebuilds it after every run and publishes it with GitHub Pages.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = Path(__file__).with_name("template.html")

LEFT_REGISTER = ("no longer authorised", "revoked", "cancelled - authorised emi",
                 "cancelled - small emi", "no longer registered as an appointed representative")


def rows(con, sql: str) -> list[dict]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def one(con, sql: str):
    return con.execute(sql).fetchone()[0]


def sentence(s: str | None) -> str:
    if not s:
        return "Unknown"
    return s[:1].upper() + s[1:]


def collect(db: Path, state_file: Path | None) -> dict:
    con = duckdb.connect(str(db), read_only=True)
    left = ", ".join(f"'{s}'" for s in LEFT_REGISTER)

    status = rows(con, """
        select coalesce(status, 'unknown') as status, count(*) as n
        from marts.mart_fca_firms_current group by 1 order by 2 desc
    """)
    top, rest = status[:7], status[7:]
    if rest:
        top.append({"status": "other", "n": sum(r["n"] for r in rest)})
    for r in top:
        r["label"] = sentence(r["status"])

    exits = rows(con, f"""
        select extract(year from status_effective_date)::int as year, count(*) as n
        from marts.mart_fca_firms_current
        where status in ({left}) and status_effective_date >= date '2002-01-01'
          and status_effective_date <= current_date
        group by 1 order by 1
    """)

    towns = rows(con, """
        select upper(trim(town)) as town, count(*) as n
        from marts.mart_fca_firms_current
        where town is not null and status in ('authorised', 'registered', 'eea authorised',
              'authorised - applied to cancel', 'appointed representative',
              'appointed representative - introducer')
        group by 1 order by 2 desc limit 10
    """)

    for r in towns:
        r["town"] = r["town"].title()

    types = rows(con, """
        select coalesce(business_type, 'Unknown') as type, count(*) as n
        from marts.mart_fca_firms_current group by 1 order by 2 desc limit 6
    """)

    changes = rows(con, """
        select frn, firm_name, status_before, status_after,
               strftime(effective_date, '%Y-%m-%d') as effective_date,
               strftime(detected_at, '%Y-%m-%d') as detected_at
        from intermediate.int_fca_status_changes
        order by detected_at desc, frn limit 25
    """)

    recent_auth = rows(con, """
        select frn, firm_name, town, strftime(status_effective_date, '%Y-%m-%d') as since
        from marts.mart_fca_firms_current
        where status = 'authorised' and status_effective_date is not null
        order by status_effective_date desc, frn limit 15
    """)

    kpi = {
        "firms": one(con, "select count(*) from marts.mart_fca_firms_current"),
        "active": one(con, f"select count(*) from marts.mart_fca_firms_current where status not in ({left})"),
        "authorised": one(con, "select count(*) from marts.mart_fca_firms_current where status = 'authorised'"),
        "changes": one(con, "select count(*) from intermediate.int_fca_status_changes"),
        "versions": one(con, "select count(*) from raw.raw_fca_firms"),
        "latest_extract": str(one(con, "select max(extracted_at) from raw.raw_fca_firms"))[:16],
    }
    con.close()

    sweep = None
    if state_file and state_file.exists():
        sw = json.loads(state_file.read_text()).get("sweep") or {}
        if sw:
            start, end, nxt = 100_000, sw.get("end", 1_050_000), sw.get("next_frn", 100_000)
            sweep = {"next": nxt, "end": end,
                     "pct": round(min(100.0, 100 * (nxt - start) / max(1, end - start)), 1)}

    return {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "kpi": kpi, "sweep": sweep, "status": top, "exits": exits, "towns": towns,
        "types": types, "changes": changes, "recent_auth": recent_auth,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", default=str(PROJECT_ROOT / "fca.duckdb"))
    p.add_argument("--state", default=str(PROJECT_ROOT / "data" / "raw" / "extract_state.json"))
    p.add_argument("--out", default=str(PROJECT_ROOT / "site" / "index.html"))
    a = p.parse_args()

    data = collect(Path(a.db), Path(a.state))
    html = TEMPLATE.read_text().replace("/*__DATA__*/null", json.dumps(data, default=str))
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html)
    print(f"Dashboard written to {out} ({data['kpi']['firms']:,} firms)")


if __name__ == "__main__":
    main()
