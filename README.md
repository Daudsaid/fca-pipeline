# FCA Register Pipeline

An **ELT pipeline** on real data from the UK Financial Conduct Authority's
Financial Services Register. It tracks how regulated firms' authorisation status
changes over time using **dbt SCD Type 2 snapshots**, stored in a single-file
**DuckDB** warehouse and refreshed daily by **GitHub Actions**.

No Docker, no servers, no Airflow: `pip install` and one command.

```
FCA Register API ──► extract_fca.py ──► data/raw/firms_<date>.jsonl
                                              │  (Extract)
                                              ▼
                     fca.duckdb  raw.raw_fca_firms          (Load: JSON landed untouched)
                                              │
            dbt:  stg_fca_firms ──► snap_fca_firms (SCD2) ──► int_fca_status_changes ──► marts
                                              (Transform + tests)
```

## Stack
| Layer | Tool |
|---|---|
| Source | FCA Financial Services Register API (free tier) |
| Extract | Python + `requests` (rate-limited, retrying, resumable) |
| Load + warehouse | DuckDB (`fca.duckdb`, one file) |
| Transform + tests | dbt (`dbt-duckdb`) |
| Orchestration | `run_pipeline.sh` + GitHub Actions daily cron |

## dbt models
| Layer | Model | What it is |
|---|---|---|
| Raw | `raw.raw_fca_firms` | Every distinct version of every firm, API JSON as returned (change-only, incremental) |
| Staging | `staging.stg_fca_firms` | Latest record per firm, JSON parsed and typed, principal address picked |
| Snapshot | `snapshots.snap_fca_firms` | SCD2 history: new row when name, status, sub-status, business type or postcode changes |
| Intermediate | `intermediate.int_fca_status_changes` | Every status transition, with FCA effective date and date detected |
| Mart | `marts.mart_fca_firms_current` | Current state of every tracked firm |
| Mart | `marts.mart_fca_status_summary` | Firms by current status, with % share |
| Mart | `marts.mart_fca_status_changes_monthly` | Monthly trend of transitions |

19 data tests, including custom tests that snapshot periods never overlap and
each firm has exactly one current row.

## Quickstart
```bash
uv venv --python 3.13 .venv && source .venv/bin/activate
uv pip install -r requirements.txt        # (plain venv instead: python3 -m venv .venv && pip install -r requirements.txt)
cp .env.example .env                      # add FCA_API_EMAIL / FCA_API_KEY

bash run_pipeline.sh --skip-extract       # build fca.duckdb from the seed data (no API calls)
bash run_pipeline.sh --limit 50           # refresh 50 firms from the live API, then dbt build
```
Explore the results:
```bash
duckdb fca.duckdb   # or python: duckdb.connect("fca.duckdb")
```
```sql
select * from marts.mart_fca_status_summary;
select * from intermediate.int_fca_status_changes order by effective_date desc;
select frn, firm_name, status, dbt_valid_from, dbt_valid_to
from snapshots.snap_fca_firms where frn = '100013' order by dbt_valid_from;
```

## Getting as much data as possible
The free API has no "list all firms" endpoint and returns one firm per call, so
the extractor has resumable modes (`python extract/extract_fca.py --help`):

| Mode | What it does |
|---|---|
| `sweep` | Tries every FRN from 100,000 to 1,050,000; fetches the ones that exist. Checkpointed, so it resumes where it stopped. **This is how you get (nearly) the whole Register.** |
| `refresh` | Re-fetches known firms, least recently checked first, so every firm is revisited in rotation and status changes are caught |
| `search` | Finds firms through the Search endpoint: quick, but only what the search terms match |
| `auto` | *(default)* `refresh` up to `--limit` firms, then continue the `sweep` until `--max-minutes` |
| `status` | Shows progress (known firms, sweep %, oldest refresh). No API calls |

**Backfill.** At the default pace (one call every 0.5 s) the full sweep is ~950k FRN
checks, so roughly **6 days of running time**. It doesn't have to be continuous:
```bash
caffeinate -i bash run_pipeline.sh sweep --max-minutes 600   # 10 h on a Mac without sleeping
python extract/extract_fca.py status                          # check progress any time
```
Stop it any time (Ctrl+C); it saves its position and the next run carries on.
Alternatively let GitHub Actions do it: each daily run spends its spare time
(~4 h) on the sweep, finishing the backfill in roughly 5 weeks.

**Run only one copy at a time** (laptop *or* GitHub Actions): they share one API key
and one state file.

## Daily schedule (GitHub Actions)
`.github/workflows/daily_pipeline.yml` runs every day at 01:17 UTC:
1. downloads `fca.duckdb` + `extract_state.json` from the repo's **`warehouse` release**
2. refreshes the 3,000 least-recently-checked firms, then continues the sweep (max 4.5 h)
3. `dbt build`, then uploads the updated files back to the release

The warehouse is kept in a release, not in git, because at full size it is larger
than git's 100 MB file limit (roughly 45 MB per 100k firm versions).

Setup, once:
1. Push the repo to GitHub.
2. **Settings → Secrets and variables → Actions**: add secrets `FCA_API_EMAIL` and
   `FCA_API_KEY` (optional variable `FCA_FIRM_LIMIT`).
3. If you've already built data locally, upload it so CI continues from there:
   `gh release create warehouse fca.duckdb data/raw/extract_state.json --title "Warehouse (fca.duckdb)"`
4. **Actions** tab → *Daily FCA pipeline* → **Run workflow** to test it.

To work locally again later, download the latest first:
`gh release download warehouse -p fca.duckdb && gh release download warehouse -p extract_state.json -D data/raw --clobber`

## About the data
- `data/raw/firms_2026-08-11.jsonl` is a seed of 269 real firms pulled in August 2026.
- Every fetched firm is written to `data/raw/firms_<date>.jsonl` (firm + addresses),
  then loaded by dbt; the JSONL files can be deleted once loaded.
- Statuses are the FCA's own, e.g. *Authorised*, *No longer authorised*,
  *Registered*, *Appointed representative - introducer*, *Cancelled - Small EMI*.

## Design notes
- **ELT, raw stays raw.** API JSON is stored untouched; all parsing lives in tested dbt
  SQL, so a new field means editing a model, not re-calling the API.
- **Change-only raw storage.** The raw model stores a new row only when a firm's
  content changed (md5 of the JSON, ignoring the API's own timestamp), and drops the
  API's self-link fields. Re-runs never duplicate; the table holds every distinct
  version of every firm.
- **Resilient extract.** Rate-limited with automatic slow-down on HTTP 429, retries
  with backoff, and an outage never gets recorded as "firm doesn't exist". Progress is
  checkpointed, and Ctrl+C or a CI timeout saves state before stopping.
- **No hard-delete invalidation** in the snapshot: firms are sampled via search, so a
  firm missing from one run hasn't necessarily left the Register.
- **Change dates.** `int_fca_status_changes` keeps both the FCA's effective date and
  when the pipeline detected the change.
- **Backfills:** the snapshot records the latest state per run, so load extracts one
  day at a time if you want each day as its own history step.
