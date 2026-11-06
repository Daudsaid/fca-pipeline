"""
FCA Register explorer: a Streamlit app on top of the pipeline's DuckDB warehouse.

Run locally:   streamlit run app/streamlit_app.py
Deploy:        Streamlit Community Cloud -> this repo -> main file app/streamlit_app.py

Data source, in order:
  1. FCA_DUCKDB_PATH env var, if set
  2. fca.duckdb in the project root (after running the pipeline locally)
  3. the latest fca.duckdb from the repo's "warehouse" GitHub Release
     (refreshed every 6 hours by the pipeline; re-downloaded here hourly)
"""
from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import requests
import streamlit as st

REPO = os.getenv("FCA_REPO", "Daudsaid/fca-pipeline")
RELEASE = f"https://github.com/{REPO}/releases/download/warehouse"
PROJECT_ROOT = Path(__file__).resolve().parents[1]

BLUE = "#2a78d6"   # categorical slot 1 of the validated palette
MUTED = "#898781"

LEFT_REGISTER = {
    "no longer authorised", "revoked", "cancelled - authorised emi", "cancelled - small emi",
    "no longer registered as an appointed representative",
}

# Status badges: colour always comes with an icon + the status text, never colour alone.
def status_badge(status: str) -> tuple[str, str, str]:
    s = (status or "").lower()
    if s in ("revoked",) or s.startswith("cancelled"):
        return "red", ":material/block:", "Left the Register"
    if s.startswith("no longer"):
        return "gray", ":material/history:", "Left the Register"
    if "applied to cancel" in s or s == "see full details":
        return "orange", ":material/warning:", "Check details"
    if s in ("authorised", "registered", "eea authorised") or s.startswith("appointed representative"):
        return "green", ":material/check_circle:", "On the Register"
    return "gray", ":material/help:", "Other"


st.set_page_config(page_title="FCA Register explorer", page_icon=":material/account_balance:", layout="wide")

st.html("""
<style>
  .block-container { padding-top: 2.2rem; padding-bottom: 3rem; max-width: 1240px; }
  [data-testid="stMetricValue"] { font-size: 1.9rem; }
  [data-testid="stMetricLabel"] p { font-size: 0.85rem; }
  .hero-sub { color: var(--text-color, #52514e); opacity: .8; margin: -0.6rem 0 0.4rem; font-size: 1.02rem; }
  .muted { color: #898781; font-size: 0.85rem; }
  .timeline { border-left: 2px solid #c3c2b7; margin: 0.4rem 0 0 0.4rem; padding-left: 1rem; }
  .timeline .item { margin: 0 0 0.9rem; position: relative; }
  .timeline .item::before { content: ""; position: absolute; left: -1.4rem; top: 0.35rem; width: 10px; height: 10px;
                            border-radius: 50%; background: #2a78d6; border: 2px solid var(--background-color, #fff); }
  .timeline .when { color: #898781; font-size: 0.8rem; }
</style>
""")


# --------------------------------------------------------------------------- data
def _download(name: str) -> Path:
    dest = Path(tempfile.gettempdir()) / f"{int(time.time())}_{name}"
    with requests.get(f"{RELEASE}/{name}", stream=True, timeout=120) as r:
        r.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in r.iter_content(1 << 20):
                fh.write(chunk)
    return dest


@st.cache_resource(ttl=3600, show_spinner="Loading the latest data…")
def warehouse() -> tuple[str, str]:
    """(path to a DuckDB file, where it came from)."""
    env = os.getenv("FCA_DUCKDB_PATH")
    if env and Path(env).exists():
        return env, "local file"
    local = PROJECT_ROOT / "fca.duckdb"
    if local.exists():
        return str(local), "local fca.duckdb"
    return str(_download("fca.duckdb")), "GitHub Release"


@st.cache_data(ttl=3600, show_spinner=False)
def pipeline_state() -> dict:
    """Sweep progress from the extractor's state file (best effort)."""
    import json
    for p in (PROJECT_ROOT / "data" / "raw" / "extract_state.json",):
        if p.exists():
            return json.loads(p.read_text())
    try:
        return json.loads(_download("extract_state.json").read_text())
    except Exception:  # noqa: BLE001
        return {}


def q(sql: str, params: list | None = None) -> pd.DataFrame:
    path, _ = warehouse()
    con = duckdb.connect(path, read_only=True)
    try:
        return con.execute(sql, params or []).df()
    finally:
        con.close()


@st.cache_data(ttl=3600, show_spinner=False)
def firms() -> pd.DataFrame:
    df = q("""
        select frn, firm_name, coalesce(status, 'unknown') as status, status_effective_date,
               coalesce(business_type, 'Unknown') as business_type, town, postcode,
               companies_house_number, website, current_since
        from marts.mart_fca_firms_current
    """)
    df["town"] = df["town"].str.strip().str.title()
    df["status_label"] = df["status"].str.capitalize()
    df["status_effective_date"] = pd.to_datetime(df["status_effective_date"]).dt.date
    df["on_register"] = ~df["status"].isin(LEFT_REGISTER)
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def changes() -> pd.DataFrame:
    return q("""
        select frn, firm_name, status_before, status_after, effective_date, detected_at
        from intermediate.int_fca_status_changes order by detected_at desc
    """)


@st.cache_data(ttl=3600, show_spinner=False)
def meta() -> dict:
    return q("""
        select (select count(*) from raw.raw_fca_firms) as versions,
               (select max(extracted_at) from raw.raw_fca_firms) as latest,
               (select count(*) from snapshots.snap_fca_firms) as snapshot_rows
    """).iloc[0].to_dict()


def history(frn: str) -> pd.DataFrame:
    return q("""
        select status, firm_name, business_type, postcode, status_effective_date,
               dbt_valid_from as valid_from, dbt_valid_to as valid_to
        from snapshots.snap_fca_firms where frn = ? order by dbt_valid_from
    """, [frn])


# --------------------------------------------------------------------------- charts
def hbar(df: pd.DataFrame, label: str, value: str, title: str):
    base = alt.Chart(df).encode(
        y=alt.Y(f"{label}:N", sort="-x", title=None, axis=alt.Axis(labelLimit=280, labelOverlap=False)),
        x=alt.X(f"{value}:Q", title=None, axis=alt.Axis(grid=True, tickCount=4, labelColor=MUTED)),
        tooltip=[alt.Tooltip(f"{label}:N", title=title), alt.Tooltip(f"{value}:Q", title="Firms", format=",")],
    )
    bars = base.mark_bar(color=BLUE, cornerRadiusTopRight=4, cornerRadiusBottomRight=4, height=16)
    text = base.mark_text(align="left", dx=5, color=MUTED, fontSize=11).encode(text=alt.Text(f"{value}:Q", format=","))
    return (bars + text).properties(height=max(120, 32 * len(df))).configure_view(strokeWidth=0)


def columns_by_year(df: pd.DataFrame, tip: str):
    return (
        alt.Chart(df)
        .mark_bar(color=BLUE, cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("year:O", title=None, axis=alt.Axis(labelAngle=0, labelOverlap=True, labelColor=MUTED)),
            y=alt.Y("firms:Q", title=None, axis=alt.Axis(labelColor=MUTED, tickCount=5)),
            tooltip=[alt.Tooltip("year:O", title="Year"), alt.Tooltip("firms:Q", title=tip, format=",")],
        )
        .properties(height=260)
        .configure_view(strokeWidth=0)
    )


# --------------------------------------------------------------------------- load
try:
    df = firms()
    m = meta()
except Exception as exc:  # noqa: BLE001
    st.error(f"Couldn't load the warehouse: {exc}")
    st.stop()

_, source = warehouse()
state = pipeline_state()
sweep = state.get("sweep") or {}
sweep_pct = None
if sweep:
    sweep_pct = min(1.0, (sweep.get("next_frn", 100_000) - 100_000) / max(1, sweep.get("end", 1_050_000) - 100_000))

# --------------------------------------------------------------------------- header
head_l, head_r = st.columns([3, 1.2], vertical_alignment="bottom")
with head_l:
    st.title("FCA Register explorer")
    st.html('<p class="hero-sub">Every UK financial firm the pipeline has collected from the FCA Financial '
            'Services Register, with how their status changes over time.</p>')
    st.markdown(f":blue-badge[:material/schedule: Updated {str(m['latest'])[:16]} UTC] "
                f":gray-badge[:material/database: {source}] "
                f":gray-badge[:material/sync: Refreshed every 6 hours]")
with head_r:
    if sweep_pct is not None:
        st.caption(f"Backfill progress · FRN {sweep.get('next_frn', 0):,} of {sweep.get('end', 0):,}")
        st.progress(sweep_pct, text=f"{sweep_pct:.1%} of the Register swept")

# --------------------------------------------------------------------------- filters
with st.container(border=True):
    f1, f2, f3, f4 = st.columns([1.6, 1.6, 1.6, 1.4])
    with f1:
        scope = st.segmented_control("Register", ["All", "Active", "Left"], default="All", key="scope",
                                     help="Active = authorised, registered or appointed representative today")
    with f2:
        types = st.multiselect("Business type", sorted(df["business_type"].unique()), placeholder="All types")
    with f3:
        towns = st.multiselect("Town", df["town"].dropna().value_counts().index[:300].tolist(), placeholder="All towns")
    with f4:
        statuses = st.multiselect("Status", df["status_label"].value_counts().index.tolist(), placeholder="All statuses")

view = df
if scope == "Active":
    view = view[view["on_register"]]
elif scope == "Left":
    view = view[~view["on_register"]]
if types:
    view = view[view["business_type"].isin(types)]
if towns:
    view = view[view["town"].isin(towns)]
if statuses:
    view = view[view["status_label"].isin(statuses)]

# --------------------------------------------------------------------------- KPIs
k1, k2, k3, k4 = st.columns(4)
k1.metric("Firms", f"{len(view):,}", border=True,
          help="Firms collected so far that match the filters")
k2.metric("On the Register", f"{int(view['on_register'].sum()):,}", border=True,
          help="Authorised, registered or appointed representatives today")
k3.metric("Fully authorised", f"{int((view['status'] == 'authorised').sum()):,}", border=True)
k4.metric("Status changes detected", f"{len(changes()):,}", border=True,
          help="Changes caught between pipeline runs by the dbt SCD Type 2 snapshot")

tab_overview, tab_search, tab_changes, tab_about = st.tabs(
    [":material/insights: Overview", ":material/search: Find a firm",
     ":material/swap_horiz: Status changes", ":material/info: About the pipeline"]
)

# --------------------------------------------------------------------------- overview
with tab_overview:
    if view.empty:
        st.info("No firms match these filters.", icon=":material/filter_alt_off:")
    else:
        c1, c2 = st.columns(2, gap="large")
        with c1, st.container(border=True):
            st.markdown("**Firms by current status**")
            s = view["status_label"].value_counts().rename_axis("status").reset_index(name="firms")
            if len(s) > 8:
                s = pd.concat([s.head(7), pd.DataFrame([{"status": "Other", "firms": s["firms"][7:].sum()}])])
            st.altair_chart(hbar(s, "status", "firms", "Status"), width="stretch")
        with c2, st.container(border=True):
            st.markdown("**Top 10 towns**")
            t = view["town"].dropna().value_counts().head(10).rename_axis("town").reset_index(name="firms")
            if t.empty:
                st.caption("No town data for these filters.")
            else:
                st.altair_chart(hbar(t, "town", "firms", "Town"), width="stretch")

        c3, c4 = st.columns(2, gap="large")
        with c3, st.container(border=True):
            st.markdown("**Firms by business type**")
            bt = view["business_type"].value_counts().head(8).rename_axis("type").reset_index(name="firms")
            st.altair_chart(hbar(bt, "type", "firms", "Business type"), width="stretch")
        with c4, st.container(border=True):
            st.markdown("**Firms leaving the Register, by year**")
            st.caption("By the year the FCA says the firm's current status took effect.")
            ex = view[(~view["on_register"]) & view["status_effective_date"].notna()].copy()
            ex["year"] = pd.to_datetime(ex["status_effective_date"]).dt.year
            ex = ex[(ex["year"] >= 2002) & (ex["year"] <= pd.Timestamp.now().year)]
            by_year = ex.groupby("year").size().reset_index(name="firms")
            if by_year.empty:
                st.caption("No exits for these filters.")
            else:
                st.altair_chart(columns_by_year(by_year, "Firms left"), width="stretch")

# --------------------------------------------------------------------------- search
with tab_search:
    s1, s2 = st.columns([4, 1], vertical_alignment="bottom")
    with s1:
        query = st.text_input("Search by firm name or FRN", placeholder="e.g. Barclays, Monzo, 122702",
                              label_visibility="collapsed")
    hits = view
    if query:
        qn = query.strip().lower()
        hits = view[view["firm_name"].str.lower().str.contains(qn, na=False, regex=False) | (view["frn"] == qn)]
    table = hits[["frn", "firm_name", "status_label", "business_type", "town", "postcode",
                  "status_effective_date"]].rename(columns={
        "frn": "FRN", "firm_name": "Firm", "status_label": "Status", "business_type": "Type",
        "town": "Town", "postcode": "Postcode", "status_effective_date": "Status since"})
    with s2:
        st.download_button(":material/download: CSV", table.to_csv(index=False).encode(),
                           file_name="fca_firms.csv", mime="text/csv", width="stretch")
    st.caption(f"{len(hits):,} firms · select a row to open its profile"
               + (" · showing the first 5,000" if len(hits) > 5000 else ""))
    shown = table.head(5000)
    picked = st.dataframe(shown, hide_index=True, width="stretch",
                          on_select="rerun", selection_mode="single-row")
    rows = picked.selection.rows if picked and picked.selection else []

    if rows:
        frn = shown.iloc[rows[0]]["FRN"]
        firm = df[df["frn"] == frn].iloc[0]
        color, icon, group = status_badge(firm["status"])
        with st.container(border=True):
            p1, p2 = st.columns([3, 2], gap="large")
            with p1:
                st.subheader(firm["firm_name"])
                st.markdown(f":{color}-badge[{icon} {firm['status_label']}] "
                            f":gray-badge[{group}] :gray-badge[FRN {frn}]")
                facts = {
                    "Business type": firm["business_type"],
                    "Status since": firm["status_effective_date"] or "Unknown",
                    "Town": firm["town"] or "Unknown",
                    "Postcode": firm["postcode"] or "Unknown",
                    "Companies House no.": firm["companies_house_number"] or "n/a",
                }
                st.markdown("\n".join(f"- **{k}:** {v}" for k, v in facts.items()))
                links = [f"[FCA Register](https://register.fca.org.uk/s/search?q={frn}&type=Companies)"]
                if firm["companies_house_number"]:
                    links.append("[Companies House](https://find-and-update.company-information.service.gov.uk/"
                                 f"company/{firm['companies_house_number']})")
                if firm["website"]:
                    links.append(f"[Website]({firm['website']})")
                st.markdown(" · ".join(links))
            with p2:
                st.markdown("**Tracked history**")
                h = history(frn)
                items = []
                for _, r in h.iterrows():
                    start = str(r["valid_from"])[:10]
                    end = "now" if pd.isna(r["valid_to"]) else str(r["valid_to"])[:10]
                    items.append(f'<div class="item"><div class="when">{start} → {end}</div>'
                                 f'<div>{str(r["status"]).capitalize()}</div></div>')
                st.html('<div class="timeline">' + "".join(items) + "</div>")
                if len(h) <= 1:
                    st.caption("One version so far. A new entry appears here when the pipeline sees this "
                               "firm's status, name, type or postcode change.")

# --------------------------------------------------------------------------- changes
with tab_changes:
    ch = changes()
    if ch.empty:
        st.info("No status changes detected yet. They appear once firms have been re-checked on a later run "
                "and something about them has changed.", icon=":material/hourglass_empty:")
    else:
        st.dataframe(ch.rename(columns={
            "frn": "FRN", "firm_name": "Firm", "status_before": "From", "status_after": "To",
            "effective_date": "Effective (FCA)", "detected_at": "Detected"}), hide_index=True, width="stretch")

# --------------------------------------------------------------------------- about
with tab_about:
    st.markdown("**How the data gets here**")
    st.graphviz_chart("""
            digraph {
              rankdir=LR; bgcolor="transparent";
              node [shape=box, style="rounded,filled", fillcolor="#cde2fb", color="#2a78d6",
                    fontname="Helvetica", fontsize=24, margin="0.2,0.1"];
              edge [color="#898781", arrowsize=1.0, penwidth=1.5];
              api   [label="FCA Register API"];
              ext   [label="Python extract\\nsweep · refresh · frontier"];
              raw   [label="DuckDB raw\\n(JSON, change-only)"];
              stg   [label="dbt staging"];
              snap  [label="dbt snapshot\\nSCD Type 2"];
              marts [label="dbt marts\\n+ 19 tests"];
              app   [label="This app\\n+ dashboard", fillcolor="#f9f9f7"];
              api -> ext -> raw -> stg -> snap -> marts -> app;
              gha [label="GitHub Actions\\nevery 6 hours", shape=note, fillcolor="#f9f9f7", color="#c3c2b7"];
              gha -> ext [style=dashed];
            }
        """, width="stretch")
    a1, a2 = st.columns([3, 2], gap="large")
    with a1:
        st.markdown(
            "- **Extract:** a resumable Python extractor calls the FCA's free API (one firm per call). It "
            "*sweeps* every firm reference number, *refreshes* known firms oldest-first, and scans the newest "
            "numbers daily for newly registered firms.\n"
            "- **Load:** raw JSON lands in DuckDB untouched; a new row is stored only when a firm changed.\n"
            "- **Transform:** dbt parses and types the JSON, keeps full history with an SCD Type 2 snapshot, "
            "and builds the marts this app reads. 19 data tests run on every build.\n"
            "- **Orchestrate:** GitHub Actions runs it every 6 hours and stores the warehouse in a GitHub Release."
        )
    with a2:
        st.markdown("**Pipeline stats**")
        st.metric("Stored firm versions", f"{int(m['versions']):,}", border=True)
        st.metric("Snapshot (history) rows", f"{int(m['snapshot_rows']):,}", border=True)
        if sweep:
            st.metric("Firms found by the sweep", f"{sweep.get('found', 0):,}", border=True)
        st.markdown(f"[Source code](https://github.com/{REPO}) · "
                    f"[Static dashboard](https://{REPO.split('/')[0].lower()}.github.io/{REPO.split('/')[1]}/) · "
                    f"[dbt docs](https://{REPO.split('/')[0].lower()}.github.io/{REPO.split('/')[1]}/dbt/)")

st.html('<p class="muted">Source: FCA Financial Services Register API. Coverage grows as the pipeline sweeps '
        'firm reference numbers. Always check a firm at register.fca.org.uk before relying on it.</p>')
