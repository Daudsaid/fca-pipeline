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
RELEASE_URL = f"https://github.com/{REPO}/releases/download/warehouse/fca.duckdb"
PROJECT_ROOT = Path(__file__).resolve().parents[1]

BLUE = "#2a78d6"  # categorical slot 1 (validated palette)
LEFT_REGISTER = (
    "no longer authorised", "revoked", "cancelled - authorised emi", "cancelled - small emi",
    "no longer registered as an appointed representative",
)

st.set_page_config(page_title="FCA Register explorer", page_icon="🏦", layout="wide")


# --------------------------------------------------------------------------- data
@st.cache_resource(ttl=3600, show_spinner="Loading the latest warehouse…")
def warehouse() -> tuple[str, str]:
    """Return (path to a DuckDB file, description of where it came from)."""
    env = os.getenv("FCA_DUCKDB_PATH")
    if env and Path(env).exists():
        return env, f"local file {env}"
    local = PROJECT_ROOT / "fca.duckdb"
    if local.exists():
        return str(local), "local fca.duckdb"
    dest = Path(tempfile.gettempdir()) / f"fca_{int(time.time())}.duckdb"
    with requests.get(RELEASE_URL, stream=True, timeout=120) as r:
        r.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in r.iter_content(1 << 20):
                fh.write(chunk)
    return str(dest), "GitHub Release (updated every 6 hours)"


def q(sql: str, params: list | None = None) -> pd.DataFrame:
    path, _ = warehouse()
    con = duckdb.connect(path, read_only=True)
    try:
        return con.execute(sql, params or []).df()
    finally:
        con.close()


@st.cache_data(ttl=3600)
def firms() -> pd.DataFrame:
    df = q("""
        select frn, firm_name, coalesce(status, 'unknown') as status, status_effective_date,
               coalesce(business_type, 'Unknown') as business_type, town, postcode,
               companies_house_number, website, current_since
        from marts.mart_fca_firms_current
    """)
    df["town"] = df["town"].str.strip().str.title()
    df["status_effective_date"] = pd.to_datetime(df["status_effective_date"]).dt.date
    df["status_label"] = df["status"].str.capitalize()
    df["on_register"] = ~df["status"].isin(LEFT_REGISTER)
    return df


@st.cache_data(ttl=3600)
def changes() -> pd.DataFrame:
    return q("""
        select frn, firm_name, status_before, status_after, effective_date, detected_at
        from intermediate.int_fca_status_changes order by detected_at desc
    """)


@st.cache_data(ttl=3600)
def meta() -> dict:
    return q("""
        select (select count(*) from raw.raw_fca_firms) as versions,
               (select max(extracted_at) from raw.raw_fca_firms) as latest
    """).iloc[0].to_dict()


def history(frn: str) -> pd.DataFrame:
    return q("""
        select status, firm_name, business_type, postcode, status_effective_date,
               dbt_valid_from as valid_from, dbt_valid_to as valid_to
        from snapshots.snap_fca_firms where frn = ? order by dbt_valid_from
    """, [frn])


def hbar(df: pd.DataFrame, label: str, value: str, title: str, height: int | None = None):
    chart = (
        alt.Chart(df)
        .mark_bar(color=BLUE, cornerRadiusTopRight=4, cornerRadiusBottomRight=4, height=16)
        .encode(
            y=alt.Y(f"{label}:N", sort="-x", title=None, axis=alt.Axis(labelLimit=260, labelOverlap=False)),
            x=alt.X(f"{value}:Q", title="Firms", axis=alt.Axis(grid=True, tickCount=5)),
            tooltip=[alt.Tooltip(f"{label}:N", title=title), alt.Tooltip(f"{value}:Q", title="Firms", format=",")],
        )
        .properties(height=height or max(120, 32 * len(df)))
    )
    text = chart.mark_text(align="left", dx=4, color="#898781").encode(text=alt.Text(f"{value}:Q", format=","))
    return (chart + text).configure_view(strokeWidth=0)


# --------------------------------------------------------------------------- page
st.title("FCA Register explorer")
st.caption(
    "Real data from the UK Financial Conduct Authority's Financial Services Register, collected by an ELT "
    "pipeline (Python → DuckDB → dbt) that runs every 6 hours on GitHub Actions. "
    f"[Source code](https://github.com/{REPO})"
)

try:
    df = firms()
except Exception as exc:  # noqa: BLE001
    st.error(f"Couldn't load the warehouse: {exc}")
    st.stop()

m = meta()
_, source = warehouse()
st.caption(f"Data: {source} · latest extract {str(m['latest'])[:16]} UTC")

# ---- filters (one row, above everything they affect)
f1, f2, f3 = st.columns([2, 2, 2])
with f1:
    scope = st.radio("Firms", ["All", "On the Register", "Left the Register"], horizontal=True)
with f2:
    types = st.multiselect("Business type", sorted(df["business_type"].unique()), placeholder="All types")
with f3:
    towns = st.multiselect("Town", df["town"].dropna().value_counts().index[:200], placeholder="All towns")

view = df
if scope == "On the Register":
    view = view[view["on_register"]]
elif scope == "Left the Register":
    view = view[~view["on_register"]]
if types:
    view = view[view["business_type"].isin(types)]
if towns:
    view = view[view["town"].isin(towns)]

# ---- KPIs
k1, k2, k3, k4 = st.columns(4)
k1.metric("Firms", f"{len(view):,}")
k2.metric("On the Register", f"{int(view['on_register'].sum()):,}")
k3.metric("Fully authorised", f"{int((view['status'] == 'authorised').sum()):,}")
k4.metric("Status changes detected", f"{len(changes()):,}")

tab_overview, tab_search, tab_changes = st.tabs(["Overview", "Find a firm", "Status changes"])

with tab_overview:
    if view.empty:
        st.info("No firms match these filters.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Firms by current status")
            s = view["status_label"].value_counts().rename_axis("status").reset_index(name="firms")
            if len(s) > 8:
                s = pd.concat([s.head(7), pd.DataFrame([{"status": "Other", "firms": s["firms"][7:].sum()}])])
            st.altair_chart(hbar(s, "status", "firms", "Status"), width="stretch")
        with c2:
            st.subheader("Top towns")
            t = view["town"].dropna().value_counts().head(10).rename_axis("town").reset_index(name="firms")
            st.altair_chart(hbar(t, "town", "firms", "Town"), width="stretch")

        st.subheader("Firms leaving the Register, by year")
        st.caption("Firms whose current status is no longer authorised, revoked, cancelled or no longer "
                   "registered, by the year the FCA says that status took effect.")
        ex = view[(~view["on_register"]) & view["status_effective_date"].notna()].copy()
        ex["year"] = pd.to_datetime(ex["status_effective_date"]).dt.year
        ex = ex[(ex["year"] >= 2002) & (ex["year"] <= pd.Timestamp.now().year)]
        by_year = ex.groupby("year").size().reset_index(name="firms")
        if by_year.empty:
            st.info("No exits for these filters.")
        else:
            st.altair_chart(
                alt.Chart(by_year)
                .mark_bar(color=BLUE, cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("year:O", title=None, axis=alt.Axis(labelAngle=0, labelOverlap=True)),
                    y=alt.Y("firms:Q", title="Firms"),
                    tooltip=[alt.Tooltip("year:O", title="Year"), alt.Tooltip("firms:Q", title="Firms left", format=",")],
                )
                .properties(height=260)
                .configure_view(strokeWidth=0),
                width="stretch",
            )

with tab_search:
    query = st.text_input("Search by firm name or FRN", placeholder="e.g. Barclays, Monzo, 122702")
    hits = view
    if query:
        qn = query.strip().lower()
        hits = view[view["firm_name"].str.lower().str.contains(qn, na=False, regex=False)
                    | (view["frn"] == qn)]
    st.caption(f"{len(hits):,} firms")
    table = hits[["frn", "firm_name", "status_label", "business_type", "town", "postcode",
                  "status_effective_date"]].rename(columns={
        "frn": "FRN", "firm_name": "Firm", "status_label": "Status", "business_type": "Type",
        "town": "Town", "postcode": "Postcode", "status_effective_date": "Status since"})
    picked = st.dataframe(table.head(5000), hide_index=True, width="stretch",
                          on_select="rerun", selection_mode="single-row")
    rows = picked.selection.rows if picked and picked.selection else []
    if rows:
        frn = table.head(5000).iloc[rows[0]]["FRN"]
        firm = df[df["frn"] == frn].iloc[0]
        st.markdown(f"#### {firm['firm_name']}  ·  FRN {frn}")
        d1, d2, d3 = st.columns(3)
        d1.metric("Status", firm["status_label"])
        d2.metric("Type", firm["business_type"])
        d3.metric("Town", firm["town"] or "Unknown")
        links = [f"[FCA Register](https://register.fca.org.uk/s/search?q={frn}&type=Companies)"]
        if firm["companies_house_number"]:
            links.append(f"[Companies House](https://find-and-update.company-information.service.gov.uk/"
                         f"company/{firm['companies_house_number']})")
        if firm["website"]:
            links.append(f"[Website]({firm['website']})")
        st.markdown(" · ".join(links))
        st.caption("History tracked by the pipeline (dbt SCD Type 2 snapshot):")
        st.dataframe(history(frn), hide_index=True, width="stretch")
    else:
        st.caption("Select a row to see the firm's details and tracked history.")

with tab_changes:
    ch = changes()
    if ch.empty:
        st.info("No status changes detected yet. They appear once firms have been checked on more than one run.")
    else:
        st.dataframe(ch.rename(columns={
            "frn": "FRN", "firm_name": "Firm", "status_before": "From", "status_after": "To",
            "effective_date": "Effective", "detected_at": "Detected"}), hide_index=True, width="stretch")

st.caption("Source: FCA Financial Services Register API. Coverage grows as the pipeline sweeps firm reference "
           "numbers. Always check a firm at register.fca.org.uk before relying on it.")
