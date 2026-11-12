import json
import time

import pytest

import extract_fca
from extract_fca import Extractor, State


def records(raw_dir):
    out = []
    for f in sorted(raw_dir.glob("firms_*.jsonl")):
        out += [json.loads(l) for l in f.read_text().splitlines()]
    return out


@pytest.fixture
def small_chunks(monkeypatch):
    monkeypatch.setattr(extract_fca, "CHUNK", 4)


def ex(tmp_path, workers=4, max_minutes=None):
    return Extractor(tmp_path, max_minutes, workers=workers)


def test_sweep_finds_existing_firms_only(fake_api, tmp_path):
    for frn in (100003, 100007, 100011):
        fake_api.add(frn)
    assert ex(tmp_path).sweep(100000, 100015) == 3
    recs = records(tmp_path)
    assert sorted(r["frn"] for r in recs) == ["100003", "100007", "100011"]
    assert all(r["addresses"] for r in recs)


def test_parallel_sweep_is_complete_and_has_no_duplicates(fake_api, tmp_path):
    expected = {str(f) for f in range(100000, 100600) if f % 3 == 0}
    for f in expected:
        fake_api.add(f)
    found = ex(tmp_path, workers=8).sweep(100000, 100599)
    frns = [r["frn"] for r in records(tmp_path)]
    assert found == len(expected)
    assert set(frns) == expected and len(frns) == len(set(frns))


def test_sweep_resumes_from_checkpoint(fake_api, tmp_path, small_chunks):
    for frn in range(100000, 100020, 2):
        fake_api.add(frn)
    ex(tmp_path).sweep(100000, 100019, limit=3)
    position = State(tmp_path).sweep["next_frn"]
    assert 100000 < position < 100019

    ex(tmp_path).sweep(None, None)  # resume
    frns = [r["frn"] for r in records(tmp_path)]
    assert len(frns) == len(set(frns)) == 10  # nothing missed, nothing twice


def test_outage_keeps_checkpoint_at_last_complete_chunk(fake_api, tmp_path, small_chunks):
    fake_api.add(100005)
    ex(tmp_path).sweep(100000, 100003)  # one full chunk
    assert State(tmp_path).sweep["next_frn"] == 100004
    fake_api.mode = "down"
    with pytest.raises(extract_fca.FCAUnavailable):
        ex(tmp_path).sweep(None, 100020)
    # the chunk that failed must be retried next time, not skipped
    assert State(tmp_path).sweep["next_frn"] == 100004
    fake_api.mode = "ok"
    ex(tmp_path).sweep(None, None)
    assert "100005" in {r["frn"] for r in records(tmp_path)}


def test_refresh_is_oldest_first_and_marks_misses(fake_api, tmp_path):
    fake_api.add(1)
    fake_api.add(2)
    st = State(tmp_path)
    st.firms = {"1": "2026-01-02T00:00:00", "2": "2026-01-01T00:00:00", "3": "2026-01-03T00:00:00"}
    st.save()

    e = ex(tmp_path)
    e.refresh(limit=1)
    assert [r["frn"] for r in records(tmp_path)] == ["2"]  # oldest first

    e.refresh(limit=3)
    assert State(tmp_path).firms["3"] > "2026-01-03"       # missing firm rotated to the back


def test_frontier_catches_up_then_runs_once_a_day(fake_api, tmp_path, monkeypatch):
    monkeypatch.setattr(extract_fca, "FRONTIER_ABOVE", 500)
    monkeypatch.setattr(extract_fca, "FRONTIER_BELOW", 1500)
    st = State(tmp_path)
    st.firms = {"500000": "2026-01-01T00:00:00"}
    st.save()
    fake_api.add(500000)
    fake_api.add(500400)  # newly registered, above the highest known
    fake_api.add(499000)  # allocated earlier, appeared late
    fake_api.add(500850)  # beyond the first window: found because 500400 extends the scan

    e = ex(tmp_path)
    assert e.frontier_scan() == 3
    fr = State(tmp_path).frontier
    assert fr["caught_up"] and fr["highest"] == 500850
    assert e.frontier_scan() == 0  # already checked today


def test_frontier_respects_budget_and_resumes(fake_api, tmp_path, monkeypatch):
    monkeypatch.setattr(extract_fca, "CHUNK", 10)
    monkeypatch.setattr(extract_fca, "FRONTIER_ABOVE", 400)
    monkeypatch.setattr(extract_fca, "FRONTIER_BELOW", 0)
    monkeypatch.setenv("FCA_MIN_INTERVAL", "0.002")
    st = State(tmp_path)
    st.firms = {"700000": "2026-01-01T00:00:00"}
    st.save()
    fake_api.add(700390)

    ex(tmp_path).frontier_scan(budget_minutes=0.002)  # ~0.1 s: pauses part-way
    fr = State(tmp_path).frontier
    assert not fr["caught_up"] and 700000 < fr["next_frn"] < 700400

    ex(tmp_path).frontier_scan()  # resumes from its own checkpoint
    fr = State(tmp_path).frontier
    assert fr["caught_up"] and "700390" in {r["frn"] for r in records(tmp_path)}


def test_checkpoint_command_runs_during_long_extracts(fake_api, tmp_path, monkeypatch, small_chunks):
    marker = tmp_path / "checkpoints.txt"
    monkeypatch.setenv("FCA_CHECKPOINT_CMD", f"echo saved >> {marker}")
    monkeypatch.setenv("FCA_CHECKPOINT_MINUTES", "0")
    fake_api.add(100001)
    e = ex(tmp_path)
    e.sweep(100000, 100011)  # three chunks -> three checkpoints
    assert e.checkpoints == 3
    assert marker.read_text().count("saved") == 3
    assert (tmp_path / "extract_state.json").exists()


def test_auto_gives_frontier_a_share_and_continues_the_sweep(fake_api, tmp_path, monkeypatch):
    monkeypatch.setattr(extract_fca, "FRONTIER_ABOVE", 50)
    monkeypatch.setattr(extract_fca, "FRONTIER_BELOW", 0)
    st = State(tmp_path)
    st.firms = {"900000": "2026-01-01T00:00:00"}
    st.sweep = {"next_frn": 100000, "end": 100099, "found": 0}
    st.save()
    fake_api.add(900000)
    fake_api.add(900020)
    fake_api.add(100050)

    ex(tmp_path, max_minutes=1).auto(limit=10, max_minutes=1, start=None, end=None)
    s = State(tmp_path)
    assert s.frontier["caught_up"]
    assert s.sweep["next_frn"] == 100100  # sweep still ran after the frontier
    assert {"900020", "100050"} <= set(s.firms)


def test_state_seeds_from_existing_files(tmp_path):
    (tmp_path / "firms_2026-08-11.jsonl").write_text(
        json.dumps({"frn": "123", "extracted_at": "2026-08-11T00:00:00+00:00", "firm": {}, "addresses": []}) + "\n"
    )
    assert "123" in State(tmp_path).firms


def test_time_limit_stops_cleanly(fake_api, tmp_path):
    e = ex(tmp_path, max_minutes=0.0001)
    time.sleep(0.05)
    assert e.sweep(100000, 200000) == 0
    assert State(tmp_path).sweep["next_frn"] == 100000
