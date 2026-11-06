import json

import extract_fca
from extract_fca import Extractor, State


def records(raw_dir):
    out = []
    for f in sorted(raw_dir.glob("firms_*.jsonl")):
        out += [json.loads(l) for l in f.read_text().splitlines()]
    return out


def test_sweep_finds_existing_firms_only(fake_api, tmp_path):
    for frn in (100003, 100007, 100011):
        fake_api.add(frn)
    ex = Extractor(tmp_path, None)
    assert ex.sweep(100000, 100015) == 3
    recs = records(tmp_path)
    assert sorted(r["frn"] for r in recs) == ["100003", "100007", "100011"]
    assert all(r["addresses"] for r in recs)


def test_sweep_resumes_from_checkpoint(fake_api, tmp_path):
    for frn in range(100000, 100020, 2):
        fake_api.add(frn)
    Extractor(tmp_path, None).sweep(100000, 100019, limit=3)
    position = State(tmp_path).sweep["next_frn"]
    assert 100000 < position < 100019

    Extractor(tmp_path, None).sweep(None, None)     # resume
    frns = [r["frn"] for r in records(tmp_path)]
    assert len(frns) == len(set(frns)) == 10        # nothing missed, nothing twice


def test_outage_keeps_checkpoint(fake_api, tmp_path):
    fake_api.add(100005)
    ex = Extractor(tmp_path, None)
    ex.sweep(100000, 100002)
    fake_api.mode = "down"
    try:
        Extractor(tmp_path, None).sweep(None, 100010)
    except extract_fca.FCAUnavailable:
        pass
    # position must not have skipped past the FRN that failed
    assert State(tmp_path).sweep["next_frn"] == 100003


def test_refresh_is_oldest_first_and_marks_misses(fake_api, tmp_path):
    fake_api.add(1)
    fake_api.add(2)
    st = State(tmp_path)
    st.firms = {"1": "2026-01-02T00:00:00", "2": "2026-01-01T00:00:00", "3": "2026-01-03T00:00:00"}
    st.save()

    ex = Extractor(tmp_path, None)
    ex.refresh(limit=1)
    assert [r["frn"] for r in records(tmp_path)] == ["2"]   # oldest first

    ex.refresh(limit=3)
    after = State(tmp_path).firms
    assert after["3"] > "2026-01-03"                          # missing firm rotated to the back


def test_frontier_finds_new_firms_above_highest(fake_api, tmp_path):
    st = State(tmp_path)
    st.firms = {"500000": "2026-01-01T00:00:00"}
    st.save()
    fake_api.add(500000)
    fake_api.add(500400)       # newly registered, above the highest known
    fake_api.add(499000)       # allocated earlier, appeared late

    monkey_above, monkey_below = extract_fca.FRONTIER_ABOVE, extract_fca.FRONTIER_BELOW
    extract_fca.FRONTIER_ABOVE, extract_fca.FRONTIER_BELOW = 500, 1500
    try:
        ex = Extractor(tmp_path, None)
        assert ex.frontier_scan() == 2
        assert ex.frontier_scan() == 0                         # once a day
        assert State(tmp_path).frontier["highest"] == 500400
    finally:
        extract_fca.FRONTIER_ABOVE, extract_fca.FRONTIER_BELOW = monkey_above, monkey_below


def test_state_seeds_from_existing_files(tmp_path):
    (tmp_path / "firms_2026-08-11.jsonl").write_text(
        json.dumps({"frn": "123", "extracted_at": "2026-08-11T00:00:00+00:00", "firm": {}, "addresses": []}) + "\n"
    )
    assert "123" in State(tmp_path).firms


def test_time_limit_stops_cleanly(fake_api, tmp_path):
    ex = Extractor(tmp_path, max_minutes=0.0001)
    import time
    time.sleep(0.05)
    assert ex.sweep(100000, 200000) == 0
    assert State(tmp_path).sweep["next_frn"] == 100000
