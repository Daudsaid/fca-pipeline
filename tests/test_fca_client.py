import pytest

from fca_client import FCAClient, FCAUnavailable


def client(**kw):
    import os
    return FCAClient(os.environ["FCA_API_EMAIL"], os.environ["FCA_API_KEY"], **kw)


def test_firm_found_and_addresses(fake_api):
    fake_api.add(122702, name="Test Bank plc")
    c = client()
    assert c.firm("122702")["Organisation Name"] == "Test Bank plc"
    towns = [a["Town"] for a in c.firm_addresses("122702")]
    assert "London" in towns


def test_missing_firm_returns_none(fake_api):
    assert client().firm("999999") is None


def test_outage_raises_instead_of_looking_like_missing_firm(fake_api):
    fake_api.mode = "down"
    with pytest.raises(FCAUnavailable):
        client().firm("122702")


def test_bad_credentials_fail_loudly(fake_api, monkeypatch):
    with pytest.raises(RuntimeError, match="credentials"):
        FCAClient("test@example.com", "wrong-key").firm("1")


def test_slows_down_on_429_and_recovers(fake_api, monkeypatch):
    import time
    import fca_client
    monkeypatch.setattr(fca_client.FCAClient, "_backoff", staticmethod(lambda a, r: time.sleep(0.3)))
    fake_api.add(100000)
    fake_api.mode = "throttle"
    fake_api.max_per_sec = 5
    c = client(min_interval=0.01)
    for _ in range(12):
        c.firm("100000")
    assert c.min_interval > 0.01          # slowed down after 429s

    fake_api.mode = "ok"
    slowed = c.min_interval
    for _ in range(150):
        c.firm("100000")
    assert c.min_interval < slowed        # sped back up after successes


def test_missing_credentials_rejected():
    with pytest.raises(ValueError):
        FCAClient("", "")


def test_search_follows_results(fake_api):
    fake_api.add(1, name="Alpha Bank")
    fake_api.add(2, name="Beta Bank")
    fake_api.add(3, name="Gamma Insurance")
    hits = list(client().search_firms("bank"))
    assert {h["Reference Number"] for h in hits} == {"1", "2"}
