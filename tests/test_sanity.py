"""
Sanity tests — run after every build or before calling it a day.
Each test covers a specific bug that was found and fixed; regression means
a bug has come back.

Usage:
    pytest tests/test_sanity.py -v
    pytest tests/ -v          # run all tests
"""
from __future__ import annotations

import datetime
from unittest.mock import patch

import pytest

# ─── Auth enforcement ────────────────────────────────────────────────────────
# Bugs: gated endpoints were accessible without a token; SSE EventSource
# couldn't send headers so auth failed even with valid token.


@pytest.fixture()
def auth_client():
    from argus.dashboard.web import _configure_auth, app
    from fastapi.testclient import TestClient

    _configure_auth("sanity-secret-9876")
    yield TestClient(app, raise_server_exceptions=False)
    _configure_auth("")  # reset so later tests run unauthenticated


def test_gated_endpoint_rejects_no_token(auth_client):
    assert auth_client.get("/api/logs").status_code == 401


def test_gated_endpoint_rejects_wrong_token(auth_client):
    resp = auth_client.get("/api/logs", headers={"X-Argus-Token": "wrong"})
    assert resp.status_code == 401


def test_gated_endpoint_accepts_correct_header_token(auth_client):
    resp = auth_client.get("/api/logs", headers={"X-Argus-Token": "sanity-secret-9876"})
    assert resp.status_code != 401


def test_gated_endpoint_accepts_query_param_token(auth_client):
    """EventSource can't send headers — token falls back to ?token= query param."""
    resp = auth_client.get("/api/logs?token=sanity-secret-9876")
    assert resp.status_code != 401


def test_no_auth_required_when_token_not_configured():
    """Dev mode: if no token is set, all endpoints are open."""
    from argus.dashboard.web import _configure_auth, app
    from fastapi.testclient import TestClient

    _configure_auth("")
    client = TestClient(app, raise_server_exceptions=False)
    assert client.get("/api/logs").status_code != 401


# ─── Security headers ────────────────────────────────────────────────────────
# Bug: responses had no clickjacking / content-sniff protection headers.


def test_security_headers_on_every_response(auth_client):
    resp = auth_client.get("/api/logs", headers={"X-Argus-Token": "sanity-secret-9876"})
    assert resp.headers.get("X-Frame-Options") == "DENY"
    assert resp.headers.get("X-Content-Type-Options") == "nosniff"
    assert resp.headers.get("Referrer-Policy") == "no-referrer"


# ─── AppleScript injection prevention ────────────────────────────────────────
# Bug: AI-generated text containing \n could inject a second AppleScript
# statement (e.g. "end tell; launch app …"); _esc now strips \n and \r.


def _capture_applescript(subject: str, body: str) -> str:
    from argus.notifications.notifier import Notifier

    captured: list[str] = []

    def _mock_run(args, **kwargs):
        if len(args) >= 3:
            captured.append(args[2])

    with patch("subprocess.run", side_effect=_mock_run):
        Notifier()._try_macos(subject, body)

    return captured[0] if captured else ""


def test_applescript_newline_stripped():
    script = _capture_applescript("Title", "line1\nend tell; say 'pwned'")
    assert script, "osascript was not called"
    assert "\n" not in script


def test_applescript_carriage_return_stripped():
    script = _capture_applescript("Title", "line1\rline2")
    assert "\r" not in script


def test_applescript_double_quote_escaped():
    """A raw " in subject must be escaped so it can't break out of the script string."""
    script = _capture_applescript('inject" danger', "body")
    # Unescaped: subtitle "inject" danger" — breaks AppleScript syntax
    # Escaped:   subtitle "inject\" danger" — safe
    assert 'inject" danger' not in script


# ─── New listing grace period ────────────────────────────────────────────────
# Bug: SPCX (IPO June 12) had only 3 bars; the 50-bar minimum blocked all
# signals.  _compute() now uses 3 bars for new listings within 90 days.


def _grace_min_bars(symbol: str, today: datetime.date) -> int:
    """Mirror of the grace-period logic inside SignalEngine._compute."""
    from argus.strategy.indicators import (
        _NEW_LISTINGS,
        _NEW_LISTING_GRACE_DAYS,
        _NEW_LISTING_MIN_BARS,
    )

    date_str = _NEW_LISTINGS.get(symbol)
    if date_str:
        listing_date = datetime.date.fromisoformat(date_str)
        if today <= listing_date + datetime.timedelta(days=_NEW_LISTING_GRACE_DAYS):
            return _NEW_LISTING_MIN_BARS
    return 50


def test_new_listing_grace_active_on_ipo_day():
    from argus.strategy.indicators import _NEW_LISTINGS
    ipo = datetime.date.fromisoformat(_NEW_LISTINGS["SPCX"])
    assert _grace_min_bars("SPCX", today=ipo) == 3


def test_new_listing_grace_active_on_day_90():
    """Boundary: grace period is inclusive on the 90th day."""
    from argus.strategy.indicators import _NEW_LISTINGS
    ipo = datetime.date.fromisoformat(_NEW_LISTINGS["SPCX"])
    assert _grace_min_bars("SPCX", today=ipo + datetime.timedelta(days=90)) == 3


def test_new_listing_grace_expired_on_day_91():
    from argus.strategy.indicators import _NEW_LISTINGS
    ipo = datetime.date.fromisoformat(_NEW_LISTINGS["SPCX"])
    assert _grace_min_bars("SPCX", today=ipo + datetime.timedelta(days=91)) == 50


def test_established_symbol_uses_50_bar_minimum():
    assert _grace_min_bars("AAPL", today=datetime.date.today()) == 50


# ─── Kill switch — day-rollover persistence ──────────────────────────────────
# Bug: _restore_session_state set kill_switch=True from DB but NEVER cleared
# it to False for a new day, so yesterday's triggered kill switch silenced all
# decisions the next day.


def test_kill_switch_clears_after_reset():
    from argus.risk.manager import RiskManager

    rm = RiskManager(daily_drawdown_limit=-0.05)
    rm.set_session_equity(10_000)
    rm.check_drawdown(9_000)  # triggers
    assert rm.kill_switch_active is True

    rm.reset_kill_switch()
    assert rm.kill_switch_active is False


def test_buys_allowed_after_kill_switch_reset():
    """After reset, approve_buy must succeed — regression: stale True blocked all decisions."""
    from argus.risk.manager import RiskManager

    rm = RiskManager(daily_drawdown_limit=-0.05)
    rm.set_session_equity(10_000)
    rm.check_drawdown(9_000)
    rm.reset_kill_switch()
    rm.set_session_equity(10_000)

    decision = rm.approve_buy("TSLA", 10_000, {})
    assert decision.allowed is True


# ─── mark_account_kill_switch upsert ─────────────────────────────────────────
# Bug: if no AccountDailyStats row existed (first trade of the day), the mark
# function was a silent no-op and the kill switch was never persisted to disk.


@pytest.fixture()
def temp_db(tmp_path):
    """Isolated single-file SQLite DB — resets between tests."""
    from argus.storage.models import init_db, get_session

    init_db(f"sqlite:///{tmp_path}/sanity.db")
    return get_session


def test_mark_kill_switch_creates_row_when_missing(temp_db):
    from argus.storage.models import AccountDailyStats, mark_account_kill_switch

    today = datetime.date.today()
    with temp_db() as session:
        mark_account_kill_switch(session, today, "main")
    # Committed — verify row exists in a fresh session
    with temp_db() as session:
        row = session.query(AccountDailyStats).filter_by(date=today, account_label="main").first()
        assert row is not None
        assert row.kill_switch_triggered is True


def test_mark_kill_switch_is_idempotent(temp_db):
    """Calling twice must not create a duplicate row."""
    from argus.storage.models import AccountDailyStats, mark_account_kill_switch

    today = datetime.date.today()
    with temp_db() as session:
        mark_account_kill_switch(session, today, "main")
    with temp_db() as session:
        mark_account_kill_switch(session, today, "main")
        count = (
            session.query(AccountDailyStats)
            .filter_by(date=today, account_label="main")
            .count()
        )
        assert count == 1


# ─── Approval queue — timezone-aware datetime ────────────────────────────────
# Bug: queue_approval stored datetime.utcnow() (naive); _poll_approvals used
# datetime.now(UTC) (aware); subtraction raised TypeError every cycle.


def test_approval_queued_at_is_timezone_aware():
    from argus.dashboard import web as _web

    _web.queue_approval("sanity-001", {"symbol": "TEST", "action": "BUY"})
    try:
        entry = _web._pending_approvals.get("sanity-001")
        assert entry is not None
        ts = datetime.datetime.fromisoformat(entry["queued_at"])
        assert ts.tzinfo is not None, "queued_at must be tz-aware (naive causes TypeError in TTL check)"
    finally:
        _web._pending_approvals.pop("sanity-001", None)


# ─── Symbol validation ────────────────────────────────────────────────────────
# Validation guards injection into broker API calls and DB queries.


def test_symbol_validation_normalises_to_uppercase():
    from argus.strategy.indicators import _validate_symbol

    assert _validate_symbol("  aapl  ") == "AAPL"


def test_symbol_validation_rejects_special_characters():
    from argus.strategy.indicators import _validate_symbol

    with pytest.raises(ValueError):
        _validate_symbol("AA; DROP TABLE")


def test_symbol_validation_rejects_oversized_input():
    from argus.strategy.indicators import _validate_symbol

    with pytest.raises(ValueError):
        _validate_symbol("A" * 11)


# ─── DB — exit_only flag ─────────────────────────────────────────────────────


def test_exit_only_persists_and_is_readable(temp_db):
    from argus.storage.models import add_to_db_watchlist, get_exit_only_symbols, set_exit_only

    with temp_db() as session:
        add_to_db_watchlist(session, "BIRD")
        set_exit_only(session, "BIRD", True)
    # Committed — read back in fresh session to confirm persistence
    with temp_db() as session:
        result = get_exit_only_symbols(session)

    assert "BIRD" in result


def test_exit_only_can_be_cleared(temp_db):
    from argus.storage.models import add_to_db_watchlist, get_exit_only_symbols, set_exit_only

    with temp_db() as session:
        add_to_db_watchlist(session, "BIRD")
        set_exit_only(session, "BIRD", True)
    with temp_db() as session:
        set_exit_only(session, "BIRD", False)
    # Committed — verify in a third session
    with temp_db() as session:
        result = get_exit_only_symbols(session)

    assert "BIRD" not in result


# ─── DB — day trade counter ──────────────────────────────────────────────────


def test_count_day_trades_excludes_today(temp_db):
    """count_day_trades_last_5_days must NOT include today — today's trades live in
    RiskManager._day_trade_count to avoid double-counting in approve_buy."""
    from argus.storage.models import count_day_trades_last_5_days, get_or_create_daily_stats

    today = datetime.date.today()
    # offsets: 0=today(1), 1=yesterday(1), 2(0), 3(1), 4(0) → past-only sum = 1+0+1+0 = 2
    with temp_db() as session:
        for offset, trades in enumerate([1, 1, 0, 1, 0]):
            s = get_or_create_daily_stats(session, today - datetime.timedelta(days=offset), 10_000)
            s.day_trades = trades

    with temp_db() as session:
        total = count_day_trades_last_5_days(session)

    assert total == 2, f"Expected 2 (today excluded), got {total}"


def test_get_today_day_trades(temp_db):
    """get_today_day_trades returns only today's count (used to seed _day_trade_count on restart)."""
    from argus.storage.models import get_or_create_daily_stats, get_today_day_trades

    today = datetime.date.today()
    with temp_db() as session:
        s = get_or_create_daily_stats(session, today, 10_000)
        s.day_trades = 2

    with temp_db() as session:
        assert get_today_day_trades(session) == 2
