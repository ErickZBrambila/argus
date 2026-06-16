"""Market session and trading day logic."""

import datetime
import logging

logger = logging.getLogger(__name__)

# ── Market hours (NYSE / NASDAQ Eastern time) ────────────────────────────────
_MARKET_OPEN_H = 9
_MARKET_OPEN_M = 30
_MARKET_CLOSE_H = 16
_MARKET_CLOSE_M = 0


_ET = None
def _et_tz():
    global _ET
    if _ET is None:
        import pytz
        _ET = pytz.timezone("America/New_York")
    return _ET


_NYSE_CAL = None
def _get_nyse_cal():
    global _NYSE_CAL
    if _NYSE_CAL is None:
        try:
            import pandas_market_calendars as mcal
            _NYSE_CAL = mcal.get_calendar("NYSE")
        except Exception:
            pass
    return _NYSE_CAL


def _is_trading_day(date: datetime.date) -> bool:
    cal = _get_nyse_cal()
    if cal is None:
        return date.weekday() < 5
    try:
        schedule = cal.schedule(
            start_date=date.strftime("%Y-%m-%d"),
            end_date=date.strftime("%Y-%m-%d"),
        )
        return not schedule.empty
    except Exception:
        return date.weekday() < 5


def get_market_session() -> str:
    """Return the current NYSE market session in ET."""
    try:
        import datetime as _dt
        now = _dt.datetime.now(_et_tz())
        if now.weekday() >= 5 or not _is_trading_day(now.date()):
            return "closed"
        t = now.time()
        if _dt.time(4, 0) <= t < _dt.time(9, 30):
            return "premarket"
        if _dt.time(9, 30) <= t < _dt.time(16, 0):
            return "open"
        if _dt.time(16, 0) <= t < _dt.time(20, 0):
            return "afterhours"
        return "closed"
    except Exception:
        logger.error("get_market_session() failed — defaulting to closed", exc_info=True)
        return "closed"   # fail closed: safer than fail open


def is_market_hours() -> bool:
    return get_market_session() == "open"
