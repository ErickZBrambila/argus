"""
market_intelligence.py — unusual options flow + SEC EDGAR insider buying screeners.
Both are called from Autopilot._refresh_screener() once per day at market open.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from datetime import date, timedelta
from functools import lru_cache

import httpx

logger = logging.getLogger(__name__)

_SYMBOL_RE = re.compile(r"^[A-Z]{1,5}$")

_EDGAR_HEADERS = {
    "User-Agent": "Argus Trading Bot contact@argus.local",
    "Accept-Encoding": "gzip, deflate",
}

# ─── EDGAR rate-limit enforcement (max 10 req/s → min 0.12s gap) ─────────────

_edgar_lock = threading.Lock()
_edgar_last_request: float = 0.0


def _edgar_get(url: str, **kwargs) -> httpx.Response:
    """httpx.get wrapper that enforces EDGAR's 10 req/s rate limit."""
    global _edgar_last_request
    with _edgar_lock:
        now = time.monotonic()
        wait = 0.12 - (now - _edgar_last_request)
        if wait > 0:
            time.sleep(wait)
        _edgar_last_request = time.monotonic()
    return httpx.get(url, headers=_EDGAR_HEADERS, **kwargs)


# ─── Unusual Options Flow ─────────────────────────────────────────────────────

def _check_symbol_options(sym: str, min_call_vol: int) -> dict | None:
    """Return an options_flow screener entry for sym, or None if not unusual."""
    try:
        import yfinance as yf
        ticker = yf.Ticker(sym)
        exps = ticker.options
        if not exps:
            return None
        chain = ticker.option_chain(exps[0])
        calls = chain.calls
        puts = chain.puts
        if calls.empty:
            return None
        call_vol = int(calls["volume"].fillna(0).sum())
        call_oi = int(calls["openInterest"].fillna(0).sum())
        put_vol = int(puts["volume"].fillna(0).sum()) if not puts.empty else 0
        if call_vol < min_call_vol:
            return None
        oi_ratio = call_vol / call_oi if call_oi > 0 else 0.0
        cp_ratio = call_vol / put_vol if put_vol > 0 else 10.0
        if oi_ratio > 0.3 or cp_ratio > 2.5:
            return {
                "symbol": sym,
                "reason": (
                    f"Unusual call flow: {call_vol:,} calls "
                    f"({oi_ratio:.0%} of OI, {cp_ratio:.1f}x call/put)"
                ),
                "category": "options_flow",
            }
    except Exception as exc:
        logger.debug("Options flow check failed for %s: %s", sym, exc)
    return None


def get_unusual_options_flow(
    symbols: list[str],
    min_call_volume: int = 500,
    max_workers: int = 8,
) -> list[dict]:
    """
    Scan symbols for unusual call option activity using yfinance.
    Returns screener-format list sorted by symbol.
    Never raises — returns [] on any error.
    """
    if not symbols:
        return []
    results: list[dict] = []
    try:
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="optflow") as pool:
            futures = {
                pool.submit(_check_symbol_options, sym, min_call_volume): sym
                for sym in symbols
            }
            for fut in as_completed(futures, timeout=12 * len(symbols)):
                try:
                    entry = fut.result(timeout=12)
                    if entry:
                        results.append(entry)
                except FuturesTimeoutError:
                    logger.debug("Options flow timeout for %s", futures[fut])
                except Exception as exc:
                    logger.debug("Options flow future error for %s: %s", futures[fut], exc)
    except Exception as exc:
        logger.warning("get_unusual_options_flow failed: %s", exc)
    return sorted(results, key=lambda x: x["symbol"])


# ─── SEC EDGAR Insider Buying ─────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _load_company_tickers() -> dict[int, str]:
    """
    Load SEC company tickers map: CIK (int) → ticker symbol.
    Cached for the lifetime of the process (refreshes on restart).
    """
    try:
        resp = _edgar_get(
            "https://www.sec.gov/files/company_tickers.json",
            timeout=15,
        )
        if resp.status_code != 200:
            return {}
        # Format: {"0": {"cik_str": 320193, "ticker": "AAPL", ...}, ...}
        return {int(v["cik_str"]): v["ticker"].upper() for v in resp.json().values()}
    except Exception as exc:
        logger.warning("EDGAR tickers map load failed: %s", exc)
        return {}


@lru_cache(maxsize=256)
def _get_primary_document(insider_cik: str, adsh: str) -> str | None:
    """
    Return the primary XML document filename for a Form 4 filing.
    Uses the insider's submissions JSON, cached per (CIK, adsh).
    adsh format: "0001234567-26-000123"
    """
    padded_cik = insider_cik.zfill(10)
    try:
        resp = _edgar_get(
            f"https://data.sec.gov/submissions/CIK{padded_cik}.json",
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        recent = resp.json().get("filings", {}).get("recent", {})
        acc_numbers = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        target = adsh.replace("-", "")
        for i, acc in enumerate(acc_numbers):
            if acc.replace("-", "") == target and i < len(primary_docs):
                doc = primary_docs[i]
                # Submissions JSON may prefix with XSL path (e.g. "xslF345X06/primarydocument.xml")
                # The raw data XML is just the filename without the directory prefix.
                return doc.split("/")[-1] if "/" in doc else doc
    except Exception as exc:
        logger.debug("EDGAR submissions lookup failed CIK=%s: %s", insider_cik, exc)
    return None


def _is_purchase_filing(insider_cik: str, adsh: str) -> bool:
    """
    Fetch the Form 4 XML and confirm it contains a Purchase transaction (code P).
    The ticker is resolved separately via the company tickers map.
    """
    primary_doc = _get_primary_document(insider_cik, adsh)
    if not primary_doc:
        return False
    acc_clean = adsh.replace("-", "")
    try:
        cik_int = int(insider_cik)
    except ValueError:
        return False
    url = (
        f"https://www.sec.gov/Archives/edgar/data/"
        f"{cik_int}/{acc_clean}/{primary_doc}"
    )
    try:
        resp = _edgar_get(url, timeout=10)
        if resp.status_code != 200:
            return False
        return "<transactionCode>P</transactionCode>" in resp.text
    except Exception as exc:
        logger.debug("EDGAR form4 fetch failed %s: %s", url, exc)
    return False


def get_insider_buys(days_back: int = 3, max_filings: int = 80) -> list[dict]:
    """
    Fetch recent insider purchases from SEC EDGAR Form 4 filings.
    Returns screener-format list sorted by insider count descending.
    Never raises — returns [] on any error.

    EDGAR EFTS response fields (confirmed):
      _source.adsh        — accession number e.g. "0001234567-26-000002"
      _source.ciks        — list of CIKs; the one matching adsh prefix is the
                            insider/filer, the other is the issuer company
    """
    today = date.today()
    start = today - timedelta(days=days_back)

    # Load company CIK → ticker map
    tickers_map = _load_company_tickers()
    if not tickers_map:
        logger.warning("EDGAR: tickers map empty, skipping insider scan")
        return []

    # Fetch all Form 4 filings in date range (EFTS doesn't support filtering
    # by transaction code — we fetch all and check each XML)
    try:
        resp = httpx.get(
            "https://efts.sec.gov/LATEST/search-index",
            params={
                "forms": "4",
                "dateRange": "custom",
                "startdt": start.isoformat(),
                "enddt": today.isoformat(),
            },
            headers=_EDGAR_HEADERS,
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning("EDGAR EFTS returned %d", resp.status_code)
            return []
        hits = resp.json().get("hits", {}).get("hits", [])
    except Exception as exc:
        logger.warning("EDGAR EFTS search failed: %s", exc)
        return []

    hits = hits[:max_filings]
    if not hits:
        return []

    # Parse hits: identify insider CIK, company CIK, accession number
    filing_tasks: list[tuple[str, str, str]] = []  # (insider_cik, adsh, ticker)
    for hit in hits:
        src = hit.get("_source", {})
        adsh = src.get("adsh", "")           # e.g. "0001234567-26-000002"
        ciks = src.get("ciks", [])            # [insider_cik, company_cik] typically
        if not adsh or len(ciks) < 2:
            continue
        # The insider CIK is the one whose digits prefix the adsh
        adsh_prefix = adsh.replace("-", "")[:10]
        insider_cik = next((c for c in ciks if c.zfill(10) == adsh_prefix), ciks[0])
        company_cik = next((c for c in ciks if c.zfill(10) != adsh_prefix), None)
        if not company_cik:
            continue
        ticker = tickers_map.get(int(company_cik))
        if not ticker or not _SYMBOL_RE.match(ticker):
            continue
        filing_tasks.append((insider_cik, adsh, ticker))

    if not filing_tasks:
        return []

    ticker_counts: dict[str, int] = {}

    try:
        with ThreadPoolExecutor(max_workers=5, thread_name_prefix="edgar") as pool:
            futures = {
                pool.submit(_is_purchase_filing, insider_cik, adsh): ticker
                for insider_cik, adsh, ticker in filing_tasks
            }
            for fut in as_completed(futures, timeout=300):
                ticker = futures[fut]
                try:
                    if fut.result(timeout=30):
                        ticker_counts[ticker] = ticker_counts.get(ticker, 0) + 1
                except Exception as exc:
                    logger.debug("EDGAR future error for %s: %s", ticker, exc)
    except Exception as exc:
        logger.warning("get_insider_buys parallel fetch failed: %s", exc)

    results = []
    for sym, count in sorted(ticker_counts.items(), key=lambda x: -x[1]):
        label = "insiders" if count > 1 else "insider"
        results.append({
            "symbol": sym,
            "reason": f"SEC Form 4: {count} {label} purchased shares (last {days_back}d)",
            "category": "insider",
        })
    return results
