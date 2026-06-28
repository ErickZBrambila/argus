"""Robinhood broker connector.

Live mode uses robin_stocks; paper mode simulates orders locally.
"""

from __future__ import annotations

import json
import logging
import pathlib
import re
import threading
import uuid
from dataclasses import dataclass, field
from typing import Optional

import pyotp
from pydantic import SecretStr

logger = logging.getLogger(__name__)

CRYPTO_SYMBOLS = {"BTC", "ETH", "DOGE", "SOL", "LTC", "BCH", "ETC"}

_YF_CRYPTO = {s: f"{s}-USD" for s in CRYPTO_SYMBOLS}  # yfinance ticker map

_SYMBOL_RE = re.compile(r"^[A-Z0-9.]{1,10}$")


def _validated_symbol(symbol: str) -> str:
    s = symbol.strip().upper()
    if not _SYMBOL_RE.fullmatch(s):
        raise ValueError(f"Rejected symbol: {symbol!r}")
    return s


_VALID_SPANS = frozenset({"day", "week", "month", "3month", "year", "5year"})
_VALID_INTERVALS = frozenset({"5minute", "10minute", "hour", "day", "week"})


@dataclass
class OrderResult:
    order_id: str
    symbol: str
    side: str
    quantity: float
    price: float
    filled: bool
    paper: bool
    raw: dict = field(default_factory=dict)


class RobinhoodBroker:
    def __init__(
        self,
        username: str,
        password: SecretStr | str,
        mfa_secret: SecretStr | str = "",
        paper: bool = True,
        account_number: str = "",
    ) -> None:
        self.username = username
        self._password = password if isinstance(password, SecretStr) else SecretStr(password)
        self._mfa_secret: Optional[SecretStr] = (
            (mfa_secret if isinstance(mfa_secret, SecretStr) else SecretStr(mfa_secret))
            if mfa_secret
            else None
        )
        self.paper = paper
        self.account_number = account_number or ""
        self._logged_in = False
        self._paper_positions: dict[str, dict] = {}
        self._paper_equity = 10_000.0
        self._paper_lock = threading.Lock()
        # Persist path keyed by account number so multi-account paper states don't collide
        _acct_suffix = f"_{account_number}" if account_number else ""
        self._paper_state_path = pathlib.Path(__file__).parent.parent.parent / f"paper_state{_acct_suffix}.json"
        self._paper_load()

        if not paper:
            self._login()
        else:
            # Login even in paper mode — Robinhood requires auth for crypto
            # historicals. Non-fatal: if credentials are missing, equity data
            # still works and crypto degrades gracefully.
            try:
                self._login()
            except Exception:
                logger.warning(
                    "Robinhood login failed in paper mode; "
                    "crypto historicals will be unavailable"
                )

    # ── Auth ────────────────────────────────────────────────────────────────

    def _login(self) -> None:
        try:
            import robin_stocks.robinhood as rh

            mfa_code: Optional[str] = None
            if self._mfa_secret:
                mfa_code = pyotp.TOTP(self._mfa_secret.get_secret_value()).now()
                self._mfa_secret = None    # clear from memory immediately

            rh.login(
                self.username,
                self._password.get_secret_value(),
                mfa_code=mfa_code,
                store_session=False,       # never persist tokens to disk
            )
            self._logged_in = True
            logger.info("Robinhood login successful")
        except Exception as exc:
            logger.error("Robinhood login failed: %s", exc)
            raise

    def logout(self) -> None:
        if self._logged_in:
            try:
                import robin_stocks.robinhood as rh
                rh.logout()
                self._logged_in = False
            except Exception as exc:
                logger.warning("Logout error: %s", exc)

    # ── Market data ─────────────────────────────────────────────────────────

    def get_price(self, symbol: str) -> float:
        symbol = _validated_symbol(symbol)
        price = self._live_get_price(symbol) if not self.paper else self._paper_get_price(symbol)
        if price <= 0:
            raise ValueError(f"Price for {symbol} is not positive: {price}")
        return price

    def _live_get_price(self, symbol: str) -> float:
        import robin_stocks.robinhood as rh

        if symbol in CRYPTO_SYMBOLS:
            data = rh.crypto.get_crypto_quote(symbol)
            return float(data["mark_price"])
        data = rh.stocks.get_latest_price(symbol)
        return float(data[0])

    def _paper_load(self) -> None:
        try:
            if not self._paper_state_path.exists():
                return
            data = json.loads(self._paper_state_path.read_text())
            self._paper_equity    = float(data.get("equity", 10_000.0))
            self._paper_positions = {
                sym: {"qty": float(p["qty"]), "avg_price": float(p["avg_price"])}
                for sym, p in data.get("positions", {}).items()
            }
            logger.info("[PAPER] Restored state: equity=$%.2f, %d position(s)",
                        self._paper_equity, len(self._paper_positions))
        except Exception as exc:
            logger.warning("[PAPER] Could not load paper state: %s", exc)

    def _paper_save(self) -> None:
        """Must be called with _paper_lock held."""
        try:
            self._paper_state_path.write_text(json.dumps({
                "equity":    round(self._paper_equity, 6),
                "positions": {
                    sym: {"qty": round(p["qty"], 8), "avg_price": round(p["avg_price"], 6)}
                    for sym, p in self._paper_positions.items()
                },
            }))
        except Exception as exc:
            logger.debug("[PAPER] Could not save paper state: %s", exc)

    def _paper_get_price(self, symbol: str) -> float:
        try:
            return self._live_get_price(symbol)
        except Exception:
            pass
        # Fallback to yfinance for crypto (Robinhood auth may be unavailable)
        if symbol in CRYPTO_SYMBOLS:
            try:
                import yfinance as yf
                t = yf.Ticker(f"{symbol}-USD")
                info = t.fast_info
                px = float(info.last_price or info.previous_close or 0)
                if px > 0:
                    return px
            except Exception:
                pass
        raise RuntimeError(f"Cannot determine price for {symbol} in paper mode")

    def get_portfolio_equity(self) -> float:
        if self.paper:
            with self._paper_lock:
                # cash remaining + full market value of open positions
                return self._paper_equity + self._paper_position_value_unsafe()
        return self._live_equity()

    def _live_equity(self) -> float:
        import robin_stocks.robinhood as rh
        acct = self.account_number or None
        profile = rh.profiles.load_portfolio_profile(account_number=acct)
        # Use extended-hours equity when available (after-hours positions are marked to market)
        eq = profile.get("extended_hours_equity") or profile.get("equity") or 0
        return float(eq)

    def _paper_position_value_unsafe(self) -> float:
        """Return total market value of all open positions. Must be called with _paper_lock held."""
        # Snapshot positions first so we can release the lock before network calls
        snapshot = dict(self._paper_positions)
        total = 0.0
        for sym, pos in snapshot.items():
            try:
                price = self._live_get_price(sym)
            except Exception:
                price = pos["avg_price"]
            total += price * pos["qty"]
        return total

    def get_open_positions(self) -> dict[str, dict]:
        if self.paper:
            with self._paper_lock:
                return dict(self._paper_positions)
        return self._live_positions()

    def _live_positions(self) -> dict[str, dict]:
        import robin_stocks.robinhood as rh

        result: dict[str, dict] = {}
        acct = self.account_number or None

        # Use account-filtered positions so multi-account live mode returns correct data
        positions = rh.account.get_open_stock_positions(account_number=acct) or []
        for pos in positions:
            instrument_url = pos.get("instrument", "")
            qty = float(pos.get("quantity", 0))
            if qty <= 0:
                continue
            avg = float(pos.get("average_buy_price", 0))
            try:
                sym_data = rh.stocks.get_instrument_by_url(instrument_url, info="symbol")
                sym = (sym_data or "").strip().upper()
            except Exception:
                continue
            if sym:
                result[sym] = {"qty": qty, "avg_price": avg}

        # Crypto positions are not account-scoped in the Robinhood API
        crypto_holdings = rh.crypto.get_crypto_positions() or []
        for item in crypto_holdings:
            sym = item.get("currency", {}).get("code", "")
            qty = float(item.get("quantity", 0))
            if sym and qty > 0:
                cost = float(item.get("cost_bases", [{}])[0].get("direct_cost_basis", 0))
                avg = cost / qty if qty else 0
                result[sym] = {"qty": qty, "avg_price": avg}
        return result

    def get_screener_symbols(self) -> list[dict]:
        """Return daily movers + upcoming earnings symbols as screener candidates.

        Each entry: {"symbol": str, "reason": str, "category": "mover"|"earnings"}
        Requires a live authenticated session; returns [] on paper or if unauthenticated.
        """
        if self.paper or not self._logged_in:
            return []
        result: list[dict] = []
        seen: set[str] = set()
        try:
            import robin_stocks.robinhood as rh

            # Top S&P 500 movers (up and down, up to 5 each)
            for direction in ("up", "down"):
                movers = rh.markets.get_top_movers_sp500(direction) or []
                for m in movers[:5]:
                    sym = (m.get("symbol") or "").strip().upper()
                    if sym and _SYMBOL_RE.fullmatch(sym) and sym not in seen:
                        seen.add(sym)
                        pct = m.get("price_movement", {}).get("percent_change", "")
                        result.append({
                            "symbol": sym,
                            "reason": f"S&P 500 top {direction} mover {pct}%",
                            "category": "mover",
                        })
        except Exception as exc:
            logger.debug("Screener movers fetch failed: %s", exc)

        try:
            import robin_stocks.robinhood as rh
            import datetime as _dt

            # Stocks with earnings in the next 7 days
            upcoming = rh.stocks.get_earnings(None, info=None) if False else []
            # get_earnings requires a symbol; use top_100 watchlist as universe instead
            top = rh.markets.get_top_movers(info="symbol") or []
            today = _dt.date.today()
            for sym in top[:20]:
                sym = (sym or "").strip().upper()
                if not sym or not _SYMBOL_RE.fullmatch(sym) or sym in seen:
                    continue
                try:
                    earnings = rh.stocks.get_earnings(sym, info=None) or []
                    for e in earnings:
                        report = e.get("report", {})
                        date_str = report.get("date") if report else None
                        if not date_str:
                            continue
                        edate = _dt.date.fromisoformat(date_str)
                        days_out = (edate - today).days
                        if 0 <= days_out <= 7:
                            seen.add(sym)
                            result.append({
                                "symbol": sym,
                                "reason": f"Earnings in {days_out}d ({date_str})",
                                "category": "earnings",
                            })
                            break
                except Exception:
                    pass
        except Exception as exc:
            logger.debug("Screener earnings fetch failed: %s", exc)

        return result

    def get_historical_prices(self, symbol: str, span: str = "month", interval: str = "day") -> list[dict]:
        symbol = _validated_symbol(symbol)
        if span not in _VALID_SPANS:
            raise ValueError(f"Invalid span {span!r}. Allowed: {_VALID_SPANS}")
        if interval not in _VALID_INTERVALS:
            raise ValueError(f"Invalid interval {interval!r}. Allowed: {_VALID_INTERVALS}")

        # When not authenticated, skip the Robinhood call entirely for crypto —
        # the yfinance fallback handles it without auth. Avoids a noisy warning
        # every tick when the session is unauthenticated.
        if self._logged_in:
            try:
                import robin_stocks.robinhood as rh

                if symbol in CRYPTO_SYMBOLS:
                    data = rh.crypto.get_crypto_historicals(symbol, interval=interval, span=span)
                else:
                    data = rh.stocks.get_stock_historicals(symbol, interval=interval, span=span)
                if data:
                    return data
            except Exception as exc:
                # Crypto falls back to yfinance below — not actionable, keep quiet
                if symbol in CRYPTO_SYMBOLS:
                    logger.debug("Robinhood crypto historicals unavailable for %s, using yfinance: %s", symbol, exc)
                else:
                    logger.warning("Could not fetch historicals for %s: %s", symbol, exc)

        # Fallback for crypto: Yahoo Finance requires no auth
        if symbol in CRYPTO_SYMBOLS:
            return self._yf_crypto_historicals(symbol, span)
        return []

    def _yf_crypto_historicals(self, symbol: str, span: str) -> list[dict]:
        """Fetch crypto OHLCV from Yahoo Finance — no Robinhood auth needed."""
        try:
            import yfinance as yf
            _period = {
                'day': '5d', 'week': '1mo', 'month': '1mo',
                '3month': '3mo', 'year': '1y', '5year': '5y',
            }.get(span, '3mo')
            df = yf.download(
                _YF_CRYPTO.get(symbol, f"{symbol}-USD"),
                period=_period, interval='1d',
                progress=False, auto_adjust=True,
            )
            if df.empty:
                return []
            # Flatten MultiIndex columns produced by yfinance
            if hasattr(df.columns, 'levels'):
                df.columns = [c[0].lower() for c in df.columns]
            else:
                df.columns = [c.lower() for c in df.columns]
            return [
                {
                    "begins_at": str(ts.date()),
                    "open_price":  str(row.get("open",  0)),
                    "close_price": str(row.get("close", 0)),
                    "high_price":  str(row.get("high",  0)),
                    "low_price":   str(row.get("low",   0)),
                    "volume":      int(row.get("volume", 0)),
                    "symbol":      symbol,
                }
                for ts, row in df.iterrows()
            ]
        except Exception as exc:
            logger.warning("Yahoo Finance fallback failed for %s: %s", symbol, exc)
            return []

    # ── Order execution ──────────────────────────────────────────────────────

    def buy(self, symbol: str, dollar_amount: float) -> OrderResult:
        symbol = _validated_symbol(symbol)
        if dollar_amount <= 0:
            raise ValueError(f"dollar_amount must be positive, got {dollar_amount}")
        price = self.get_price(symbol)    # raises if price <= 0
        qty = dollar_amount / price

        if self.paper:
            return self._paper_buy(symbol, qty, price)
        return self._live_buy(symbol, qty, price)

    def sell(self, symbol: str, quantity: float) -> OrderResult:
        symbol = _validated_symbol(symbol)
        if quantity <= 0:
            raise ValueError(f"quantity must be positive, got {quantity}")
        price = self.get_price(symbol)

        if self.paper:
            return self._paper_sell(symbol, quantity, price)
        return self._live_sell(symbol, quantity, price)

    # ── Paper order helpers ──────────────────────────────────────────────────

    def _paper_buy(self, symbol: str, qty: float, price: float) -> OrderResult:
        with self._paper_lock:
            cost = qty * price
            if cost > self._paper_equity:
                qty = self._paper_equity / price
                cost = self._paper_equity

            if symbol in self._paper_positions:
                old = self._paper_positions[symbol]
                new_qty = old["qty"] + qty
                new_avg = (old["qty"] * old["avg_price"] + qty * price) / new_qty
                self._paper_positions[symbol] = {"qty": new_qty, "avg_price": new_avg}
            else:
                self._paper_positions[symbol] = {"qty": qty, "avg_price": price}

            self._paper_equity -= cost
            self._paper_save()

        order_id = str(uuid.uuid4())
        logger.info("[PAPER] BUY %s %.4f @ $%.4f", symbol, qty, price)
        return OrderResult(order_id=order_id, symbol=symbol, side="buy", quantity=qty, price=price, filled=True, paper=True)

    def _paper_sell(self, symbol: str, qty: float, price: float) -> OrderResult:
        with self._paper_lock:
            pos = self._paper_positions.get(symbol)
            if pos is None or pos["qty"] < qty:
                qty = pos["qty"] if pos else 0.0
            if qty <= 0:
                return OrderResult(order_id="", symbol=symbol, side="sell", quantity=0, price=price, filled=False, paper=True)

            proceeds = qty * price
            self._paper_equity += proceeds
            remaining = pos["qty"] - qty
            if remaining < 1e-8:
                del self._paper_positions[symbol]
            else:
                self._paper_positions[symbol]["qty"] = remaining
            self._paper_save()

        order_id = str(uuid.uuid4())
        logger.info("[PAPER] SELL %s %.4f @ $%.4f", symbol, qty, price)
        return OrderResult(order_id=order_id, symbol=symbol, side="sell", quantity=qty, price=price, filled=True, paper=True)

    # ── Live order helpers ───────────────────────────────────────────────────

    _FILL_STATES   = frozenset({"filled", "partially_filled"})
    _CANCEL_STATES = frozenset({"cancelled", "failed", "rejected"})

    def _poll_until_filled(self, order_id: str, is_crypto: bool, timeout: float = 30.0) -> dict:
        """Poll order status until filled, cancelled, or timeout. Returns last order dict."""
        import robin_stocks.robinhood as rh
        import time

        order: dict = {}   # guard against UnboundLocalError if every poll attempt throws
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                order = (
                    rh.orders.get_crypto_order_info(order_id)
                    if is_crypto
                    else rh.orders.get_stock_order_info(order_id)
                )
                state = order.get("state", "")
                if state in self._FILL_STATES or state in self._CANCEL_STATES:
                    return order
            except Exception as exc:
                logger.warning("Poll order %s failed: %s", order_id, exc)
            time.sleep(2)
        logger.warning("Order %s did not fill within %.0fs", order_id, timeout)
        return order  # return last known state

    def _live_buy(self, symbol: str, qty: float, price: float) -> OrderResult:
        import robin_stocks.robinhood as rh

        is_crypto = symbol in CRYPTO_SYMBOLS
        try:
            if is_crypto:
                order = rh.orders.order_buy_crypto_by_quantity(symbol, qty)
            else:
                order = rh.orders.order_buy_fractional_by_quantity(
                    symbol, qty, account_number=self.account_number or None
                )

            order_id = order.get("id", str(uuid.uuid4()))
            if order.get("state") not in self._FILL_STATES:
                order = self._poll_until_filled(order_id, is_crypto)
            filled = order.get("state") in self._FILL_STATES
            logger.info("[LIVE] BUY %s %.4f @ $%.4f — id=%s state=%s", symbol, qty, price, order_id, order.get("state"))
            return OrderResult(order_id=order_id, symbol=symbol, side="buy", quantity=qty, price=price, filled=filled, paper=False, raw=order)
        except Exception as exc:
            logger.error("Live buy failed for %s: %s", symbol, exc)
            raise

    def _live_sell(self, symbol: str, qty: float, price: float) -> OrderResult:
        import robin_stocks.robinhood as rh

        is_crypto = symbol in CRYPTO_SYMBOLS
        try:
            if is_crypto:
                order = rh.orders.order_sell_crypto_by_quantity(symbol, qty)
            else:
                order = rh.orders.order_sell_fractional_by_quantity(
                    symbol, qty, account_number=self.account_number or None
                )

            order_id = order.get("id", str(uuid.uuid4()))
            if order.get("state") not in self._FILL_STATES:
                order = self._poll_until_filled(order_id, is_crypto)
            filled = order.get("state") in self._FILL_STATES
            logger.info("[LIVE] SELL %s %.4f @ $%.4f — id=%s state=%s", symbol, qty, price, order_id, order.get("state"))
            return OrderResult(order_id=order_id, symbol=symbol, side="sell", quantity=qty, price=price, filled=filled, paper=False, raw=order)
        except Exception as exc:
            logger.error("Live sell failed for %s: %s", symbol, exc)
            raise
