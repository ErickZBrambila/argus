"""Robinhood broker connector.

Live mode uses robin_stocks; paper mode simulates orders locally.
"""

from __future__ import annotations

import logging
import re
import threading
import uuid
from dataclasses import dataclass, field
from typing import Optional

import pyotp
from pydantic import SecretStr

logger = logging.getLogger(__name__)

CRYPTO_SYMBOLS = {"BTC", "ETH", "DOGE", "SOL", "LTC", "BCH", "ETC"}

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

        if not paper:
            self._login()

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
        if not self.paper and self._logged_in:
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

    def _paper_get_price(self, symbol: str) -> float:
        try:
            return self._live_get_price(symbol)
        except Exception:
            return 100.0

    def get_portfolio_equity(self) -> float:
        if self.paper:
            with self._paper_lock:
                return self._paper_equity + self._paper_unrealized_unsafe()
        return self._live_equity()

    def _live_equity(self) -> float:
        import robin_stocks.robinhood as rh
        profile = rh.profiles.load_portfolio_profile()
        return float(profile.get("equity", 0))

    def _paper_unrealized_unsafe(self) -> float:
        """Must be called with _paper_lock held."""
        total = 0.0
        for sym, pos in self._paper_positions.items():
            try:
                price = self._live_get_price(sym)
            except Exception:
                price = pos["avg_price"]
            total += (price - pos["avg_price"]) * pos["qty"]
        return total

    def get_open_positions(self) -> dict[str, dict]:
        if self.paper:
            with self._paper_lock:
                return dict(self._paper_positions)
        return self._live_positions()

    def _live_positions(self) -> dict[str, dict]:
        import robin_stocks.robinhood as rh

        result: dict[str, dict] = {}
        holdings = rh.account.build_holdings()
        for sym, data in holdings.items():
            result[sym] = {
                "qty": float(data.get("quantity", 0)),
                "avg_price": float(data.get("average_buy_price", 0)),
            }
        crypto_holdings = rh.crypto.get_crypto_positions()
        for item in crypto_holdings:
            sym = item.get("currency", {}).get("code", "")
            qty = float(item.get("quantity", 0))
            if sym and qty > 0:
                cost = float(item.get("cost_bases", [{}])[0].get("direct_cost_basis", 0))
                avg = cost / qty if qty else 0
                result[sym] = {"qty": qty, "avg_price": avg}
        return result

    def get_historical_prices(self, symbol: str, span: str = "month", interval: str = "day") -> list[dict]:
        symbol = _validated_symbol(symbol)
        if span not in _VALID_SPANS:
            raise ValueError(f"Invalid span {span!r}. Allowed: {_VALID_SPANS}")
        if interval not in _VALID_INTERVALS:
            raise ValueError(f"Invalid interval {interval!r}. Allowed: {_VALID_INTERVALS}")
        try:
            import robin_stocks.robinhood as rh

            if symbol in CRYPTO_SYMBOLS:
                data = rh.crypto.get_crypto_historicals(symbol, interval=interval, span=span)
            else:
                data = rh.stocks.get_stock_historicals(symbol, interval=interval, span=span)
            return data or []
        except Exception as exc:
            logger.warning("Could not fetch historicals for %s: %s", symbol, exc)
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

        order_id = str(uuid.uuid4())
        logger.info("[PAPER] SELL %s %.4f @ $%.4f", symbol, qty, price)
        return OrderResult(order_id=order_id, symbol=symbol, side="sell", quantity=qty, price=price, filled=True, paper=True)

    # ── Live order helpers ───────────────────────────────────────────────────

    def _live_buy(self, symbol: str, qty: float, price: float) -> OrderResult:
        import robin_stocks.robinhood as rh

        try:
            if symbol in CRYPTO_SYMBOLS:
                order = rh.orders.order_buy_crypto_by_quantity(symbol, qty)
            else:
                order = rh.orders.order_buy_fractional_by_quantity(
                    symbol, qty, account_number=self.account_number or None
                )

            order_id = order.get("id", str(uuid.uuid4()))
            filled = order.get("state") in ("filled", "partially_filled")
            logger.info("[LIVE] BUY %s %.4f @ $%.4f — id=%s", symbol, qty, price, order_id)
            return OrderResult(order_id=order_id, symbol=symbol, side="buy", quantity=qty, price=price, filled=filled, paper=False, raw=order)
        except Exception as exc:
            logger.error("Live buy failed for %s: %s", symbol, exc)
            raise

    def _live_sell(self, symbol: str, qty: float, price: float) -> OrderResult:
        import robin_stocks.robinhood as rh

        try:
            if symbol in CRYPTO_SYMBOLS:
                order = rh.orders.order_sell_crypto_by_quantity(symbol, qty)
            else:
                order = rh.orders.order_sell_fractional_by_quantity(
                    symbol, qty, account_number=self.account_number or None
                )

            order_id = order.get("id", str(uuid.uuid4()))
            filled = order.get("state") in ("filled", "partially_filled")
            logger.info("[LIVE] SELL %s %.4f @ $%.4f — id=%s", symbol, qty, price, order_id)
            return OrderResult(order_id=order_id, symbol=symbol, side="sell", quantity=qty, price=price, filled=filled, paper=False, raw=order)
        except Exception as exc:
            logger.error("Live sell failed for %s: %s", symbol, exc)
            raise
