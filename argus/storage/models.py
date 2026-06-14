"""SQLAlchemy models and DB helpers."""

from __future__ import annotations

import datetime
import os
import stat
from contextlib import contextmanager
from typing import Generator, Optional

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

_UTC = datetime.timezone.utc


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(_UTC)


class Base(DeclarativeBase):
    pass


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, index=True)
    side = Column(String(4), nullable=False)
    quantity = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    total_value = Column(Float, nullable=False)
    paper = Column(Boolean, default=True)
    order_id = Column(String(100), nullable=True)
    reasoning = Column(Text, nullable=True)          # capped at 4000 chars before insert
    created_at = Column(DateTime(timezone=True), default=_utcnow)


class Position(Base):
    __tablename__ = "positions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, unique=True, index=True)
    quantity = Column(Float, nullable=False)
    entry_price = Column(Float, nullable=False)
    current_price = Column(Float, default=0.0)
    stop_loss_price = Column(Float, nullable=False)
    unrealized_pnl = Column(Float, default=0.0)
    unrealized_pnl_pct = Column(Float, default=0.0)
    opened_at = Column(DateTime(timezone=True), default=_utcnow)
    updated_at = Column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)


class Signal(Base):
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, index=True)
    rsi = Column(Float, nullable=True)
    macd = Column(Float, nullable=True)
    macd_signal = Column(Float, nullable=True)
    macd_hist = Column(Float, nullable=True)
    bb_upper = Column(Float, nullable=True)
    bb_mid = Column(Float, nullable=True)
    bb_lower = Column(Float, nullable=True)
    sma_20 = Column(Float, nullable=True)
    ema_50 = Column(Float, nullable=True)
    price = Column(Float, nullable=True)
    volume = Column(Float, nullable=True)
    composite_signal = Column(String(10), nullable=True)
    created_at = Column(DateTime(timezone=True), default=_utcnow)


class DailyStats(Base):
    __tablename__ = "daily_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False, unique=True, index=True)   # use SQLAlchemy Date type
    starting_equity = Column(Float, default=0.0)
    current_equity = Column(Float, default=0.0)
    realized_pnl = Column(Float, default=0.0)
    trade_count = Column(Integer, default=0)
    day_trades = Column(Integer, default=0)
    kill_switch_triggered = Column(Boolean, default=False)


class AccountDailyStats(Base):
    """Per-account starting equity and kill switch state — survives restarts."""
    __tablename__ = "account_daily_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False, index=True)
    account_label = Column(String(50), nullable=False)
    starting_equity = Column(Float, default=0.0)
    kill_switch_triggered = Column(Boolean, default=False)

    __table_args__ = (UniqueConstraint("date", "account_label", name="uq_account_daily"),)


# ── Engine / session factory ─────────────────────────────────────────────────

_engine = None
_SessionLocal = None


def init_db(url: str = "sqlite:///argus.db") -> None:
    global _engine, _SessionLocal
    connect_args: dict = {}
    if url.startswith("sqlite"):
        # Single connection for SQLite — avoids concurrency issues
        connect_args = {"check_same_thread": False}
        # Ensure the file directory exists and restrict permissions
        if url.startswith("sqlite:////") or url.startswith("sqlite:///"):
            db_path = url.replace("sqlite:///", "", 1).replace("sqlite:////", "/", 1)
            if db_path and not db_path.startswith(":"):
                db_dir = os.path.dirname(os.path.abspath(db_path))
                os.makedirs(db_dir, exist_ok=True)

    _engine = create_engine(url, connect_args=connect_args, echo=False)
    Base.metadata.create_all(_engine)
    _SessionLocal = sessionmaker(bind=_engine, autocommit=False, autoflush=False)

    # Restrict DB file permissions to owner-only (0600)
    if url.startswith("sqlite:///") and not url.endswith(":memory:"):
        db_path = url.replace("sqlite:///", "", 1)
        if db_path and os.path.exists(db_path):
            try:
                os.chmod(db_path, stat.S_IRUSR | stat.S_IWUSR)
            except OSError:
                pass


@contextmanager
def get_session() -> Generator[Session, None, None]:
    if _SessionLocal is None:
        raise RuntimeError("call init_db() before get_session()")
    session: Session = _SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ── Convenience queries ──────────────────────────────────────────────────────

def upsert_position(session: Session, symbol: str, quantity: float, entry_price: float, stop_loss_price: float) -> Position:
    pos = session.query(Position).filter_by(symbol=symbol).first()
    if pos is None:
        pos = Position(symbol=symbol, quantity=quantity, entry_price=entry_price, stop_loss_price=stop_loss_price)
        session.add(pos)
    else:
        pos.quantity = quantity
        pos.entry_price = entry_price
        pos.stop_loss_price = stop_loss_price
    return pos


def delete_position(session: Session, symbol: str) -> None:
    session.query(Position).filter_by(symbol=symbol).delete()


def get_or_create_daily_stats(session: Session, date_val: datetime.date, starting_equity: float) -> DailyStats:
    stats = session.query(DailyStats).filter_by(date=date_val).first()
    if stats is None:
        stats = DailyStats(date=date_val, starting_equity=starting_equity, current_equity=starting_equity)
        session.add(stats)
    return stats


def count_day_trades_last_5_days(session: Session) -> int:
    cutoff = datetime.date.today() - datetime.timedelta(days=5)
    result = (
        session.query(func.sum(DailyStats.day_trades))
        .filter(DailyStats.date >= cutoff)
        .scalar()
    )
    return int(result or 0)


def get_or_create_account_daily_stats(
    session: Session,
    date_val: datetime.date,
    account_label: str,
    starting_equity: float,
) -> AccountDailyStats:
    row = (
        session.query(AccountDailyStats)
        .filter_by(date=date_val, account_label=account_label)
        .first()
    )
    if row is None:
        row = AccountDailyStats(
            date=date_val,
            account_label=account_label,
            starting_equity=starting_equity,
        )
        session.add(row)
    return row


def mark_account_kill_switch(
    session: Session, date_val: datetime.date, account_label: str
) -> None:
    row = (
        session.query(AccountDailyStats)
        .filter_by(date=date_val, account_label=account_label)
        .first()
    )
    if row and not row.kill_switch_triggered:
        row.kill_switch_triggered = True
