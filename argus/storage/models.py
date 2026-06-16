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
    text,
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
    symbol = Column(String(20), nullable=False, index=True)
    account_label = Column(String(50), nullable=False, default="main")
    quantity = Column(Float, nullable=False)
    entry_price = Column(Float, nullable=False)
    current_price = Column(Float, default=0.0)
    stop_loss_price = Column(Float, nullable=False)
    unrealized_pnl = Column(Float, default=0.0)
    unrealized_pnl_pct = Column(Float, default=0.0)
    opened_at = Column(DateTime(timezone=True), default=_utcnow)
    updated_at = Column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)

    __table_args__ = (UniqueConstraint("symbol", "account_label", name="uq_position_symbol_account"),)


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


class HistoricalPrice(Base):
    __tablename__ = "historical_prices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=True)

    __table_args__ = (UniqueConstraint("symbol", "timestamp", name="uq_symbol_timestamp"),)


class DailyStats(Base):
    __tablename__ = "daily_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False, unique=True, index=True)
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


class Watchlist(Base):
    """Persistent user watchlist symbols."""
    __tablename__ = "watchlist"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, unique=True, index=True)
    order = Column(Integer, default=0)
    exit_only = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), default=_utcnow)


# ── Engine / session factory ─────────────────────────────────────────────────

_engine = None
_SessionLocal = None


def _apply_migrations(engine) -> None:
    """Apply incremental schema migrations for existing databases."""
    with engine.connect() as conn:
        # --- positions: add account_label column ---
        cols = {r[1] for r in conn.execute(text("PRAGMA table_info(positions)")).fetchall()}
        if "account_label" not in cols:
            conn.execute(text(
                "ALTER TABLE positions ADD COLUMN account_label VARCHAR(50) NOT NULL DEFAULT 'main'"
            ))
            # Rebuild table to replace the old UNIQUE(symbol) with UNIQUE(symbol, account_label)
            conn.execute(text("""
                CREATE TABLE positions_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol VARCHAR(20) NOT NULL,
                    account_label VARCHAR(50) NOT NULL DEFAULT 'main',
                    quantity FLOAT NOT NULL,
                    entry_price FLOAT NOT NULL,
                    current_price FLOAT DEFAULT 0.0,
                    stop_loss_price FLOAT NOT NULL,
                    unrealized_pnl FLOAT DEFAULT 0.0,
                    unrealized_pnl_pct FLOAT DEFAULT 0.0,
                    opened_at DATETIME,
                    updated_at DATETIME,
                    CONSTRAINT uq_position_symbol_account UNIQUE (symbol, account_label)
                )
            """))
            conn.execute(text("""
                INSERT INTO positions_new
                SELECT id, symbol, 'main', quantity, entry_price, current_price,
                       stop_loss_price, unrealized_pnl, unrealized_pnl_pct, opened_at, updated_at
                FROM positions
            """))
            conn.execute(text("DROP TABLE positions"))
            conn.execute(text("ALTER TABLE positions_new RENAME TO positions"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_positions_symbol ON positions (symbol)"))
        # --- watchlist: add exit_only column ---
        wl_cols = {r[1] for r in conn.execute(text("PRAGMA table_info(watchlist)")).fetchall()}
        if "exit_only" not in wl_cols:
            conn.execute(text("ALTER TABLE watchlist ADD COLUMN exit_only BOOLEAN NOT NULL DEFAULT 0"))
        conn.commit()


def init_db(url: str = "sqlite:///argus.db") -> None:
    global _engine, _SessionLocal
    connect_args: dict = {}
    if url.startswith("sqlite"):
        connect_args = {"check_same_thread": False}
        if url.startswith("sqlite:////") or url.startswith("sqlite:///"):
            db_path = url.replace("sqlite:///", "", 1).replace("sqlite:////", "/", 1)
            if db_path and not db_path.startswith(":"):
                db_dir = os.path.dirname(os.path.abspath(db_path))
                os.makedirs(db_dir, exist_ok=True)

    _engine = create_engine(url, connect_args=connect_args, echo=False)
    Base.metadata.create_all(_engine)
    if url.startswith("sqlite"):
        _apply_migrations(_engine)
        # Enable WAL mode for concurrent read/write between main loop and FastAPI thread
        with _engine.connect() as conn:
            conn.execute(text("PRAGMA journal_mode=WAL"))
            conn.commit()
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

def upsert_position(
    session: Session,
    symbol: str,
    quantity: float,
    entry_price: float,
    stop_loss_price: float,
    account_label: str = "main",
) -> Position:
    pos = session.query(Position).filter_by(symbol=symbol, account_label=account_label).first()
    if pos is None:
        pos = Position(
            symbol=symbol,
            account_label=account_label,
            quantity=quantity,
            entry_price=entry_price,
            stop_loss_price=stop_loss_price,
        )
        session.add(pos)
    else:
        pos.quantity = quantity
        pos.entry_price = entry_price
        pos.stop_loss_price = stop_loss_price
    return pos


def delete_position(session: Session, symbol: str, account_label: str = "main") -> None:
    session.query(Position).filter_by(symbol=symbol, account_label=account_label).delete()


def get_or_create_daily_stats(session: Session, date_val: datetime.date, starting_equity: float) -> DailyStats:
    stats = session.query(DailyStats).filter_by(date=date_val).first()
    if stats is None:
        stats = DailyStats(date=date_val, starting_equity=starting_equity, current_equity=starting_equity)
        session.add(stats)
    return stats


def increment_day_trades(session: Session) -> None:
    today = datetime.date.today()
    stats = session.query(DailyStats).filter_by(date=today).first()
    if stats:
        stats.day_trades = (stats.day_trades or 0) + 1


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
    if row is None:
        row = AccountDailyStats(date=date_val, account_label=account_label, kill_switch_triggered=True)
        session.add(row)
    elif not row.kill_switch_triggered:
        row.kill_switch_triggered = True


def get_cached_historicals(session: Session, symbol: str, limit: int = 500) -> list[dict]:
    rows = (
        session.query(HistoricalPrice)
        .filter_by(symbol=symbol)
        .order_by(HistoricalPrice.timestamp.asc())
        .limit(limit)
        .all()
    )
    return [
        {
            "begins_at": r.timestamp.isoformat(),
            "open_price": str(r.open),
            "high_price": str(r.high),
            "low_price": str(r.low),
            "close_price": str(r.close),
            "volume": int(r.volume or 0),
            "symbol": r.symbol,
        }
        for r in rows
    ]


def save_historicals(session: Session, symbol: str, data: list[dict]) -> None:
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert
    for d in data:
        try:
            ts_str = d.get("begins_at")
            if not ts_str:
                continue
            # Handle both ISO strings and date strings
            if "T" in ts_str:
                ts = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            else:
                ts = datetime.datetime.strptime(ts_str, "%Y-%m-%d").replace(tzinfo=_UTC)
            
            stmt = sqlite_insert(HistoricalPrice).values(
                symbol=symbol,
                timestamp=ts,
                open=float(d.get("open_price") or d.get("open", 0)),
                high=float(d.get("high_price") or d.get("high", 0)),
                low=float(d.get("low_price") or d.get("low", 0)),
                close=float(d.get("close_price") or d.get("close", 0)),
                volume=float(d.get("volume") or 0),
            ).on_conflict_do_nothing()
            session.execute(stmt)
        except Exception as e:
            logger.debug("Failed to save historical candle for %s: %s", symbol, e)


def get_db_watchlist(session: Session) -> list[str]:
    rows = session.query(Watchlist).order_by(Watchlist.order.asc(), Watchlist.symbol.asc()).all()
    return [r.symbol for r in rows]


def add_to_db_watchlist(session: Session, symbol: str) -> None:
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert
    stmt = sqlite_insert(Watchlist).values(symbol=symbol).on_conflict_do_nothing()
    session.execute(stmt)


def remove_from_db_watchlist(session: Session, symbol: str) -> None:
    session.query(Watchlist).filter_by(symbol=symbol).delete()


def set_exit_only(session: Session, symbol: str, value: bool) -> None:
    row = session.query(Watchlist).filter_by(symbol=symbol).first()
    if row:
        row.exit_only = value


def get_exit_only_symbols(session: Session) -> set[str]:
    rows = session.query(Watchlist).filter_by(exit_only=True).all()
    return {r.symbol for r in rows}
