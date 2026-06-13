"""In-memory ring buffer that captures log records for dashboard display."""

from __future__ import annotations

import collections
import datetime
import logging
import threading
from typing import TypedDict

_lock   = threading.Lock()
_buffer: collections.deque = collections.deque(maxlen=500)

# Level → short label used by UIs for colour coding
_LEVEL_LABEL = {
    "DEBUG":    "DBG",
    "INFO":     "INF",
    "WARNING":  "WRN",
    "ERROR":    "ERR",
    "CRITICAL": "CRT",
}


class LogEntry(TypedDict):
    ts:    str    # HH:MM:SS
    level: str    # "INFO" | "WARNING" | "ERROR" …
    name:  str    # logger name (last segment)
    msg:   str


class _RingBufferHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = record.getMessage()
            if record.exc_info:
                msg += " — " + self.formatException(record.exc_info)
            entry: LogEntry = {
                "ts":    datetime.datetime.fromtimestamp(record.created).strftime("%H:%M:%S"),
                "level": record.levelname,
                "name":  record.name.split(".")[-1],
                "msg":   msg,
            }
            with _lock:
                _buffer.append(entry)
        except Exception:
            pass


def install(min_level: int = logging.DEBUG) -> None:
    """Attach the ring buffer handler to the root logger."""
    handler = _RingBufferHandler()
    handler.setLevel(min_level)
    logging.getLogger().addHandler(handler)


def get_recent(n: int = 100) -> list[LogEntry]:
    with _lock:
        return list(_buffer)[-n:]
