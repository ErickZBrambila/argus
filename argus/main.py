"""Argus autopilot — main entry point."""

from __future__ import annotations

import logging
import os
import sys

from argus.engine.autopilot import Autopilot

logger = logging.getLogger(__name__)


def _setup_logging() -> None:
    from argus.dashboard.log_buffer import install as _install_log_buffer
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("argus.log", mode="a"),
        ],
    )
    # Restrict log file to owner-only
    try:
        import stat as _stat
        os.chmod("argus.log", _stat.S_IRUSR | _stat.S_IWUSR)
    except OSError:
        pass
    _install_log_buffer()


def main() -> None:
    _setup_logging()
    pilot = Autopilot()
    pilot.run()


if __name__ == "__main__":
    main()
