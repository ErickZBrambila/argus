"""Argus autopilot — main entry point."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from argus.engine.autopilot import Autopilot

_LOG_DIR = Path.home() / "Library" / "Logs" / "argus"
_LOG_FILE = _LOG_DIR / "argus.log"

logger = logging.getLogger(__name__)


def _setup_logging() -> None:
    from argus.dashboard.log_buffer import install as _install_log_buffer
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(_LOG_FILE, mode="a"),
        ],
    )
    try:
        import stat as _stat
        os.chmod(_LOG_FILE, _stat.S_IRUSR | _stat.S_IWUSR)
    except OSError:
        pass
    _install_log_buffer()


def _fix_tls_cert() -> None:
    """Pin REQUESTS_CA_BUNDLE to the certifi from this Python environment.

    Prevents stale DEFAULT_CA_BUNDLE_PATH values (e.g. from a deleted old
    venv) from breaking HTTPS connections to Robinhood.
    """
    try:
        import certifi
        cert_path = certifi.where()
        if cert_path and os.path.exists(cert_path):
            os.environ.setdefault("REQUESTS_CA_BUNDLE", cert_path)
            os.environ.setdefault("SSL_CERT_FILE", cert_path)
    except Exception:
        pass


def main() -> None:
    _fix_tls_cert()
    _setup_logging()
    pilot = Autopilot()
    pilot.run()


if __name__ == "__main__":
    main()
