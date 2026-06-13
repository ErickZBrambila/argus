"""Keychain-backed secret store.

Wraps the `keyring` library so secrets are kept in the OS native store:
  - macOS  → Keychain
  - Windows → Credential Manager
  - Linux  → Secret Service (libsecret) or encrypted fallback file

All secrets are namespaced under the service name "argus".
Non-secret config (watchlist, scan interval, etc.) still lives in .env.
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

_SERVICE = "argus"

# Canonical names used as keychain account keys — these are the env-var names
# for the corresponding secrets.
SECRET_KEYS: tuple[str, ...] = (
    "ANTHROPIC_API_KEY",
    "ROBINHOOD_PASSWORD",
    "ROBINHOOD_MFA_SECRET",
    "SMTP_PASSWORD",
    "TWILIO_AUTH_TOKEN",
    "SLACK_BOT_TOKEN",
)


def _keyring():
    """Lazy import so the rest of the module loads without keyring installed."""
    try:
        import keyring
        return keyring
    except ImportError as exc:
        raise ImportError("Install keyring: pip install keyring") from exc


def get_secret(key: str) -> Optional[str]:
    """Return the secret stored under *key*, or None if not present."""
    try:
        value = _keyring().get_password(_SERVICE, key)
        return value or None
    except Exception as exc:
        logger.debug("Keychain read failed for %s: %s", key, exc)
        return None


def set_secret(key: str, value: str) -> None:
    """Store *value* under *key* in the OS keychain."""
    if not value:
        raise ValueError(f"Refusing to store empty value for {key}")
    _keyring().set_password(_SERVICE, key, value)
    logger.debug("Stored %s in keychain", key)


def delete_secret(key: str) -> None:
    """Remove *key* from the OS keychain (silently if not present)."""
    try:
        _keyring().delete_password(_SERVICE, key)
    except Exception:
        pass


def list_stored() -> dict[str, bool]:
    """Return a dict of {key: is_stored} for all known secret keys."""
    return {k: get_secret(k) is not None for k in SECRET_KEYS}


def all_secrets_present() -> bool:
    """True if the two mandatory secrets are in the keychain."""
    mandatory = {"ANTHROPIC_API_KEY", "ROBINHOOD_PASSWORD"}
    return all(get_secret(k) is not None for k in mandatory)
