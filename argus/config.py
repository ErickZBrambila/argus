"""Central configuration.

Secret resolution priority (highest → lowest):
  1. Environment variables  — always wins (CI / Docker / manual override)
  2. OS keychain            — production default (run `argus-setup` to populate)
  3. .env file              — fallback / non-secret config
  4. Defaults

Run `argus-setup` once to store secrets in the OS keychain.
Only non-secret config (watchlist, scan interval, ports, etc.) needs to be in .env.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

logger = logging.getLogger(__name__)

# Keys that are fetched from the keychain (not the .env file)
_KEYCHAIN_SECRET_KEYS: frozenset[str] = frozenset({
    "ANTHROPIC_API_KEY",
    "GEMINI_API_KEY",
    "ROBINHOOD_PASSWORD",
    "ROBINHOOD_MFA_SECRET",
    "SMTP_PASSWORD",
    "TWILIO_AUTH_TOKEN",
    "SLACK_BOT_TOKEN",
})


class _KeychainSource(PydanticBaseSettingsSource):
    """Pydantic settings source that reads from the OS keychain."""

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
        alias = str(field.alias) if field.alias else field_name.upper()
        if alias not in _KEYCHAIN_SECRET_KEYS:
            return None, field_name, False
        try:
            from argus.secrets import get_secret
            value = get_secret(alias)
            return value, field_name, False
        except Exception as exc:
            logger.debug("Keychain lookup failed for %s: %s", alias, exc)
            return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        for field_name, field_info in self.settings_cls.model_fields.items():
            val, _, _ = self.get_field_value(field_info, field_name)
            if val is not None:
                data[field_name] = val
        return data


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        # Keep populate_by_name so both alias and field name work
        populate_by_name=True,
    )

    # ── Secrets (keychain-backed, env var override supported) ───────────────
    anthropic_api_key: SecretStr = Field(..., alias="ANTHROPIC_API_KEY")
    gemini_api_key: SecretStr = Field("", alias="GEMINI_API_KEY")
    robinhood_password: SecretStr = Field(..., alias="ROBINHOOD_PASSWORD")
    robinhood_mfa_secret: SecretStr = Field("", alias="ROBINHOOD_MFA_SECRET")
    smtp_password: SecretStr = Field("", alias="SMTP_PASSWORD")
    twilio_auth_token: SecretStr = Field("", alias="TWILIO_AUTH_TOKEN")
    slack_bot_token: SecretStr = Field("", alias="SLACK_BOT_TOKEN")

    # ── Non-secret config (kept in .env / env vars) ─────────────────────────
    robinhood_username: str = Field(..., alias="ROBINHOOD_USERNAME")
    paper_trade: bool = Field(True, alias="PAPER_TRADE")

    # Account numbers — set in .env (not secrets, just IDs)
    agentic_account_number: str = Field("", alias="AGENTIC_ACCOUNT_NUMBER")
    default_account_number: str = Field("", alias="DEFAULT_ACCOUNT_NUMBER")
    # Minimum risk level that requires approval on the default account ("medium" or "high")
    approval_threshold: str = Field("medium", alias="APPROVAL_THRESHOLD")
    watchlist_raw: str = Field("AAPL,TSLA,NVDA,BTC,ETH", alias="WATCHLIST")

    # Goal tracking — target equity to lift PDT restriction
    equity_goal: float = Field(25_000.0, alias="EQUITY_GOAL")

    # Monthly API spend budget in USD — shown in dashboard token section
    monthly_api_budget: float = Field(10.0, alias="MONTHLY_API_BUDGET")

    # Risk parameters
    max_position_pct: float = Field(0.10, alias="MAX_POSITION_PCT")
    stop_loss_pct: float = Field(0.05, alias="STOP_LOSS_PCT")
    max_positions: int = Field(5, alias="MAX_POSITIONS")
    daily_drawdown_limit: float = Field(-0.05, alias="DAILY_DRAWDOWN_LIMIT")

    # Model overrides
    claude_model: str = Field("claude-sonnet-4-6", alias="CLAUDE_MODEL")
    gemini_model: str = Field("gemini-2.5-flash", alias="GEMINI_MODEL")

    # Scan loop — adaptive intervals per market session
    scan_interval_seconds: int = Field(300, alias="SCAN_INTERVAL_SECONDS")
    interval_open:         int = Field(90,  alias="INTERVAL_OPEN")
    interval_premarket:    int = Field(180, alias="INTERVAL_PREMARKET")
    interval_afterhours:   int = Field(180, alias="INTERVAL_AFTERHOURS")
    interval_closed:       int = Field(300, alias="INTERVAL_CLOSED")

    # Notifications — email
    notify_email: str = Field("", alias="NOTIFY_EMAIL")
    smtp_host: str = Field("smtp.gmail.com", alias="SMTP_HOST")
    smtp_port: int = Field(587, alias="SMTP_PORT")
    smtp_user: str = Field("", alias="SMTP_USER")

    # Twilio
    twilio_account_sid: str = Field("", alias="TWILIO_ACCOUNT_SID")
    twilio_from: str = Field("", alias="TWILIO_FROM")
    twilio_to: str = Field("", alias="TWILIO_TO")

    # Slack
    slack_channel: str = Field("#argus-alerts", alias="SLACK_CHANNEL")

    # Discord webhook (paste URL from Server Settings → Integrations → Webhooks)
    discord_webhook_url: str = Field("", alias="DISCORD_WEBHOOK_URL")

    # ntfy.sh push notifications (e.g. https://ntfy.sh/your-topic-name)
    ntfy_url: str = Field("", alias="NTFY_URL")

    # Web dashboard — default localhost; set 0.0.0.0 only behind an auth proxy
    web_host: str = Field("127.0.0.1", alias="WEB_HOST")
    web_port: int = Field(8000, alias="WEB_PORT")
    # If non-empty, all mutating API endpoints require X-Argus-Token: <value>
    dashboard_token: SecretStr = Field("", alias="DASHBOARD_TOKEN")

    # Database
    database_url: str = Field("sqlite:///argus.db", alias="DATABASE_URL")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Priority: init > env vars > keychain > .env > defaults
        return (
            init_settings,
            env_settings,
            _KeychainSource(settings_cls),
            dotenv_settings,
        )

    # ── Validators ──────────────────────────────────────────────────────────

    @property
    def watchlist(self) -> list[str]:
        return [s.strip().upper() for s in self.watchlist_raw.split(",") if s.strip()]

    @field_validator("max_position_pct", "stop_loss_pct")
    @classmethod
    def pct_in_range(cls, v: float) -> float:
        if not 0 < v <= 0.25:
            raise ValueError("Percentage values must be between 0 (exclusive) and 0.25 (inclusive)")
        return v

    @field_validator("daily_drawdown_limit")
    @classmethod
    def drawdown_must_be_negative(cls, v: float) -> float:
        if v >= 0:
            raise ValueError("DAILY_DRAWDOWN_LIMIT must be negative (e.g. -0.05)")
        if v < -1.0:
            raise ValueError("DAILY_DRAWDOWN_LIMIT below -1.0 makes no sense")
        return v

    @field_validator("scan_interval_seconds")
    @classmethod
    def scan_interval_minimum(cls, v: int) -> int:
        if v < 30:
            raise ValueError("SCAN_INTERVAL_SECONDS must be >= 30 to avoid rate-limiting")
        return v

    @field_validator("max_positions")
    @classmethod
    def max_positions_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("MAX_POSITIONS must be >= 1")
        return v

    @field_validator("smtp_port", "web_port")
    @classmethod
    def port_in_range(cls, v: int) -> int:
        if not 1 <= v <= 65535:
            raise ValueError("Port must be between 1 and 65535")
        return v


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
