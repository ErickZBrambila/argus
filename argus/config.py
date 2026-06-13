"""Central configuration loaded from environment / .env file."""

from __future__ import annotations

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Anthropic
    anthropic_api_key: SecretStr = Field(..., alias="ANTHROPIC_API_KEY")

    # Robinhood
    robinhood_username: str = Field(..., alias="ROBINHOOD_USERNAME")
    robinhood_password: SecretStr = Field(..., alias="ROBINHOOD_PASSWORD")
    robinhood_mfa_secret: SecretStr = Field("", alias="ROBINHOOD_MFA_SECRET")

    # Trading mode
    paper_trade: bool = Field(True, alias="PAPER_TRADE")

    # Watchlist
    watchlist_raw: str = Field("AAPL,TSLA,NVDA,BTC,ETH", alias="WATCHLIST")

    # Risk parameters
    max_position_pct: float = Field(0.10, alias="MAX_POSITION_PCT")
    stop_loss_pct: float = Field(0.05, alias="STOP_LOSS_PCT")
    max_positions: int = Field(5, alias="MAX_POSITIONS")
    daily_drawdown_limit: float = Field(-0.05, alias="DAILY_DRAWDOWN_LIMIT")

    # Scan loop
    scan_interval_seconds: int = Field(300, alias="SCAN_INTERVAL_SECONDS")

    # Notifications — email
    notify_email: str = Field("", alias="NOTIFY_EMAIL")
    smtp_host: str = Field("smtp.gmail.com", alias="SMTP_HOST")
    smtp_port: int = Field(587, alias="SMTP_PORT")
    smtp_user: str = Field("", alias="SMTP_USER")
    smtp_password: SecretStr = Field("", alias="SMTP_PASSWORD")

    # Notifications — Twilio / SMS
    twilio_account_sid: str = Field("", alias="TWILIO_ACCOUNT_SID")
    twilio_auth_token: SecretStr = Field("", alias="TWILIO_AUTH_TOKEN")
    twilio_from: str = Field("", alias="TWILIO_FROM")
    twilio_to: str = Field("", alias="TWILIO_TO")

    # Notifications — Slack
    slack_bot_token: SecretStr = Field("", alias="SLACK_BOT_TOKEN")
    slack_channel: str = Field("#argus-alerts", alias="SLACK_CHANNEL")

    # Web dashboard — default localhost only; set to 0.0.0.0 only behind an authenticated proxy
    web_host: str = Field("127.0.0.1", alias="WEB_HOST")
    web_port: int = Field(8000, alias="WEB_PORT")

    # Database
    database_url: str = Field("sqlite:///argus.db", alias="DATABASE_URL")

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
