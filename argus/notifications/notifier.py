"""Notification dispatcher — email (SMTP), SMS (Twilio), Slack."""

from __future__ import annotations

import logging
import smtplib
from email.mime.text import MIMEText
from typing import Optional

from pydantic import SecretStr

logger = logging.getLogger(__name__)


def _secret(v: SecretStr | str | None) -> str:
    if isinstance(v, SecretStr):
        return v.get_secret_value()
    return v or ""


class Notifier:
    def __init__(
        self,
        notify_email: str = "",
        smtp_host: str = "smtp.gmail.com",
        smtp_port: int = 587,
        smtp_user: str = "",
        smtp_password: SecretStr | str = "",
        twilio_account_sid: str = "",
        twilio_auth_token: SecretStr | str = "",
        twilio_from: str = "",
        twilio_to: str = "",
        slack_bot_token: SecretStr | str = "",
        slack_channel: str = "#argus-alerts",
    ) -> None:
        self._email = notify_email
        self._smtp_host = smtp_host
        self._smtp_port = smtp_port
        self._smtp_user = smtp_user
        self._smtp_password = smtp_password

        self._twilio_sid = twilio_account_sid
        self._twilio_token = twilio_auth_token
        self._twilio_from = twilio_from
        self._twilio_to = twilio_to

        self._slack_token = slack_bot_token
        self._slack_channel = slack_channel

    def send(self, subject: str, body: str) -> None:
        self._try_email(subject, body)
        self._try_sms(f"{subject}: {body}")
        self._try_slack(f"*{subject}*\n{body}")

    # ── Email ────────────────────────────────────────────────────────────────

    def _try_email(self, subject: str, body: str) -> None:
        if not self._email or not self._smtp_user:
            return
        try:
            msg = MIMEText(body, "plain", "utf-8")
            msg["Subject"] = f"[Argus] {subject}"
            msg["From"] = self._smtp_user
            msg["To"] = self._email
            with smtplib.SMTP(self._smtp_host, self._smtp_port, timeout=15) as server:
                server.ehlo()
                server.starttls()
                server.login(self._smtp_user, _secret(self._smtp_password))
                server.sendmail(self._smtp_user, [self._email], msg.as_string())
            logger.info("Email sent to %s", self._email)
        except Exception as exc:
            logger.warning("Email failed: %s", exc)

    # ── SMS via Twilio ───────────────────────────────────────────────────────

    def _try_sms(self, message: str) -> None:
        if not self._twilio_sid or not _secret(self._twilio_token):
            return
        try:
            from twilio.rest import Client as TwilioClient

            client = TwilioClient(self._twilio_sid, _secret(self._twilio_token))
            client.messages.create(
                body=message[:1600],
                from_=self._twilio_from,
                to=self._twilio_to,
            )
            logger.info("SMS sent to %s", self._twilio_to)
        except Exception as exc:
            logger.warning("SMS failed: %s", exc)

    # ── Slack ────────────────────────────────────────────────────────────────

    def _try_slack(self, message: str) -> None:
        if not _secret(self._slack_token):
            return
        try:
            from slack_sdk import WebClient as SlackClient

            client = SlackClient(token=_secret(self._slack_token))
            client.chat_postMessage(channel=self._slack_channel, text=message)
            logger.info("Slack message sent to %s", self._slack_channel)
        except Exception as exc:
            logger.warning("Slack failed: %s", exc)
