# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in Argus, please **do not open a public issue**.

Email: erick_zb04@outlook.com

Include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact (especially anything that could trigger unintended trades or expose credentials)

I'll respond within 48 hours and coordinate a fix before any public disclosure.

## Scope

Issues of highest concern:
- Anything that could cause unintended real-money trades
- Credential or API key exposure
- Authentication bypass on the web dashboard
- Remote code execution via the MCP bridge or dashboard endpoints

## Out of scope

- robin_stocks itself (report to that project)
- Robinhood's platform security
- Rate limiting issues with SEC EDGAR (SEC's responsibility)

## Security design notes

- **Secrets are never written to disk** — Robinhood password and all API keys are stored in the OS keychain (macOS Keychain / Windows Credential Manager / Linux Secret Service)
- **Paper trading by default** — `PAPER_TRADE=true` is the default; real-money mode requires a deliberate `.env` edit
- **Dashboard auth token** — set `DASHBOARD_TOKEN` in `.env` if you expose the dashboard beyond localhost
- **MFA sessions not cached** — `store_session=False` prevents Robinhood session tokens from persisting to disk
