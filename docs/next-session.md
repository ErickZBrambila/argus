# Session Plan — ongoing

Work top to bottom.

---

## ✅ Done (2026-06-13)

- Fullscreen layout bug fixed (tab panes now span full grid width)
- Performance Analytics tab (win rate, streak, by-symbol, confidence accuracy)
- Promote to Agentic button (sell on Default, re-buy on Agentic)
- $25K PDT goal tracker per account (web + terminal)
- Token Usage moved to top of dashboard
- Docker support (`Dockerfile`, `docker-compose.yml`)
- Headless mode (`ARGUS_NO_TERMINAL=1`)
- Branding assets (logo, favicon, banner)
- Terminal countdown fix (`_LiveRenderable` wrapper)
- `argus-restart` shell alias

## ✅ Done (2026-06-14)

- Session persistence across restarts:
  - Daily P&L baseline (starting equity) survives restart — drawdown kill switch recalibrates correctly
  - Kill switch state persists — restarting after a drawdown breach doesn't re-enable trading

---

## 1. Power & Sleep Settings (5 min)

Keep the laptop alive while Argus runs unattended.

- System Settings → Battery → Options
  - Enable **"Prevent automatic sleeping when display is off"** (while on power adapter)
- System Settings → Lock Screen
  - Set "Turn display off" to **Never** (when on power adapter)
- Plug in before leaving the laptop unattended

---

## 2. Phone Setup — Tailscale (10 min)

Access the dashboard from your iPhone without exposing anything to the internet.

**On the laptop:**
```bash
brew install tailscale
sudo tailscaled &
tailscale up           # opens browser to log in — use GitHub or Google
```

**On iPhone:**
- App Store → install **Tailscale**
- Log in with the same account

**Test:**
- Find laptop's Tailscale IP: click Tailscale menu bar icon
- iPhone Safari → `http://100.x.x.x:8000`
- You should see the full Argus dashboard

---

## 3. Docker Setup (20 min)

Get Argus running in Docker on the laptop — same setup we'll use on the Mac Mini.

**Install Docker Desktop:**
```bash
brew install --cask docker
open -a Docker   # wait for it to start
```

**Build and run:**
```bash
cd ~/argus

# Export secrets (Docker reads from host env, not Keychain)
export ANTHROPIC_API_KEY=$(security find-generic-password -a ANTHROPIC_API_KEY -s argus -w)
export ROBINHOOD_PASSWORD=$(security find-generic-password -a ROBINHOOD_PASSWORD -s argus -w)
export GEMINI_API_KEY=$(security find-generic-password -a GEMINI_API_KEY -s argus -w)
export ROBINHOOD_MFA_SECRET=$(security find-generic-password -a ROBINHOOD_MFA_SECRET -s argus -w)

argus-stop   # can't both use port 8000

docker compose up -d
docker compose logs -f
```

**Verify:**
```bash
curl http://localhost:8000/api/status
open http://localhost:8000
```

---

## 4. Review performance (10 min)

- Check flashcards — any trades attempted?
- Check token usage — how much did Claude + Gemini cost?
- Check signals — is the watchlist producing meaningful signals?
- Tweak watchlist in `.env` if needed (`WATCHLIST=AAPL,TSLA,NVDA,BTC,ETH`)

---

## Notes

- Market opens 9:30 AM ET — scan interval drops to 90s automatically
- Kill switch reset (if needed): `UPDATE account_daily_stats SET kill_switch_triggered = 0 WHERE date = date('now');`
- If Docker works well on the laptop, Mac Mini migration is just `git clone` + `docker compose up -d`
- Tailscale is free for personal use (up to 3 devices)
