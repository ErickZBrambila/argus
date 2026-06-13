# Tomorrow's Plan — 2026-06-14

Pick up here when you get home. Work top to bottom.

---

## 1. Power & Sleep Settings (5 min)

Keep the laptop alive while Argus runs unattended.

- System Settings → Battery → Options
  - Enable **"Prevent automatic sleeping when display is off"** (while on power adapter)
- System Settings → Lock Screen
  - Set "Turn display off" to **Never** (when on power adapter)
- Plug in before leaving the laptop unattended

---

## 2. Verify Argus survived the night (5 min)

```bash
argus-status          # should return JSON with equity + P&L
argus-log             # check for errors overnight
open http://127.0.0.1:8000  # web dashboard
```

If it crashed, just `argus-restart`.

---

## 3. Phone Setup — Tailscale (10 min)

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

## 4. Docker Setup (20 min)

Get Argus running in Docker on the laptop — this is the exact same setup we'll use on the Mac Mini later, so it's worth validating now.

**Install Docker Desktop:**
```bash
brew install --cask docker
# Open Docker Desktop and wait for it to start
open -a Docker
```

**Build and run:**
```bash
cd ~/argus

# Export secrets (Docker reads from host env, not Keychain)
export ANTHROPIC_API_KEY=$(security find-generic-password -a ANTHROPIC_API_KEY -s argus -w)
export ROBINHOOD_PASSWORD=$(security find-generic-password -a ROBINHOOD_PASSWORD -s argus -w)
export GEMINI_API_KEY=$(security find-generic-password -a GEMINI_API_KEY -s argus -w)
export ROBINHOOD_MFA_SECRET=$(security find-generic-password -a ROBINHOOD_MFA_SECRET -s argus -w)

# Stop native Argus first (can't both use port 8000)
argus-stop

# Build and launch
docker compose up -d
docker compose logs -f
```

**Verify:**
```bash
curl http://localhost:8000/api/status
open http://localhost:8000
```

---

## 5. Review overnight performance (10 min)

- Check flashcards — were any trades attempted?
- Check token usage card — how much did Claude + Gemini cost overnight?
- Check signals — is the watchlist producing meaningful signals?
- Tweak watchlist in `.env` if needed (`WATCHLIST=AAPL,TSLA,NVDA,BTC,ETH`)

---

## Notes

- Market opens 9:30 AM ET — scan interval drops to 90s automatically
- If Docker works well on the laptop, Mac Mini migration is just `git clone` + `docker compose up -d`
- Tailscale is free for personal use (up to 3 devices)
