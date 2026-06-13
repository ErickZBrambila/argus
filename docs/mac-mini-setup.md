# Mac Mini Setup Guide

Steps to migrate Argus from your laptop to a Mac Mini running Docker.

---

## 1. Mac Mini prerequisites

```bash
# Install Homebrew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Docker Desktop for Mac
brew install --cask docker

# Install Tailscale (remote access — no public internet exposure)
brew install --cask tailscale

# Open Tailscale and log in, then install on your iPhone too
open -a Tailscale
```

---

## 2. Clone Argus

```bash
git clone git@github.com:ErickZBrambila/argus.git ~/argus
cd ~/argus
```

---

## 3. Configure secrets

Secrets are injected as environment variables at runtime — never stored in the image.
Add these to `~/.zshrc` on the Mac Mini:

```zsh
export ANTHROPIC_API_KEY="your_anthropic_key"
export GEMINI_API_KEY="your_gemini_key"
export ROBINHOOD_PASSWORD="your_robinhood_password"
export ROBINHOOD_MFA_SECRET="your_mfa_secret"   # optional
export SMTP_PASSWORD=""                           # optional
export TWILIO_AUTH_TOKEN=""                       # optional
export SLACK_BOT_TOKEN=""                         # optional
```

Then reload:
```bash
source ~/.zshrc
```

---

## 4. Copy config

Transfer your `.env` from the laptop (non-secret config: watchlist, intervals, etc.):

```bash
# Run this on your laptop
scp ~/argus/.env mac-mini.local:~/argus/.env
```

Or copy the values manually from `.env.example`.

---

## 5. Build and start

```bash
cd ~/argus
docker compose up -d
```

Check it's running:
```bash
docker compose ps
docker compose logs -f
```

Web dashboard: `http://localhost:8000`

---

## 6. Access from your iPhone via Tailscale

Once Tailscale is running on both the Mac Mini and your iPhone:

1. Find the Mac Mini's Tailscale IP: open the Tailscale menu bar icon
2. On iPhone Safari: `http://100.x.x.x:8000`

No open ports, no public internet exposure.

---

## 7. Auto-start on boot

Docker Desktop for Mac restarts containers automatically if `restart: unless-stopped` is set
(already configured in `docker-compose.yml`). Just make sure Docker Desktop is set to
launch at login: Docker Desktop → Settings → General → "Start Docker Desktop when you log in".

---

## 8. Useful commands

```bash
docker compose up -d          # start in background
docker compose down           # stop
docker compose restart argus  # restart
docker compose logs -f        # follow logs
docker compose pull && docker compose up -d  # update to latest
```

---

## 9. Verify

- [ ] `docker compose ps` shows argus as healthy
- [ ] `curl http://localhost:8000/api/status` returns JSON
- [ ] Dashboard loads on iPhone via Tailscale IP
- [ ] Paper trade scan runs (check logs for "Next scan in Xs")
- [ ] Run `argus-stop` on the laptop to decommission it
