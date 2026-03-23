# Straddle Bot — Setup & Deployment Guide

## What This Bot Does

| Time | Action |
|------|--------|
| Every hour (all day) | Posts a brief market snapshot to Telegram |
| 11 AM – 1 PM IST (every 15 min) | Full pre-trade checklist → TRADE / WAIT / PASS verdict |
| After you confirm entry (every 10 min) | HOLD / PARTIAL PROFIT / EXIT alerts |

---

## Step 1 — Create Your Telegram Bot

1. Open Telegram, search for `@BotFather`
2. Send `/newbot` and follow instructions
3. Copy the **API token** (looks like `123456789:ABCdef...`)
4. Create a **channel** (private or public, your choice)
5. Add your bot as an **admin** to the channel
6. Get your **channel ID**:
   - For public channels: just use `@your_channel_name`
   - For private channels: forward a message to `@userinfobot` to get the numeric ID (e.g. `-1001234567890`)

---

## Step 2 — Configure Environment Variables

Copy `.env.example` to `.env` and fill in:

```
TELEGRAM_BOT_TOKEN=your_token_from_botfather
TELEGRAM_CHANNEL_ID=@your_channel_or_numeric_id
```

The other variables have sensible defaults — only change if needed.

---

## Step 3A — Deploy on Railway (Recommended)

1. Push this repo to GitHub
2. Go to [railway.app](https://railway.app) → New Project → Deploy from GitHub repo
3. Select your repo
4. Go to **Variables** tab and add:
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHANNEL_ID`
5. Railway auto-detects `railway.toml` and starts the bot
6. Check **Logs** tab — you should see "Bot starting..." and a startup message in your channel

Railway free tier: 500 hours/month — sufficient for a bot that runs daily.
Upgrade to Starter ($5/month) for always-on operation.

---

## Step 3B — Deploy on Render

1. Push repo to GitHub
2. Go to [render.com](https://render.com) → New → Background Worker
3. Connect your repo
4. Add environment variables in the Render dashboard
5. Render uses `render.yaml` — it will auto-configure
6. Free tier on Render **spins down after inactivity** — use the Starter plan ($7/month) for a worker bot

---

## Step 3C — Run Locally (for testing)

```bash
cd straddle-bot
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your tokens
python src/bot.py
```

---

## Step 4 — Daily Workflow

### Normal day:
- Bot posts hourly snapshots automatically
- At 11 AM, entry window scans start — check your Telegram channel
- If bot says **TRADE**: enter manually on Delta Exchange, then confirm:
  ```
  /entry 601 MV-BTC-70600-200326
  ```
- Bot posts monitoring alerts every 10 minutes
- Close position manually when bot says EXIT or at 4:30 PM hard exit
- Confirm close:
  ```
  /exit
  ```

### Macro event day (FOMC, CPI, NFP):
```
/skip FOMC today — no trading
```
Bot will stop scanning and post a skip notice to the channel.

### On demand checks:
```
/status     — current market snapshot
/check      — full pre-trade checklist right now
/help       — command list
```

---

## Architecture

```
bot.py              Main orchestrator, scheduler, Telegram commands
├── delta_client.py     Delta Exchange API — fetches all raw data
├── analysis_engine.py  Checklist logic — encodes the playbook rules
└── formatter.py        Converts analysis output to Telegram messages
```

### How PCR, Max Pain, IV, RV are calculated:

**PCR** — Fetched directly from the options chain API. Sum of all put OI values / sum of all call OI values. No scraping needed.

**Max Pain** — Calculated from the full options chain. For each possible settlement strike, computes the total payout market makers would owe to all option buyers. The strike minimising that payout = Max Pain. This is the exact same calculation Delta Exchange uses for their Analytics tab display.

**Realised Volatility (RV)** — Computed from 24 hourly OHLCV candles using log returns, annualised to match the options market convention.

**Implied Volatility (IV)** — Back-calculated from the ATM straddle price using the standard straddle approximation formula. Also uses the `implied_volatility` field from the ticker API if available.

**IV-RV Spread** — Simply IV - RV. Positive = options expensive (seller edge). Negative = options cheap (buyer edge).

---

## Troubleshooting

**Bot not posting to channel:**
- Confirm bot is admin of the channel
- Check `TELEGRAM_CHANNEL_ID` format (use `@channelname` or numeric `-100xxxxxxxxxx`)

**"No straddles found":**
- Delta Exchange may have changed the contract naming format
- Check `delta_client.py` `get_today_straddles()` — the `expiry_date_str` pattern
- Run `python src/delta_client.py` to test connectivity

**PCR/Max Pain shows 0:**
- Options chain endpoint may have changed
- Delta Exchange sometimes gates options data — check their API docs
- Fallback: use `/skip` and input manually, then upgrade later

**Bot restarts losing position state:**
- This is intentional — positions are daily and reset is safe
- If this is a problem, add Redis or a simple SQLite file for persistence (v2 feature)

---

## Known Limitations & V2 Roadmap

| Feature | Current | V2 |
|---------|---------|-----|
| Max Pain calc | Approximate (by OI value) | Exact (by contract count × intrinsic value) |
| IV source | ATM straddle formula + API field | Full volatility surface fit |
| Position persistence | In-memory (resets on restart) | SQLite / Redis |
| Multiple positions | Not supported | Supported |
| Macro event detection | Manual `/skip` command | Auto-detect from economic calendar API |
| Trade journal | Manual Notion logging | Auto-post to Notion on entry/exit |

---

## Security Notes

- The bot token gives anyone who has it full control of your bot. Keep it secret.
- The bot only posts to your configured channel — it cannot execute trades.
- All trading decisions remain with you. The bot is advisory only.
- No personal financial data is stored — the bot only processes public market data.
