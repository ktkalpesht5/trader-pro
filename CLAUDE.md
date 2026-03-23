# CLAUDE.md — BTC Straddle Bot: Complete Developer Context

> This file is the single source of truth for Claude Code working on this codebase. Read it fully before making any changes.

---

# 1. What This Project Is

An **automated trading analysis bot** for shorting BTC daily straddle options on Delta Exchange India. It does NOT execute trades — it analyses market data and posts actionable alerts to a Telegram channel. The human trader makes all final decisions and executes manually.

**The bot must never execute trades. It is advisory only.**

---

# 2. Project Structure

```
straddle-bot/
├── src/
│   ├── bot.py              # Main entry point. Scheduler + Telegram command handlers
│   ├── delta_client.py     # Async HTTP client for Delta Exchange India public API
│   ├── analysis_engine.py  # All trading logic. Encodes the playbook rules as Python
│   └── formatter.py        # Converts analysis output to Telegram MarkdownV2 messages
├── requirements.txt
├── railway.toml            # Railway deployment config (primary)
├── render.yaml             # Render deployment config (alternative)
├── .env.example
└── README.md
```

Dependency flow (no circular imports):
```
bot.py → delta_client.py → (raw data)
       → analysis_engine.py → (checklist logic)
       → formatter.py → (Telegram messages)
```

---

# 3. The Trading Strategy (Read Before Touching analysis_engine.py)

## Entry window
12:00 PM – 1:00 PM IST only. Expiry is 5:30 PM IST daily. This gives exactly 4–5.5 hours of the steepest theta decay curve. Hard exit: 4:30 PM IST always.

## Two-section checklist

**Section A — Hard gates (any fail = PASS immediately):**
- A1: Time to expiry 4–5.5 hours
- A2: BTC 4hr move < $800
- A3: 24h BTC range < $2,500
- A4: No macro events (manual check — bot always passes this, human uses /skip)
- A5: At least one straddle with |delta| < 0.15 exists in chain

**Section B — Quality checks (6+/8 = TRADE full size, 5/8 = TRADE half size, <5 = WAIT):**
- B1: IV < 55%
- B2: IV-RV spread > -10
- B3: |Delta| < 0.15 — **NON-NEGOTIABLE — individual veto**
- B4: Theta/price > 2.5%/hr — **NON-NEGOTIABLE — individual veto**
- B5: Vega < 18
- B6: Strike volume > $1M
- B7: BTC 4hr move < $400 (flat)
- B8: Max Pain within $2,000 of BTC spot

**B3 and B4 individually veto the trade regardless of all other scores.**

## Position monitoring exit triggers (every 10 minutes)
1. TP: straddle ≤ 50% of entry
2. SL: straddle ≥ 170% of entry
3. Delta breach: |delta| > 0.45
4. BTC moved > $700 from strike
5. Straddle bounced > 15% from entry
6. Hard time: 4:30 PM IST — unconditional
7. 4:15 PM IST — 15-minute warning

## Greek thresholds and why
- **Delta < 0.15**: Entry must be directionally neutral. High delta = directional bet disguised as volatility trade.
- **Gamma < 0.00065**: High gamma with >5hrs left = accelerating losses on any BTC move.
- **Theta/price > 2.5%/hr**: Minimum profit rate. Below this, decay isn't fast enough to justify risk.
- **Vega < 18**: If IV spikes 10% and vega=18, straddle rises $180 against you.

## Why Max Pain matters
Max Pain = strike where option buyers collectively lose the most. Market makers hedge toward it, creating gravitational pull on BTC at settlement. Gap > $2,000 = no pin gravity = short straddle thesis (BTC pins near strike) breaks down.

---

# 4. API Details

## Delta Exchange India Public API
Base URL: `https://api.india.delta.exchange`  
No authentication required for market data.

### Endpoints
- `GET /v2/tickers/{symbol}` — mark price, OI, greeks (`result.greeks.{delta,gamma,theta,vega}`), IV (`result.implied_volatility` as decimal e.g. 0.38)
- `GET /v2/products` — all contracts. Params: `contract_types=move_options` for straddles, `put_options,call_options` for options chain
- `GET /v2/history/candles` — OHLCV. Params: `symbol`, `resolution` (seconds), `start`/`end` (unix timestamps)

### Symbol formats
- Straddles: `MV-BTC-{STRIKE}-{DDMMYY}` e.g. `MV-BTC-70600-200326`
- Calls: `C-BTC-{STRIKE}-{DDMMYY}`
- Puts: `P-BTC-{STRIKE}-{DDMMYY}`

### Known quirks
- IV field is sometimes null — always fall back to the straddle price formula
- `oi_value` (dollar value) more reliable than `oi` (contracts) for PCR calc
- After 5:30 PM IST, today's contracts disappear from the API

---

# 5. Calculated Metrics

### PCR
`sum(put.oi_value) / sum(call.oi_value)` across all today's options

### Max Pain
For each candidate strike S: sum of (intrinsic value × contract count) for all calls where strike < S, plus all puts where strike > S. The S that minimises this sum = Max Pain.

### Realised Volatility (RV)
Log returns on 24 hourly closes, annualised: `stdev(log_returns) * sqrt(365*24) * 100`

### Implied Volatility (IV)
Primary: ticker API `implied_volatility` × 100.  
Fallback: `(straddle_price / (2 * spot * sqrt(T/(2*pi)))) * 100` where T = hours_to_expiry / (365*24)

### IV-RV Spread
`IV - RV`. Positive = seller's edge. Negative = buyer's edge.  
Thresholds: >+15 ideal, 0 to +15 acceptable, -10 to 0 caution, <-10 avoid short.

---

# 6. State and Scheduling

## BotState (in-memory, resets on restart — intentional for daily trading)
- `skip_today` / `skip_reason`: set by /skip command
- `position_active` + entry details: set by /entry command
- `last_snapshot`: cached for /status command

## Scheduler (APScheduler, IST timezone)
| Job | Trigger | Condition |
|-----|---------|-----------|
| `job_hourly_scan` | CronTrigger :00 every hour | Skips if in entry window or skip_today |
| `job_entry_window_scan` | CronTrigger :00,:15,:30,:45 hours 11-12 | Only 11 AM–1 PM IST, skips if position active |
| `job_monitor_position` | IntervalTrigger every 10 min | Only if state.position_active = True |

---

# 7. Commands
| Command | Effect |
|---------|--------|
| `/status` | Fresh market snapshot |
| `/check` | Full pre-trade checklist on demand |
| `/entry PRICE SYMBOL` | Log entry, activate monitoring. E.g. `/entry 601 MV-BTC-70600-200326` |
| `/exit` | Clear position, stop monitoring |
| `/skip REASON` | Skip today, bot goes silent |
| `/resume` | Re-enable after skip |

TP/SL auto-calculated on /entry: TP = entry × 0.50 (weekday) or × 0.40 (weekend). SL = entry × 1.70.

---

# 8. Telegram MarkdownV2 Rules
- Always escape with `_escape()` before inserting any dynamic string
- Escape these: `_ * [ ] ( ) ~ \` > # + - = | { } . !`
- Dollar signs in numbers must be escaped: `\$70,600`
- Fallback in `send_message()` strips all markdown if MarkdownV2 fails

---

# 9. Known Issues
1. **`asyncio.coroutine` deprecation**: In `fetch_full_snapshot()`, `hours_to_expiry()` is called as an async task but it's synchronous. Fix: call it directly after the gather.
2. **`oi_value` sometimes missing**: Fall back to `oi * mark_price` if `oi_value` is 0.
3. **No straddles found**: Verify it's before 5:30 PM IST and expiry format `strftime("%d%m%y")` matches Delta's naming.

---

# 10. Invariants (Never Break)
1. Bot never executes trades
2. Hard exit at 4:30 PM IST is unconditional — no override
3. `monitor_position()` must never return `AVERAGE_DOWN`
4. B3 (delta) and B4 (theta ratio) individually veto regardless of other scores
5. All delta thresholds use absolute value |delta|
6. IST timezone everywhere — all scheduling uses `pytz.timezone('Asia/Kolkata')`
7. `analysis_engine.py` must never import from `bot.py`
8. `formatter.py` must never call the API

---

# 11. File Responsibilities
| File | Only responsible for |
|------|---------------------|
| `delta_client.py` | API fetching. No business logic. |
| `analysis_engine.py` | Trading rules. Pure functions, no I/O. |
| `formatter.py` | String formatting. No logic. |
| `bot.py` | Orchestration, scheduling, commands. |

---

# 12. Real Trade History (Empirical Basis for Thresholds)
| Trade | Date | Entry | Result |
|-------|------|-------|--------|
| #1 | Mar 18 2026, MV-BTC-74000-180326 | $571 | +$1.06 (+11.46%) |
| #2 | Mar 20 2026, MV-BTC-70600-200326 | $601 | +~$2.30 est. |

All thresholds map to real observed scenarios. Full journal in Notion.
