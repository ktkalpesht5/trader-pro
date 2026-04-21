"""
macro_calendar.py
-----------------
Hardcoded macro event calendar for the overnight straddle strategy.
Skip dates are IST calendar dates on which we should NOT trade overnight.

──────────────────────────────────────────────────────────────────────────────
IST CONVERSION REFERENCE
──────────────────────────────────────────────────────────────────────────────
US observes EDT (UTC-4) from Mar 8 through Nov 1, 2026.
US observes EST (UTC-5) from Nov 1 2026 onward.

During EDT (Mar 8 – Nov 1):
  8:30 AM EDT = 18:00 IST  same calendar day  (NFP / CPI / PPI)
  2:00 PM EDT = 23:30 IST  same calendar day  (FOMC announcement)

During EST (Nov 1 – Dec 31):
  8:30 AM EST = 19:00 IST  same calendar day
  2:00 PM EST = 00:30 IST  NEXT calendar day  (FOMC announcement)

──────────────────────────────────────────────────────────────────────────────
WHY EACH EVENT TYPE IS SKIPPED
──────────────────────────────────────────────────────────────────────────────
FOMC (CRITICAL skip):
  Announcement at 2:00 PM EDT = 23:30 IST — fires 30–90 min before our
  00:00–00:30 IST overnight entry. BTC enters price discovery right as we
  are entering, or continues moving violently through our hold window.
  We skip the MORNING AFTER the announcement date (the IST date of our entry).

NFP / CPI / PPI (HIGH skip):
  8:30 AM EDT = 18:00 IST — fires ~6 hours before our midnight entry.
  Large moves driven by a single data print can establish strong directional
  momentum that persists overnight, neutralising the straddle thesis.
  We skip the MORNING AFTER the release date.

──────────────────────────────────────────────────────────────────────────────
2026 FOMC SCHEDULE (decision day → IST skip date)
──────────────────────────────────────────────────────────────────────────────
  Jan 29 → skip Jan 30  ✓ past (EST: 2PM EST = 00:30 IST Jan 30)
  Mar 19 → skip Mar 20  ✓ past (EDT kicks in Mar 8: 2PM EDT = 23:30 IST Mar 19 → skip Mar 20)
  Apr 29 → skip Apr 30  ← next  (2PM EDT = 23:30 IST Apr 29 → enter Apr 30 Thu at 00:00 = 30 min later)
  Jun 17 → skip Jun 18         (2PM EDT = 23:30 IST Jun 17)
  Jul 29 → skip Jul 30         (2PM EDT = 23:30 IST Jul 29)
  Sep 16 → skip Sep 17         (2PM EDT = 23:30 IST Sep 16)
  Oct 28 → skip Oct 29         (2PM EDT = 23:30 IST Oct 28; clocks fall back Nov 1)
  Dec  9 → skip Dec 10         (2PM EST = 00:30 IST Dec 10 — back to EST after Nov 1)

To update: add dates to SKIP_DATES with a reason string.
Run `python macro_calendar.py` to print the upcoming skip dates.
"""

from datetime import date, timedelta

# ── Skip dates (IST calendar date of the overnight hold to skip) ──────────────
# Key   = IST date on which we would ENTER the overnight trade (00:00–00:30 IST)
# Value = human-readable reason for skipping

SKIP_DATES: dict[date, str] = {

    # ── FOMC announcements (CRITICAL) ─────────────────────────────────────────
    # Decision day 2:00 PM EDT = 23:30 IST same day → skip the FOLLOWING morning

    date(2026, 4, 30): "FOMC rate decision (Apr 29 2PM EDT = 23:30 IST Apr 29; enter Apr 30 00:00 = 30 min post-FOMC)",
    date(2026, 6, 18): "FOMC rate decision + dot-plot (Jun 17 2PM EDT = 23:30 IST Jun 17)",
    date(2026, 7, 30): "FOMC rate decision (Jul 29 2PM EDT = 23:30 IST Jul 29)",
    date(2026, 9, 17): "FOMC rate decision (Sep 16 2PM EDT = 23:30 IST Sep 16)",
    date(2026, 10, 29): "FOMC rate decision (Oct 28 2PM EDT = 23:30 IST Oct 28)",
    date(2026, 12, 10): "FOMC rate decision (Dec 9 2PM EST = 00:30 IST Dec 10 — back to EST)",

    # ── NFP — Non-Farm Payrolls (HIGH) ────────────────────────────────────────
    # Released 8:30 AM EDT = 18:00 IST same day → skip the FOLLOWING morning
    # (strong directional momentum can persist 6+ hours into our window)

    date(2026, 5,  9): "US NFP (May 8 8:30AM EDT = 18:00 IST May 8; enter May 9 Sat 00:30)",
    date(2026, 6,  6): "US NFP (Jun 5 8:30AM EDT = 18:00 IST Jun 5; enter Jun 6 Sat 00:30)",
    date(2026, 7,  3): "US NFP (Jul 2 8:30AM EDT = 18:00 IST Jul 2; enter Jul 3 Fri 00:30)",

    # ── CPI — Consumer Price Index (HIGH) ─────────────────────────────────────
    # Released 8:30 AM EDT = 18:00 IST same day → skip the FOLLOWING morning

    date(2026, 5, 13): "US CPI Apr (May 12 8:30AM EDT = 18:00 IST May 12; enter May 13 Wed — skip)",
    date(2026, 6, 12): "US CPI May est. (Jun 11 8:30AM EDT = 18:00 IST Jun 11; enter Jun 12 Fri 00:30)",

    # ── PPI — Producer Price Index (MEDIUM-HIGH) ──────────────────────────────
    # Released 8:30 AM EDT = 18:00 IST same day, typically 1 day after CPI

    date(2026, 5, 14): "US PPI Apr (May 13 8:30AM EDT = 18:00 IST May 13; enter May 14 Thu 00:00)",
    date(2026, 6, 13): "US PPI May est. (Jun 12 8:30AM EDT = 18:00 IST Jun 12; enter Jun 13 Sat 00:30)",
}


def should_skip(trade_date: date) -> tuple[bool, str]:
    """
    Returns (True, reason) if the overnight trade on trade_date should be skipped.
    trade_date is the IST calendar date of the overnight hold (00:00–05:30 IST entry).
    """
    reason = SKIP_DATES.get(trade_date)
    if reason:
        return True, reason
    return False, ""


def upcoming_skip_dates(days_ahead: int = 60) -> list[tuple[date, str]]:
    """Returns all skip dates within the next N days."""
    today  = date.today()
    cutoff = today + timedelta(days=days_ahead)
    return [
        (d, r) for d, r in sorted(SKIP_DATES.items())
        if today <= d <= cutoff
    ]


if __name__ == "__main__":
    print("Upcoming macro skip dates (next 60 days):")
    print("─" * 70)
    skips = upcoming_skip_dates(60)
    if skips:
        for d, reason in skips:
            print(f"  {d.strftime('%a %d %b %Y')}  →  {reason}")
    else:
        print("  None")
