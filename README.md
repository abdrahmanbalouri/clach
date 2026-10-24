# ⚔️ CR Snipe

Real-time Clash Royale snipe dashboard.

## How it works

1. **Discovery (your exact ranked range)** — every 5 min the app builds the pool
   of players matchmaking can pair you with: YOUR recent PoL opponents + their
   recent PoL opponents, filtered to **your league** and **PoL rating within
   ±RANGE_WINDOW** of yours (default 250). Pool auto-evicts inactive players to
   stay fresh (max 80). Plus: your recent opponents of any mode, plus manual tags.
2. **Tracking** — every `POLL_INTERVAL` seconds (default 4s) it polls every
   target's battle log.
3. **Snipe window + confidence %** — the moment a target's new ranked battle
   appears in their log (= their match just ended), they go **HOT** for
   `HOT_WINDOW` seconds (default 60s): big red "DKHOL DABA" banner, sound
   alert, live countdown, and a **confidence %** that they are in the queue
   right now. The score decays with time since match end and is boosted for
   players who chain-queue matches (median gap between their games).
   ⚠️ No tool can know the queue with 100% certainty before the match —
   Supercell does not expose it. What we CAN do:
4. **Post-match confirmation (100%)** — when the target's next battle appears,
   the app checks the opponent tag: if it was YOU → green "✅ SNIPED" badge;
   if they matched someone else while you were hunting → "❌ missed" badge.
5. **Counter hint** — their last deck is shown instantly, plus a one-line
   "best of your decks vs theirs" hint (matrix in `counters.json`).

## Setup

```bash
venv/bin/pip install -r requirements.txt
```

`.env`:

```
CLASH_ROYALE_API_KEY=your_key
MY_TAG=#YOURTAG
# optional tuning:
# POLL_INTERVAL=4
# HOT_WINDOW=60
# MAX_AUTO_TARGETS=12
```

> ⚠️ The API key only works from whitelisted IPs. If you see a 403 banner, the
> dashboard shows your current public IP — add it at
> <https://developer.clashroyale.com> → your key → allowed IPs.

## Run

```bash
venv/bin/python app.py
# open http://127.0.0.1:5000
```

## Files

| file | role |
|---|---|
| `app.py` | server + poller + discovery |
| `counter_engine.py` | deck-vs-deck scoring |
| `counters.json` | matchup matrix + win conditions (edit freely) |
| `templates/index.html` | live dashboard |
| `targets.json` | tracked players (auto-saved) |
| `my_decks.json` | your decks (auto-saved) |
