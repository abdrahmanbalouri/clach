#!/usr/bin/env python3
"""
CR Snipe - Clash Royale PoL snipe engine (stable version).

Chno jdid f had version:
  - RateLimiter m3a penalize() f 429  -> ma b9ach kayt9al lprocess
  - poller / refresh_me / range_discovery kollhom m7miyin b try/except + traceback
  - budget dyal requests l kol cycle (ma kaywsslch l 429)
  - timeouts qsar (2s connect, 4s read)
  - kaytl3o KOLLHOM lplayers li f range dyalk (PRIME / STRICT / OUTER)
  - "HOT" = player salla lmatch dyalo daba (f HOT_WINDOW) -> hada howa li t-queue m3ah

Run:  venv/bin/python app.py   then open http://127.0.0.1:5000
"""

import json
import os
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

BASE = Path(__file__).resolve().parent
load_dotenv(BASE / ".env")

API_KEY = os.getenv("CLASH_ROYALE_API_KEY", "").strip()
MY_TAG = os.getenv("MY_TAG", "").strip().upper()
if MY_TAG and not MY_TAG.startswith("#"):
    MY_TAG = "#" + MY_TAG

API_BASES = [
    "https://proxy.royaleapi.dev/v1",
    "https://api.clashroyale.com/v1",
]
active_base_idx = 0

POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "0.5"))
MAX_RPS = float(os.getenv("MAX_RPS", "20"))
HOT_WINDOW = int(os.getenv("HOT_WINDOW", "60"))
FRESH_WINDOW = int(os.getenv("FRESH_WINDOW", "40"))          # HOT/alert ghir ila salla f akhir X tawani
STRICT_ELO_WINDOW = int(os.getenv("STRICT_ELO_WINDOW", "28"))
RANGE_WINDOW = int(os.getenv("RANGE_WINDOW", "60"))
MAX_TARGETS_TOTAL = int(os.getenv("MAX_TARGETS_TOTAL", "1000"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT = os.getenv("TELEGRAM_CHAT_ID", "").strip()
ALERT_MIN_SCORE = int(os.getenv("ALERT_MIN_SCORE", "75"))   # a9al score bach ytsift alert
CRAWL_GAP = float(os.getenv("CRAWL_GAP", "0.3"))          # tanya bin kol crawl (~1.5 req/s)
LEADERBOARD_EVERY = int(os.getenv("LEADERBOARD_EVERY", "600"))  # kol 10 min
LEADERBOARD_PAGES = int(os.getenv("LEADERBOARD_PAGES", "10"))

TARGETS_FILE = BASE / "targets.json"
CARDS_CACHE = BASE / "cards_cache.json"

app = Flask(__name__)
http = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32)
http.mount("https://", adapter)
http.headers.update({"Authorization": f"Bearer {API_KEY}"})

lock = threading.Lock()
from collections import deque
lag_samples = deque(maxlen=60)   # (pub_lag, our_delay) dyal akhir detections
hunt_load = {"n": 0}
targets = {}
me = {
    "tag": MY_TAG, "name": None, "trophies": None,
    "pol_trophies": None, "pol_delta": None, "league": None,
    "last_battle_time": None, "last_battle_end": None,
    "mm_avg_diff": 9, "mm_max_diff": 26, "updated_at": 0,
}
cards_index = {}
api_state = {"ok": True, "error": None, "public_ip": None, "base": API_BASES[0]}


class ApiError(Exception):
    def __init__(self, status, message=""):
        super().__init__(message)
        self.status = status


def norm_tag(tag):
    tag = (tag or "").strip().upper().replace("O", "0")
    if tag and not tag.startswith("#"):
        tag = "#" + tag
    return tag


def refresh_public_ip():
    if not api_state.get("public_ip"):
        try:
            api_state["public_ip"] = requests.get("https://api.ipify.org", timeout=4).text
        except Exception:
            pass


class RateLimiter:
    def __init__(self, rps):
        self.min_interval = max(1.0 / max(rps, 1.0), 0.02)
        self.lock = threading.Lock()
        self.next_ok = 0.0

    def wait(self):
        with self.lock:
            now = time.time()
            slot = max(self.next_ok, now)
            self.next_ok = slot + self.min_interval
        delay = slot - time.time()
        if delay > 0:
            time.sleep(delay)

    def penalize(self, sec):
        """Kolchi kaywqef sec thwaya (machi ghir thread wa7ed)."""
        with self.lock:
            self.next_ok = max(self.next_ok, time.time() + sec)


RATE = RateLimiter(MAX_RPS)


def api_get(path):
    global active_base_idx
    for attempt in (1, 2):
        RATE.wait()
        base = API_BASES[active_base_idx]
        try:
            r = http.get(base + path, timeout=(2, 4))
        except Exception as e:
            if attempt == 2:
                raise ApiError(500, str(e)[:120])
            continue
        if r.status_code == 200:
            api_state["ok"] = True
            api_state["error"] = None
            api_state["base"] = base
            return r.json()
        if r.status_code == 403:
            other_idx = 1 - active_base_idx
            try:
                r2 = http.get(API_BASES[other_idx] + path, timeout=(2, 4))
                if r2.status_code == 200:
                    active_base_idx = other_idx
                    api_state["ok"] = True
                    api_state["error"] = None
                    api_state["base"] = API_BASES[other_idx]
                    return r2.json()
            except Exception:
                pass
            api_state["ok"] = False
            api_state["error"] = ("API key rejected (403). Zid 45.79.218.79 (RoyaleAPI Proxy) "
                                  "wla IP dyalk f developer.clashroyale.com")
            refresh_public_ip()
            raise ApiError(403, r.text[:200])
        if r.status_code == 404:
            raise ApiError(404, "not found")
        if r.status_code == 429:
            RATE.penalize(2.0)
            if attempt == 2:
                raise ApiError(429, "rate limited")
            continue
        raise ApiError(r.status_code, r.text[:200])


def parse_bt(s):
    try:
        return datetime.strptime(s, "%Y%m%dT%H%M%S.%fZ").replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return None


def calc_post_elo(player_obj):
    """ELO exact mn battlelog: startingTrophies + trophyChange."""
    if not player_obj:
        return None, None
    start = player_obj.get("startingTrophies")
    delta = player_obj.get("trophyChange")
    if start is None or start <= 0:
        return None, None
    if delta is None:
        return int(start), 0
    return int(start) + int(delta), int(delta)


def extract_deck(player_obj):
    return [{
        "name": c.get("name"),
        "icon": (c.get("iconUrls") or {}).get("medium"),
        "level": c.get("level"),
        "evo": bool(c.get("evolutionLevel")),
    } for c in (player_obj or {}).get("cards", [])]


# ---------------------------------------------------------------- targets

def new_target(tag, name=None, source="range"):
    return {
        "tag": tag, "name": name or tag, "source": source, "added": time.time(),
        "trophies": None, "pol_trophies": None, "pol_delta": None,
        "starting_elo": None, "league": 7,
        "last_battle_time": None, "last_battle_end": None,
        "detected_at": None, "pub_lag": None,
        "hot_since": None, "hot_type": None,
        "deck": [], "result": None, "crowns": None, "battle_type": None,
        "error": None, "info_ts": 0,
        "median_gap": None, "last_gap": None, "games_last_hour": 0,
        "last_opp_tag": None, "last_opp_name": None, "last_opp_elo": None,
        "sniped_at": None, "missed_at": None,
        "polled_at": 0, "crawled_at": 0, "our_delay": None,
    }


def elo_band_status(t, my_rating):
    pt = t.get("pol_trophies")
    if pt is None or not my_rating:
        return "UNKNOWN"
    d = abs(pt - my_rating)
    if d <= 18:
        return "PRIME"
    if d <= STRICT_ELO_WINDOW:
        return "STRICT"
    if d <= RANGE_WINDOW:
        return "OUTER"
    return "OUT"


def snipe_score(t, now, my_rating):
    hs = t.get("hot_since")
    if not hs:
        return 0, "NONE"
    dt = now - hs
    if dt < 0 or dt > HOT_WINDOW:
        return 0, "NONE"
    if t.get("missed_at") and t["missed_at"] >= hs:
        return 0, "NONE"

    pt = t.get("pol_trophies")
    if pt is None or not my_rating:
        return 0, "NONE"

    diff = abs(pt - my_rating)
    if diff > 45:
        return 0, "NONE"

    if diff <= 10:
        score = 96
    elif diff <= 18:
        score = 92
    elif diff <= STRICT_ELO_WINDOW:
        score = 84
    else:
        score = 42

    if dt <= 18:
        pass
    elif dt <= 32:
        score -= 5
    elif dt <= 45:
        score -= 16
    else:
        score -= 32

    lg = t.get("last_gap")
    mg = t.get("median_gap")
    gh = t.get("games_last_hour") or 0
    if (lg is not None and lg <= 230) or (mg is not None and mg <= 240):
        score += 5
    elif mg is not None and mg <= 320:
        score += 3
    elif mg is not None and mg >= 700:
        score -= 18
    if gh >= 4:
        score += 2

    score = max(0, min(99, int(round(score))))
    if score >= 88 and diff <= STRICT_ELO_WINDOW and dt <= 35:
        grade = "S+"
    elif score >= 75 and diff <= STRICT_ELO_WINDOW:
        grade = "A"
    elif score >= 50:
        grade = "B"
    else:
        grade = "C"
    return score, grade


def save_targets():
    data = {
        tag: {
            "name": t.get("name"),
            "source": t.get("source", "range"),
            "pol_trophies": t.get("pol_trophies"),
            "pol_delta": t.get("pol_delta"),
            "league": t.get("league"),
            "last_battle_end": t.get("last_battle_end"),
        }
        for tag, t in targets.items()
    }
    TARGETS_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))


def load_targets():
    if TARGETS_FILE.exists():
        try:
            raw = json.loads(TARGETS_FILE.read_text())
            for tag, meta in raw.items():
                nt = new_target(tag, meta.get("name"), meta.get("source", "range"))
                nt["pol_trophies"] = meta.get("pol_trophies")
                nt["pol_delta"] = meta.get("pol_delta")
                nt["league"] = meta.get("league") or 7
                nt["last_battle_end"] = meta.get("last_battle_end")
                targets[tag] = nt
        except Exception:
            traceback.print_exc()


# ---------------------------------------------------------------- ingestion & polling

def upsert_opponent_from_battle(opp_obj, bt_str, evt_ts, now_ts, my_rating, was_new_battle, max_age=900):
    """Kol battle kat3tina 2 players li salaw f nafs tthanya."""
    if not opp_obj or not my_rating:
        return
    otag = norm_tag(opp_obj.get("tag"))
    if not otag or otag == MY_TAG:
        return
    post_elo, delta = calc_post_elo(opp_obj)
    if post_elo is None:
        return
    in_band = abs(post_elo - my_rating) <= RANGE_WINDOW

    with lock:
        ot = targets.get(otag)
        if not ot:
            if not in_band or now_ts - evt_ts > max_age:
                return
            if len(targets) >= MAX_TARGETS_TOTAL:
                cands = [x for x in targets.values() if x["source"] == "range"]
                if not cands:
                    return
                cands.sort(key=lambda x: (
                    0 if (x.get("pol_trophies") is None or abs(x["pol_trophies"] - my_rating) > RANGE_WINDOW) else 1,
                    x.get("last_battle_end") or 0
                ))
                targets.pop(cands[0]["tag"], None)
            ot = new_target(otag, opp_obj.get("name"), "range")
            targets[otag] = ot

        prev_end = ot.get("last_battle_end") or 0
        if evt_ts >= prev_end:
            is_newer = (ot.get("last_battle_time") is not None
                        and bt_str != ot.get("last_battle_time") and evt_ts > prev_end)
            ot["name"] = opp_obj.get("name") or ot["name"]
            ot["pol_trophies"] = post_elo
            ot["pol_delta"] = delta
            ot["starting_elo"] = opp_obj.get("startingTrophies")
            ot["info_ts"] = now_ts
            ot["last_battle_time"] = bt_str
            ot["last_battle_end"] = evt_ts
            ot["battle_type"] = "pathOfLegend"
            if delta is not None:
                ot["result"] = "W" if delta > 0 else ("L" if delta < 0 else "D")
            if opp_obj.get("cards"):
                ot["deck"] = extract_deck(opp_obj)
            if (was_new_battle or is_newer) and (now_ts - evt_ts <= HOT_WINDOW):
                ot["hot_since"] = evt_ts
                ot["hot_type"] = "pathOfLegend"
                ot["detected_at"] = now_ts
                ot["pub_lag"] = max(0, round(now_ts - evt_ts))


def poll_target(tag):
    with lock:
        t = targets.get(tag)
        my_rating = me.get("pol_trophies") or 0
    if not t:
        return
    try:
        log = api_get("/players/" + quote(tag, safe="") + "/battlelog")
        now_ts = time.time()
        with lock:
            prev_poll = t.get("polled_at") or 0
            t["polled_at"] = now_ts
        pol_battles = [b for b in log if b.get("type") == "pathOfLegend"]

        bts = sorted([x for x in (parse_bt(b.get("battleTime")) for b in pol_battles) if x], reverse=True)
        gaps = [a - b for a, b in zip(bts, bts[1:]) if 90 < a - b < 3600]
        last_gap = gaps[0] if gaps else None
        median_gap = sorted(gaps)[len(gaps) // 2] if gaps else None
        games_1h = sum(1 for x in bts if now_ts - x <= 3600)

        latest = pol_battles[0] if pol_battles else None
        if latest:
            bt = latest.get("battleTime")
            evt = parse_bt(bt) or now_ts
            if evt > now_ts or evt < now_ts - 86400 * 30:
                evt = now_ts
            team = (latest.get("team") or [{}])[0]
            opp = (latest.get("opponent") or [{}])[0]
            tc, oc = team.get("crowns", 0), opp.get("crowns", 0)
            post_elo, delta = calc_post_elo(team)
            opp_elo, _ = calc_post_elo(opp)
            opp_tag = norm_tag(opp.get("tag"))

            was_new_battle = False
            with lock:
                t["median_gap"] = median_gap
                t["last_gap"] = last_gap
                t["games_last_hour"] = games_1h

                prev_bt = t.get("last_battle_time")
                if prev_bt and bt and bt != prev_bt:
                    was_new_battle = True
                    was_hot = t.get("hot_since") and (now_ts - t["hot_since"] < 300)
                    t["hot_since"] = evt
                    t["hot_type"] = "pathOfLegend"
                    t["detected_at"] = now_ts
                    t["pub_lag"] = max(0, round(now_ts - evt))
                    t["our_delay"] = round(now_ts - prev_poll, 1) if prev_poll else None
                    lag_samples.append((t["pub_lag"], t["our_delay"]))
                    if opp_tag == MY_TAG:
                        t["sniped_at"] = now_ts
                    elif was_hot:
                        t["missed_at"] = now_ts
                elif prev_bt is None and (now_ts - evt <= HOT_WINDOW):
                    was_new_battle = True
                    t["hot_since"] = evt
                    t["hot_type"] = "pathOfLegend"
                    t["detected_at"] = now_ts
                    t["pub_lag"] = max(0, round(now_ts - evt))

                if bt:
                    t["last_battle_time"] = bt
                    t["last_battle_end"] = evt
                if post_elo is not None:
                    t["pol_trophies"] = post_elo
                    t["pol_delta"] = delta
                    t["starting_elo"] = team.get("startingTrophies")
                    t["info_ts"] = now_ts
                t["last_opp_tag"] = opp_tag
                t["last_opp_name"] = opp.get("name")
                t["last_opp_elo"] = opp_elo
                t["deck"] = extract_deck(team)
                t["battle_type"] = "pathOfLegend"
                t["result"] = "W" if tc > oc else ("L" if tc < oc else "D")
                t["crowns"] = f"{tc}-{oc}"
                if team.get("name"):
                    t["name"] = team["name"]
                t["error"] = None

            upsert_opponent_from_battle(opp, bt, evt, now_ts, my_rating, was_new_battle)

            for b in pol_battles[1:4]:
                b_bt = b.get("battleTime")
                b_evt = parse_bt(b_bt)
                if b_evt and now_ts - b_evt < 1200:
                    b_opp = (b.get("opponent") or [{}])[0]
                    upsert_opponent_from_battle(b_opp, b_bt, b_evt, now_ts, my_rating, False)

        if t.get("pol_trophies") is None and now_ts - t.get("info_ts", 0) > 120:
            try:
                p = api_get("/players/" + quote(tag, safe=""))
                pol = p.get("currentPathOfLegendSeasonResult") or {}
                with lock:
                    t["name"] = p.get("name", t["name"])
                    t["trophies"] = p.get("trophies")
                    t["pol_trophies"] = pol.get("trophies")
                    t["league"] = pol.get("leagueNumber")
                    t["info_ts"] = now_ts
            except ApiError:
                pass
    except ApiError as e:
        with lock:
            t["error"] = "player not found" if e.status == 404 else f"api {e.status}"
    except Exception:
        traceback.print_exc()
        with lock:
            t["error"] = "network error"


def refresh_me():
    """ELO dyalk exact mn battlelog (kaysbq profile)."""
    try:
        now_ts = time.time()
        log = api_get("/players/" + quote(MY_TAG, safe="") + "/battlelog")
        pol_battles = [b for b in log if b.get("type") == "pathOfLegend"]

        log_elo, log_delta, log_bt, log_evt, my_name = None, None, None, None, None
        diffs = []
        for b in pol_battles:
            tm = (b.get("team") or [{}])[0]
            op = (b.get("opponent") or [{}])[0]
            ts, os_ = tm.get("startingTrophies"), op.get("startingTrophies")
            if ts and os_:
                diffs.append(abs(ts - os_))
        if pol_battles:
            b0 = pol_battles[0]
            tm0 = (b0.get("team") or [{}])[0]
            op0 = (b0.get("opponent") or [{}])[0]
            log_elo, log_delta = calc_post_elo(tm0)
            log_bt = b0.get("battleTime")
            log_evt = parse_bt(log_bt)
            my_name = tm0.get("name")

            op0_tag = norm_tag(op0.get("tag"))
            if op0_tag and log_evt and now_ts - log_evt < 600:
                with lock:
                    if op0_tag in targets:
                        targets[op0_tag]["sniped_at"] = log_evt

        prof_elo, prof_league, prof_trophies = None, None, None
        if now_ts - me.get("updated_at", 0) > 15 or log_elo is None:
            p = api_get("/players/" + quote(MY_TAG, safe=""))
            mypol = p.get("currentPathOfLegendSeasonResult") or {}
            prof_elo = mypol.get("trophies")
            prof_league = mypol.get("leagueNumber")
            prof_trophies = p.get("trophies")
            my_name = p.get("name") or my_name

        exact_elo = log_elo if log_elo is not None else prof_elo
        if exact_elo is None and me.get('pol_trophies') is None:
            print('[refresh_me] ma l9ina 7ta ELO PoL dyalk. tag=%s pol_battles=%d' % (MY_TAG, len(pol_battles)))

        with lock:
            if my_name:
                me["name"] = my_name
            if exact_elo is not None:
                me["pol_trophies"] = exact_elo
            if log_delta is not None:
                me["pol_delta"] = log_delta
            if prof_league is not None:
                me["league"] = prof_league
            if prof_trophies is not None:
                me["trophies"] = prof_trophies
            if log_bt:
                me["last_battle_time"] = log_bt
                me["last_battle_end"] = log_evt
            if diffs:
                me["mm_avg_diff"] = round(sum(diffs) / len(diffs), 1)
                me["mm_max_diff"] = max(diffs)
            me["updated_at"] = now_ts

        if exact_elo:
            for b in pol_battles[:15]:
                b_bt = b.get("battleTime")
                b_evt = parse_bt(b_bt) or 0
                op = (b.get("opponent") or [{}])[0]
                upsert_opponent_from_battle(op, b_bt, b_evt, now_ts, exact_elo, False, max_age=86400)
    except Exception:
        traceback.print_exc()


def predicted_window(t, now):
    """Wa9t li mtwaqa3 ysali fih next match: last_end + gap dyalo.
    Kanraqbo ghir f had l'window (-15s .. +35s) bach nkhdmo 3la li 9rib ysali."""
    lbe = t.get("last_battle_end") or 0
    if not lbe:
        return False
    gap = t.get("last_gap") or t.get("median_gap")
    if not gap or gap > 420:
        gap = 230
    exp_end = lbe + gap
    return (exp_end - 15) <= now <= (exp_end + 35)


def tier_and_priority(t, now, my_rating):
    pt = t.get("pol_trophies")
    diff = abs(pt - my_rating) if (pt is not None and my_rating) else 999
    lbe = t.get("last_battle_end") or 0
    ago = now - lbe if lbe else 999999
    in_expected_finish_window = (150 <= ago <= 320)

    if not t.get("polled_at") and diff <= RANGE_WINDOW:
        return 1, 2, diff           # jdid f range: poll-o awwalan

    # 1) li mtwaqa3 ysali daba (strict band) -> kol cycle, awwal priority
    if diff <= STRICT_ELO_WINDOW and predicted_window(t, now):
        return 1, 0, diff
    # 2) li salaw daba: kanchoufo wach l9a match akhor (a9al ahamiya)
    if t.get("hot_since") and now - t["hot_since"] < HOT_WINDOW and diff <= RANGE_WINDOW:
        return 2, 1, diff
    if diff <= STRICT_ELO_WINDOW and (ago < 900 or in_expected_finish_window):
        return 3, 3, diff
    if diff <= RANGE_WINDOW and ago < 600:
        return 3, 4, diff
    if diff <= STRICT_ELO_WINDOW and ago < 3600:
        return 4, 5, diff
    if diff <= RANGE_WINDOW and ago < 3600:
        return 5, 6, diff
    if diff <= RANGE_WINDOW:
        return 8, 7, diff
    return 20, 9, diff


def poller():
    cycle = 0
    refresh_me()
    while True:
        try:
            cycle += 1
            start = time.time()
            now = start
            with lock:
                tlist = list(targets.values())
                my_rating = me.get("pol_trophies") or 0

            scored = [(t["tag"], *tier_and_priority(t, now, my_rating), t.get("polled_at", 0)) for t in tlist]
            scored.sort(key=lambda x: (x[1], x[2], x[4]))   # tier, priority, li 9dam polled awwalan

            # budget: ma nzidouch 3la MAX_RPS (7na 5ssna requests dyal refresh_me)
            hunt_load["n"] = sum(1 for x in scored if x[1] == 1 and x[2] == 0)
            max_calls = max(8, int(MAX_RPS * POLL_INTERVAL * 0.8))
            due = [x[0] for x in scored if cycle % x[1] == 0][:max_calls]

            if due:
                with ThreadPoolExecutor(max_workers=10) as ex:
                    list(ex.map(poll_target, due))

            if cycle % 4 == 0:
                refresh_me()

            elapsed = time.time() - start
            time.sleep(max(0.15, POLL_INTERVAL - elapsed))
        except Exception:
            traceback.print_exc()
            time.sleep(2)


def add_range_target(tag, name, elo, my_rating, now_ts):
    """Zid player mn leaderboard (bla battle)."""
    tag = norm_tag(tag)
    if not tag or tag == MY_TAG or not elo or abs(elo - my_rating) > RANGE_WINDOW:
        return False
    with lock:
        if tag in targets or len(targets) >= MAX_TARGETS_TOTAL:
            return False
        nt = new_target(tag, name, "range")
        nt["pol_trophies"] = int(elo)
        nt["info_ts"] = now_ts
        targets[tag] = nt
    return True


def seed_location(loc_id, my_rating):
    """Jib players mn leaderboard dyal location (global wla blad) li f range dyalk."""
    added, after = 0, None
    try:
        for _ in range(LEADERBOARD_PAGES):
            path = f"/locations/{loc_id}/pathoflegend/players?limit=1000"
            if after:
                path += "&after=" + quote(str(after), safe="")
            data = api_get(path)
            items = data.get("items") or []
            if not items:
                break
            now_ts = time.time()
            for it in items:
                if add_range_target(it.get("tag"), it.get("name"), it.get("eloRating"), my_rating, now_ts):
                    added += 1
            first_elo = items[0].get("eloRating") or 0
            last_elo = items[-1].get("eloRating") or 0
            if first_elo and first_elo < my_rating - RANGE_WINDOW:
                break           # top dyal had location a9al mnk
            if last_elo and last_elo < my_rating - RANGE_WINDOW:
                break           # douz range dyalk
            after = ((data.get("paging") or {}).get("cursors") or {}).get("after")
            if not after:
                break
    except ApiError:
        pass
    except Exception:
        traceback.print_exc()
    return added


def leaderboard_worker():
    """Thread mlfsel: global kol LEADERBOARD_EVERY, o kol l'blad'at kol 30 min."""
    time.sleep(5)
    last_global, last_countries = 0, 0
    country_ids = []
    while True:
        try:
            with lock:
                my_rating = me.get("pol_trophies") or 0
            if not my_rating:
                time.sleep(3)
                continue
            now_ts = time.time()
            if now_ts - last_global > LEADERBOARD_EVERY:
                last_global = now_ts
                n = seed_location("global", my_rating)
                print(f"[leaderboard] global: +{n}")
            if now_ts - last_countries > 1800:
                last_countries = now_ts
                if not country_ids:
                    try:
                        locs = api_get("/locations?limit=1000").get("items", [])
                        country_ids = [l["id"] for l in locs if l.get("isCountry")]
                    except Exception:
                        traceback.print_exc()
                total = 0
                for lid in country_ids:
                    total += seed_location(lid, my_rating)
                    with lock:
                        my_rating = me.get("pol_trophies") or my_rating
                print(f"[leaderboard] {len(country_ids)} blad: +{total}")
        except Exception:
            traceback.print_exc()
        time.sleep(5)


def crawl_seed(tag, my_rating):
    """Kan9ra battlelog dyal seed o kanzid KOL opponents dyalo (2h lfaytin)."""
    slog = api_get("/players/" + quote(tag, safe="") + "/battlelog")
    now_ts = time.time()
    with lock:
        if tag in targets:
            targets[tag]["crawled_at"] = now_ts
    for b in slog:
        if b.get("type") != "pathOfLegend":
            continue
        bt = b.get("battleTime")
        evt = parse_bt(bt)
        if not evt or now_ts - evt > 7200:
            continue
        op = (b.get("opponent") or [{}])[0]
        upsert_opponent_from_battle(op, bt, evt, now_ts, my_rating, False, max_age=7200)


def range_discovery():
    """Crawler continu: kaykber l'pool dyal players li f range dyalk.
    BFS (opponents dyal opponents) + leaderboard. 1 seed kol CRAWL_GAP tanya."""
    time.sleep(3)
    last_lb, last_save = 0, 0
    while True:
        try:
            with lock:
                my_rating = me.get("pol_trophies") or 0
            if not my_rating:
                time.sleep(3)
                continue
            now_ts = time.time()

            with lock:
                to_drop = [
                    tg for tg, t in targets.items()
                    if t.get("source") in ("range", "auto")
                    and t.get("pol_trophies") is not None
                    and (t["pol_trophies"] <= 0 or abs(t["pol_trophies"] - my_rating) > RANGE_WINDOW)
                ]
                for tg in to_drop:
                    targets.pop(tg, None)

            with lock:
                cands = [t for t in targets.values()
                         if t.get("pol_trophies") and abs(t["pol_trophies"] - my_rating) <= RANGE_WINDOW]
                if not cands:
                    cands = list(targets.values())
                # l'aqdam crawl awwalan, o lli nashtin (last_battle_end jdid) qbl
                cands.sort(key=lambda t: (t.get("crawled_at", 0), -(t.get("last_battle_end") or 0)))
                seed = cands[0]["tag"] if cands else None

            if seed:
                try:
                    crawl_seed(seed, my_rating)
                except ApiError:
                    with lock:
                        if seed in targets:
                            targets[seed]["crawled_at"] = time.time()

            if now_ts - last_save > 60:
                last_save = now_ts
                with lock:
                    save_targets()
        except Exception:
            traceback.print_exc()
        time.sleep(CRAWL_GAP * (4 if hunt_load["n"] > 15 else 1))   # khelli rate l hunters


def send_telegram(text):
    if not (TELEGRAM_TOKEN and TELEGRAM_CHAT):
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": text}, timeout=4)
    except Exception:
        pass


def notifier():
    """Kay-checki kol 0.2s: ila player f STRICT band salla f akhir FRESH_WINDOW
    o score >= ALERT_MIN_SCORE -> alert fouran (terminal + Telegram), merra wa7da l kol match."""
    alerted = set()
    while True:
        try:
            now = time.time()
            with lock:
                my_rating = me.get("pol_trophies") or 0
                snap = [dict(t) for t in targets.values() if t.get("hot_since")]
            for t in snap:
                hs = t["hot_since"]
                if now - hs > FRESH_WINDOW:
                    continue
                key = f"{t['tag']}|{hs}"
                if key in alerted:
                    continue
                pt = t.get("pol_trophies")
                if pt is None or not my_rating or abs(pt - my_rating) > STRICT_ELO_WINDOW:
                    continue
                conf, grade = snipe_score(t, now, my_rating)
                if conf < ALERT_MIN_SCORE:
                    continue
                alerted.add(key)
                diff = pt - my_rating
                msg = (f"QUEUE DABA: {t.get('name')} {t['tag']}\n"
                       f"ELO {pt} ({'+' if diff > 0 else ''}{diff}) | salla men {int(now - hs)}s | "
                       f"score {conf}% {grade}")
                print("\a" + msg.replace("\n", " | "))
                threading.Thread(target=send_telegram, args=(msg,), daemon=True).start()
            if len(alerted) > 5000:
                alerted.clear()
        except Exception:
            traceback.print_exc()
        time.sleep(0.2)


def refresh_cards():
    global cards_index
    try:
        if CARDS_CACHE.exists():
            cards_index = json.loads(CARDS_CACHE.read_text())
        data = api_get("/cards")
        cards_index = {c["name"]: (c.get("iconUrls") or {}).get("medium")
                       for c in data.get("items", [])}
        CARDS_CACHE.write_text(json.dumps(cards_index))
    except Exception:
        traceback.print_exc()


def build_analysis(t, now, my_pol, conf, grade, band):
    """Tahlil dyal snipe: verdict + asbab (b facts mn API, machi tawa99o3)."""
    hs = t.get("hot_since")
    dt = (now - hs) if hs else None
    pt = t.get("pol_trophies")
    diff = abs(pt - my_pol) if (pt is not None and my_pol) else None
    reasons = []
    if diff is not None:
        reasons.append(f"ELO {pt} vs {my_pol} (fr9 {diff}, band {band})")
    if dt is not None:
        od = t.get("our_delay")
        extra = f", mnha max {od}s hiya dyalna (bin 2 polls)" if od is not None else ""
        reasons.append(f"salla men {int(dt)}s (tbayn men be3d {t.get('pub_lag')}s{extra}; lbaqi ta2khir dyal API)")
    lg, mg, gh = t.get("last_gap"), t.get("median_gap"), t.get("games_last_hour") or 0
    if lg is not None:
        reasons.append(f"akhir gap bin matchat: {int(lg)}s")
    if mg is not None:
        reasons.append(f"median gap: {int(mg)}s | {gh} matchat f akhir sa3a")
    if t.get("missed_at") and hs and t["missed_at"] >= hs:
        reasons.append("l9a match akhor deja")
    if conf >= 75 and diff is not None and diff <= STRICT_ELO_WINDOW and dt is not None and dt <= 35:
        verdict = "QUEUE NOW"
    elif conf >= 50:
        verdict = "QUEUE (momkin)"
    elif conf > 0:
        verdict = "DA3IF"
    else:
        verdict = "SKIP"
    return {"verdict": verdict, "reasons": reasons,
            "time_left": max(0, round(FRESH_WINDOW - dt)) if dt is not None else None}


# ---------------------------------------------------------------- serialize

def serialize():
    now = time.time()
    with lock:
        tlist = [dict(t) for t in targets.values()]
        me_copy = dict(me)
        api = dict(api_state)

    my_pol = me_copy.get("pol_trophies") or 0
    out = []
    for t in tlist:
        pt = t.get("pol_trophies")
        band = elo_band_status(t, my_pol)
        if band == "OUT" and t.get("source") != "manual":
            continue

        conf, grade = snipe_score(t, now, my_pol)
        if t.get("hot_since") and now - t["hot_since"] > FRESH_WINDOW:
            conf, grade = 0, "NONE"      # douz FRESH_WINDOW -> machi HOT
        lbe = t.get("last_battle_end")
        ago = round(now - lbe) if lbe else None

        in_game_pred = False
        est_finish_in = None
        mg = t.get("median_gap")
        lg = t.get("last_gap")
        typical_cycle = lg if (lg and lg <= 300) else (mg if (mg and mg <= 300) else None)
        if conf == 0 and band in ("PRIME", "STRICT") and ago is not None and typical_cycle:
            if 120 <= ago <= typical_cycle + 45:
                in_game_pred = True
                est_finish_in = max(5, round(typical_cycle - ago))

        if conf > 0:
            status = "HOT"
        elif band == "OUT":
            status = "OUT"
        elif in_game_pred:
            status = "PLAYING"
        elif ago is not None and ago < 600:
            status = "ACTIVE"
        else:
            status = "COLD"

        out.append({
            "tag": t["tag"], "name": t.get("name"),
            "pol_trophies": pt, "pol_delta": t.get("pol_delta"),
            "starting_elo": t.get("starting_elo"),
            "elo_diff": (pt - my_pol) if (pt is not None and my_pol) else None,
            "abs_diff": abs(pt - my_pol) if (pt is not None and my_pol) else 999,
            "band": band,
            "in_range": band in ("PRIME", "STRICT", "OUTER"),
            "in_strict": band in ("PRIME", "STRICT"),
            "elo_ago": round(now - t["info_ts"]) if t.get("info_ts") else None,
            "status": status,
            "hot_since": t.get("hot_since"), "hot_type": t.get("hot_type"),
            "pub_lag": t.get("pub_lag"),
            "confidence": conf, "grade": grade,
            "median_gap": mg, "last_gap": lg,
            "games_last_hour": t.get("games_last_hour", 0),
            "in_game_pred": in_game_pred, "est_finish_in": est_finish_in,
            "seconds_since_end": round(now - t["hot_since"]) if t.get("hot_since") else None,
            "last_battle_ago": ago,
            "result": t.get("result"), "crowns": t.get("crowns"),
            "battle_type": t.get("battle_type"),
            "deck": t.get("deck", []),
            "last_opp_tag": t.get("last_opp_tag"),
            "last_opp_name": t.get("last_opp_name"),
            "last_opp_elo": t.get("last_opp_elo"),
            "sniped_ago": round(now - t["sniped_at"]) if t.get("sniped_at") else None,
            "missed_ago": round(now - t["missed_at"]) if t.get("missed_at") else None,
            "source": t.get("source"), "error": t.get("error"),
            "analysis": build_analysis(t, now, my_pol, conf, grade, band),
        })

    order = {"HOT": 0, "PLAYING": 1, "ACTIVE": 2, "COLD": 3, "OUT": 4}
    out.sort(key=lambda x: (
        order.get(x["status"], 9),
        -(x["confidence"] or 0),
        x["abs_diff"],
        x["last_battle_ago"] if x["last_battle_ago"] is not None else 999999,
    ))
    hot = [t for t in out if t["status"] == "HOT" and t["in_strict"]]
    playing = [t for t in out if t["status"] == "PLAYING"]
    return {
        "api": api, "me": me_copy,
        "hot_window": FRESH_WINDOW,
        "strict_window": STRICT_ELO_WINDOW,
        "range_window": RANGE_WINDOW,
        "best": (hot[0] if hot else None),
        "playing_soon": playing[:5],
        "counts": {
            "total": len(out),
            "in_range": sum(1 for x in out if x["in_range"]),
            "prime": sum(1 for x in out if x["band"] == "PRIME"),
            "strict": sum(1 for x in out if x["in_strict"]),
            "hot": len(hot),
            "playing": len(playing),
            "active": sum(1 for x in out if x["status"] == "ACTIVE"),
        },
        "targets": out,
    }


# ---------------------------------------------------------------- routes

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/state")
def api_state_route():
    return jsonify(serialize())


@app.route("/api/hot")
def hot_route():
    st = serialize()
    hot = [t for t in st["targets"] if t["status"] == "HOT" and t["in_range"]]
    hot.sort(key=lambda x: (-x["confidence"], x["abs_diff"]))
    samples = list(lag_samples)
    def med(vals):
        vals = sorted(v for v in vals if v is not None)
        return vals[len(vals) // 2] if vals else None
    lag = {"n": len(samples), "pub_lag_median": med([a for a, _ in samples]),
           "our_delay_median": med([b for _, b in samples])}
    return jsonify({"lag": lag, "me": st["me"], "hot": hot, "playing_soon": st["playing_soon"],
                    "counts": st["counts"], "api": st["api"]})


HOT_HTML = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>HOT</title>
<style>
body{font-family:system-ui,sans-serif;background:#0f1115;color:#eee;margin:0;padding:16px}
h1{font-size:18px;margin:0 0 12px}.me{opacity:.7;font-size:13px;margin-bottom:12px}
.card{background:#1a1d24;border-left:6px solid #888;border-radius:8px;padding:12px;margin-bottom:10px}
.card.now{border-color:#2ecc71;background:#16261c}.card.maybe{border-color:#f1c40f}
.big{font-size:20px;font-weight:700}.tag{font-family:monospace;opacity:.8}
.v{float:right;font-weight:700}.r{font-size:13px;opacity:.85;margin-top:6px}
.t{font-size:28px;font-weight:700;float:right;margin-left:12px}
.empty{opacity:.5;padding:30px;text-align:center}
</style></head><body>
<h1>Players li salaw daba f range dyalk</h1><div class="me" id="me"></div><div id="list"></div>
<script>
let seen=new Set(), ctx=null;
function beep(){try{ctx=ctx||new (window.AudioContext||window.webkitAudioContext)();
const o=ctx.createOscillator(),g=ctx.createGain();o.connect(g);g.connect(ctx.destination);
o.frequency.value=880;g.gain.value=.15;o.start();o.stop(ctx.currentTime+.25);}catch(e){}}
function el(tag,cls,txt){const e=document.createElement(tag);if(cls)e.className=cls;if(txt!=null)e.textContent=txt;return e;}
async function tick(){
 try{
  const r=await fetch('/api/hot');const d=await r.json();
  document.getElementById('me').textContent=
   'ELO dyalk: '+(d.me.pol_trophies??'?')+' | targets f range: '+d.counts.in_range+' | api: '+(d.api.ok?'ok':d.api.error)+' | tbayn f akhir '+d.lag.n+' matchat: median '+(d.lag.pub_lag_median??'?')+'s (mnha dyalna: '+(d.lag.our_delay_median??'?')+'s max)';
  const L=document.getElementById('list');L.replaceChildren();
  if(!d.hot.length){L.appendChild(el('div','empty','Ma kayn 7ta wa7ed salla daba... kan-tsennaw'));}
  d.hot.forEach(t=>{
   const key=t.tag+'|'+t.hot_since;
   if(!seen.has(key)&&t.analysis.verdict!=='SKIP'){seen.add(key);beep();
    if(window.Notification&&Notification.permission==='granted'){new Notification('QUEUE DABA: '+(t.name||t.tag),{body:t.pol_trophies+' ('+t.elo_diff+') '+t.analysis.verdict});}}
   const c=el('div','card '+(t.analysis.verdict==='QUEUE NOW'?'now':(t.analysis.verdict.startsWith('QUEUE')?'maybe':'')));
   c.appendChild(el('span','t',(t.analysis.time_left??'')+'s'));
   c.appendChild(el('span','v',t.analysis.verdict+' ('+t.confidence+'% '+t.grade+')'));
   c.appendChild(el('div','big',(t.name||'')+'  '+t.pol_trophies+' ('+(t.elo_diff>0?'+':'')+t.elo_diff+')'));
   c.appendChild(el('div','tag',t.tag+'  '+(t.crowns||'')+' '+(t.result||'')));
   c.appendChild(el('div','r',t.analysis.reasons.join(' • ')));
   L.appendChild(c);
  });
 }catch(e){}
 setTimeout(tick,700);
}
document.addEventListener('click',()=>{if(!ctx)beep();if(window.Notification&&Notification.permission==='default')Notification.requestPermission();},{once:true});
tick();
</script></body></html>"""


@app.route("/hot")
def hot_page():
    return HOT_HTML


@app.route("/api/debug")
def debug_route():
    with lock:
        return jsonify({
            "my_tag": MY_TAG,
            "api_key_set": bool(API_KEY),
            "me": dict(me),
            "api": dict(api_state),
            "targets_total": len(targets),
            "targets_with_elo": sum(1 for t in targets.values() if t.get("pol_trophies")),
        })


@app.route("/api/targets", methods=["POST"])
def add_target():
    tag = norm_tag((request.get_json(force=True) or {}).get("tag"))
    if not tag or len(tag) < 4:
        return jsonify({"error": "invalid tag"}), 400
    with lock:
        if tag not in targets:
            targets[tag] = new_target(tag, source="manual")
            save_targets()
    threading.Thread(target=poll_target, args=(tag,), daemon=True).start()
    return jsonify({"ok": True, "tag": tag})


@app.route("/api/targets/<path:tag>", methods=["DELETE"])
def del_target(tag):
    tag = norm_tag(tag)
    with lock:
        targets.pop(tag, None)
        save_targets()
    return jsonify({"ok": True})


# ---------------------------------------------------------------- main

if __name__ == "__main__":
    load_targets()
    threading.Thread(target=refresh_cards, daemon=True).start()
    threading.Thread(target=poller, daemon=True).start()
    threading.Thread(target=range_discovery, daemon=True).start()
    threading.Thread(target=leaderboard_worker, daemon=True).start()
    threading.Thread(target=notifier, daemon=True).start()
    print(f"CR Snipe running -> http://127.0.0.1:5000  (me: {MY_TAG or '?'})")
    app.run(host="127.0.0.1", port=5000, threaded=True)