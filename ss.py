import math
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter

BASE_URL = "https://api.clashroyale.com/v1"


def load_env(path=".env"):
    """Load KEY=VALUE pairs from a .env file (no extra dependencies).

    Values in .env win over already-exported environment variables,
    so updating .env is always enough to switch keys.
    """
    if not os.path.exists(path):
        return

    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ[key.strip()] = value.strip()


load_env()

API_KEY = os.getenv("CLASH_ROYALE_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

MAX_PAGES = 600
PAGE_SIZE = 1000                # big pages = far fewer requests (falls back to 50)
RANK_RANGE = 50                 # leaderboard ranks above/below yours to consider

RESULTS_COUNT = 5              # how many players to show
ACTIVE_MINUTES = 5             # played a ranked battle in the last N minutes = "active"
MAX_ACTIVITY_CHECKS = 150       # max players whose battle log we check

MAX_POOL = 300                  # max players scanned (outside top 10,000)
MAX_CRAWL = 30                  # max battle logs read to grow the player pool

WORKERS = 10                    # parallel requests (lower it if you get 429 errors)

OUTPUT_FILE = "ranked_report.txt"

# Your connection rotates between a few egress IPs and the API key only
# allows one of them, so a 403 invalid-IP answer is often temporary.
IP_RETRIES = 6
IP_RETRY_DELAY = 0.5

session = requests.Session()
session.mount("https://", HTTPAdapter(pool_connections=WORKERS,
                                       pool_maxsize=WORKERS))

report_lines = []


def out(text=""):
    """Print a line and keep it for the output file."""
    print(text)
    report_lines.append(text)


def save_report():
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as handle:
        handle.write(f"Report generated: {stamp}\n")
        handle.write("\n".join(report_lines) + "\n")
    print(f"\nSaved to {os.path.abspath(OUTPUT_FILE)}")


def api_get(endpoint, params=None):
    last_ip_error = None

    for attempt in range(IP_RETRIES):
        response = session.get(
            BASE_URL + endpoint,
            headers=HEADERS,
            params=params,
            timeout=20,
        )

        if response.status_code == 403 and "invalidIp" in response.text:
            last_ip_error = response
            time.sleep(IP_RETRY_DELAY * (attempt + 1))
            continue

        if response.status_code == 429:
            time.sleep(IP_RETRY_DELAY * (attempt + 1))
            continue

        if response.status_code == 401:
            raise RuntimeError("API key invalid.")
        if response.status_code == 403:
            raise RuntimeError("Access denied. Check your API key and IP.")
        if response.status_code == 404:
            raise RuntimeError("Player or Ranked season not found.")

        response.raise_for_status()
        return response.json()

    raise RuntimeError(
        "Access denied: your public IP is not whitelisted for this key "
        f"(tried {IP_RETRIES} times). Add the IP range in the Supercell "
        "developer portal or retry later. Last error: "
        f"{last_ip_error.text if last_ip_error is not None else 'unknown'}"
    )


def safe(func, *args):
    """Run an API call, return None instead of raising (for parallel use)."""
    try:
        return func(*args)
    except (RuntimeError, requests.RequestException):
        return None


def parallel_map(func, items):
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        return list(pool.map(func, items))


def get_player(tag):
    return api_get("/players/" + quote(tag, safe=""))


def get_battlelog(tag):
    return api_get("/players/" + quote(tag, safe="") + "/battlelog")


def get_global_rankings(stop_after_tag=None):
    # Current (live) season leaderboard
    endpoint = "/locations/global/pathoflegend/players"

    page_size = PAGE_SIZE
    players = []
    cursor = None
    found = False
    pages_after_found = 0

    for _ in range(MAX_PAGES):
        params = {"limit": page_size}

        if cursor:
            params["after"] = cursor

        try:
            data = api_get(endpoint, params)
        except requests.HTTPError:
            # The API refused the big page size: use the safe one.
            if page_size > 50 and not players:
                page_size = 50
                continue
            raise

        items = data.get("items", [])
        players.extend(items)

        if stop_after_tag and any(
            p.get("tag", "").upper() == stop_after_tag for p in items
        ):
            found = True

        if found:
            pages_after_found += 1
            if pages_after_found > math.ceil(RANK_RANGE / page_size):
                break

        cursor = data.get("paging", {}).get("cursors", {}).get("after")

        if not cursor:
            break

    return players


def parse_battle_time(value):
    # Format: 20261017T123456.000Z
    try:
        return datetime.strptime(value, "%Y%m%dT%H%M%S.%fZ").replace(
            tzinfo=timezone.utc
        )
    except (TypeError, ValueError):
        return None


def minutes_since_last_battle(tag):
    """Minutes since the player's most recent RANKED battle (None if none)."""
    battles = safe(get_battlelog, tag)
    if not battles:
        return None

    times = [
        parse_battle_time(b.get("battleTime"))
        for b in battles
        if "pathoflegend" in str(b.get("type", "")).lower()
    ]
    times = [t for t in times if t is not None]
    if not times:
        return None

    delta = datetime.now(timezone.utc) - max(times)
    return max(delta.total_seconds() / 60, 0)


def is_active(candidate):
    minutes = candidate.get("minutes")
    return minutes is not None and minutes <= ACTIVE_MINUTES


def pick_closest(candidates):
    """candidates must be sorted from closest to farthest.

    Checks battle logs in parallel chunks, closest players first, and
    stops as soon as RESULTS_COUNT active players are found. If not
    enough players are active, the closest inactive ones fill the rest.
    """
    out("\nChecking who is active (closest players first)...")
    active, inactive = [], []
    chunk_size = WORKERS * 2
    todo = candidates[:MAX_ACTIVITY_CHECKS]

    for start in range(0, len(todo), chunk_size):
        chunk = todo[start:start + chunk_size]
        minutes_list = parallel_map(
            lambda c: minutes_since_last_battle(c["tag"]), chunk
        )
        for candidate, minutes in zip(chunk, minutes_list):
            candidate["minutes"] = minutes
            (active if is_active(candidate) else inactive).append(candidate)

        if len(active) >= RESULTS_COUNT:
            break

    return active[:RESULTS_COUNT], inactive


def format_minutes(minutes):
    if minutes is None:
        return "no ranked battle in log"
    if minutes < 60:
        return f"{int(minutes)} min ago"
    if minutes < 60 * 24:
        return f"{minutes / 60:.1f} h ago"
    return f"{minutes / (60 * 24):.1f} days ago"


def describe(candidate):
    parts = [candidate.get("name", "Unknown"), candidate["tag"]]
    if candidate.get("rank") is not None:
        parts.append(f"Rank #{candidate['rank']}")
    if candidate.get("elo") is not None:
        parts.append(f"ELO {candidate['elo']}")
    if candidate.get("gap_text"):
        parts.append(candidate["gap_text"])
    parts.append(f"Clan: {candidate.get('clan', 'No clan')}")
    parts.append(
        f"Last ranked battle: {format_minutes(candidate.get('minutes'))}"
    )
    return " | ".join(parts)


def show_result(title, active, inactive):
    out(
        f"\n--- {title}: {len(active)} ACTIVE NOW "
        f"(ranked battle in last {ACTIVE_MINUTES} min) ---"
    )
    if not active:
        out("Nobody close to you played recently.")
    for i, c in enumerate(active, 1):
        out(f"{i}. {describe(c)}")

    missing = RESULTS_COUNT - len(active)
    if missing > 0 and inactive:
        out(f"\n--- NEXT CLOSEST (no ranked battle in last {ACTIVE_MINUTES} min) ---")
        for i, c in enumerate(inactive[:missing], len(active) + 1):
            out(f"{i}. {describe(c)}")


def run_leaderboard_mode(player_info, players, me):
    my_rank = me.get("rank")
    if not isinstance(my_rank, int):
        out("Your rank is unavailable, can't compute a rank range.")
        return

    out("\n--- YOUR RANKED INFO ---")
    out(f"Global rank: {my_rank}")
    out(f"ELO: {me.get('eloRating', 'N/A')}")
    out(f"League: {me.get('leagueNumber', 'N/A')}")

    candidates = []
    for p in players:
        if p.get("tag", "").upper() == player_info["tag"].upper():
            continue
        rank = p.get("rank")
        if not isinstance(rank, int) or abs(rank - my_rank) > RANK_RANGE:
            continue
        candidates.append({
            "rank": rank,
            "name": p.get("name", "Unknown"),
            "tag": p.get("tag", "N/A"),
            "elo": p.get("eloRating"),
            "clan": (p.get("clan") or {}).get("name", "No clan"),
            "diff": abs(rank - my_rank),
            "gap_text": f"{rank - my_rank:+d} ranks",
        })

    candidates.sort(key=lambda c: (c["diff"], c["rank"]))
    active, inactive = pick_closest(candidates)
    show_result("CLOSEST PLAYERS TO YOUR RANK", active, inactive)


def ranked_opponents(battles):
    """Opponents (tag + name) from Path of Legend battles only."""
    found = []
    for battle in battles or []:
        if "pathoflegend" not in str(battle.get("type", "")).lower():
            continue
        for opp in battle.get("opponent", []):
            if opp.get("tag"):
                found.append({
                    "tag": opp["tag"],
                    "name": opp.get("name", "Unknown"),
                    "clan": (opp.get("clan") or {}).get("name", "No clan"),
                })
    return found


def run_fallback_mode(player_info, players):
    """Used when the player is outside the public top-10,000 leaderboard.

    Builds a pool of real ranked players (your opponents and their
    opponents), reads each one's REAL Path of Legend result from their
    profile, and sorts them by how close they are to yours.
    """
    result = player_info.get("currentPathOfLegendSeasonResult") or {}
    my_trophies = result.get("trophies")
    my_tag = player_info["tag"].upper()

    out("\n--- YOUR PATH OF LEGEND INFO (from your profile) ---")
    out(f"League: {result.get('leagueNumber', 'N/A')}")
    out(f"Trophies/ELO: {my_trophies if my_trophies is not None else 'N/A'}")
    out(f"Rank: {result.get('rank') or 'outside top 10,000'}")

    if players:
        last = players[-1]
        out(
            f"Leaderboard cutoff: rank #{last.get('rank', 'N/A')} "
            f"= ELO {last.get('eloRating', 'N/A')}"
        )

    if my_trophies is None:
        out("\nYour Path of Legend trophies are unavailable, "
            "can't find close players.")
        return

    # 1) Build the pool: my opponents + their opponents (in parallel)
    print("\nBuilding player pool from battle logs...")
    pool = {}
    for opp in ranked_opponents(get_battlelog(player_info["tag"])):
        pool[opp["tag"].upper()] = opp

    frontier = list(pool.values())
    crawled = 0
    while frontier and len(pool) < MAX_POOL and crawled < MAX_CRAWL:
        batch = frontier[:MAX_CRAWL - crawled]
        frontier = []
        crawled += len(batch)
        logs = parallel_map(lambda o: safe(get_battlelog, o["tag"]), batch)
        for battles in logs:
            for extra in ranked_opponents(battles):
                key = extra["tag"].upper()
                if key not in pool and len(pool) < MAX_POOL:
                    pool[key] = extra
                    frontier.append(extra)

    pool.pop(my_tag, None)

    # 2) Read the real ranked result of every pool player (in parallel)
    print(f"Reading ranked profiles of {len(pool)} players...")
    pool_list = list(pool.values())
    profiles = parallel_map(lambda c: safe(get_player, c["tag"]), pool_list)

    candidates = []
    for cand, profile in zip(pool_list, profiles):
        if not profile:
            continue
        res = profile.get("currentPathOfLegendSeasonResult") or {}
        trophies = res.get("trophies")
        if trophies is None:
            continue
        cand["name"] = profile.get("name", cand["name"])
        cand["elo"] = trophies
        cand["league"] = res.get("leagueNumber")
        cand["rank"] = res.get("rank")
        cand["diff"] = abs(trophies - my_trophies)
        cand["gap_text"] = f"diff {cand['diff']}"
        candidates.append(cand)

    if not candidates:
        out("\nNo players with a ranked result were found.")
        return

    # 3) Closest by ELO first; keep the first RESULTS_COUNT active ones
    candidates.sort(key=lambda c: c["diff"])
    active, inactive = pick_closest(candidates)
    show_result("CLOSEST PLAYERS TO YOUR ELO", active, inactive)


def main():
    if not API_KEY:
        print("Error: CLASH_ROYALE_API_KEY is not set.")
        print("Set your API key before running this program.")
        return

    tag = input("Enter your Clash Royale Player Tag: ").strip().upper()

    if not tag.startswith("#"):
        tag = "#" + tag

    started = time.time()

    try:
        player_info = get_player(tag)

        out(f"Player: {player_info.get('name')}")
        out(f"Tag: {player_info.get('tag')}")
        print("\nLoading Ranked leaderboard...")

        players = get_global_rankings(
            stop_after_tag=player_info["tag"].upper(),
        )

        me = next(
            (
                p for p in players
                if p.get("tag", "").upper()
                == player_info["tag"].upper()
            ),
            None,
        )

        if me is None:
            out(
                "\nYour account wasn't found in the leaderboard "
                f"({len(players)} players downloaded). "
                "You are probably outside the top 10,000."
            )
            run_fallback_mode(player_info, players)
        else:
            run_leaderboard_mode(player_info, players, me)

        out(
            "\nNote: 'Active' means the player finished a RANKED battle in the "
            f"last {ACTIVE_MINUTES} minutes. The API has no real online "
            "status, and the battle log can lag a little."
        )
        out(f"Done in {time.time() - started:.1f} seconds.")
        save_report()

    except requests.RequestException as error:
        print(f"\nAPI/network error: {error}")
    except (RuntimeError, ValueError, KeyError, TypeError) as error:
        print(f"\nError: {error}")


if __name__ == "__main__":
    main()