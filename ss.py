import os
import time
from datetime import datetime, timezone
from urllib.parse import quote

import requests

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
PAGE_SIZE = 50
RANK_RANGE = 10                 # ranks above/below yours to check
PAGES_AFTER_FOUND = RANK_RANGE // PAGE_SIZE + 1

ACTIVE_MINUTES = 15             # played a battle in the last N minutes = "active"
MAX_ACTIVITY_CHECKS = 60        # max players whose battle log we check
ACTIVITY_DELAY = 0.15           # pause between checks (avoid rate limit)

RESULTS_COUNT = 20              # closest players to show (outside top 10,000)
MAX_POOL = 80                   # max players to scan for closeness

OUTPUT_FILE = "ranked_report.txt"

# Your connection rotates between a few egress IPs and the API key only
# allows one of them, so a 403 invalid-IP answer is often temporary.
IP_RETRIES = 6
IP_RETRY_DELAY = 0.5

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
        response = requests.get(
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


def get_player(tag):
    return api_get("/players/" + quote(tag, safe=""))


def get_battlelog(tag):
    return api_get("/players/" + quote(tag, safe="") + "/battlelog")


def get_global_rankings(stop_after_tag=None):
    # Current (live) season leaderboard
    endpoint = "/locations/global/pathoflegend/players"

    players = []
    cursor = None
    found = False
    pages_after_found = 0

    for _ in range(MAX_PAGES):
        params = {"limit": PAGE_SIZE}

        if cursor:
            params["after"] = cursor

        data = api_get(endpoint, params)
        items = data.get("items", [])
        players.extend(items)

        if stop_after_tag and any(
            p.get("tag", "").upper() == stop_after_tag for p in items
        ):
            found = True

        if found:
            pages_after_found += 1
            if pages_after_found > PAGES_AFTER_FOUND:
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
    """Minutes since the player's most recent battle (None if unknown)."""
    try:
        battles = get_battlelog(tag)
    except (RuntimeError, requests.RequestException):
        return None

    times = [parse_battle_time(b.get("battleTime")) for b in battles]
    times = [t for t in times if t is not None]
    if not times:
        return None

    delta = datetime.now(timezone.utc) - max(times)
    return max(delta.total_seconds() / 60, 0)


def add_activity(candidates):
    """Fill the 'minutes' field of each candidate using its battle log."""
    todo = candidates[:MAX_ACTIVITY_CHECKS]
    out(f"\nChecking recent activity of {len(todo)} players...")

    for candidate in todo:
        candidate["minutes"] = minutes_since_last_battle(candidate["tag"])
        time.sleep(ACTIVITY_DELAY)


def format_minutes(minutes):
    if minutes is None:
        return "unknown"
    if minutes < 60:
        return f"{int(minutes)} min ago"
    if minutes < 60 * 24:
        return f"{minutes / 60:.1f} h ago"
    return f"{minutes / (60 * 24):.1f} days ago"


def describe(candidate):
    parts = []
    if candidate.get("rank") is not None:
        parts.append(f"#{candidate['rank']}")
    parts.append(candidate.get("name", "Unknown"))
    parts.append(candidate["tag"])
    if candidate.get("elo") is not None:
        parts.append(f"ELO {candidate['elo']}")
    parts.append(f"Clan: {candidate.get('clan', 'No clan')}")
    parts.append(f"Last battle: {format_minutes(candidate.get('minutes'))}")
    return " | ".join(parts)


def print_candidates(title, candidates):
    is_active = lambda c: (
        c.get("minutes") is not None and c["minutes"] <= ACTIVE_MINUTES
    )
    active = [c for c in candidates if is_active(c)]
    active.sort(key=lambda c: c["minutes"])

    out(f"\n--- {title}: ACTIVE NOW (battle in last {ACTIVE_MINUTES} min) ---")
    if active:
        for c in active:
            out(describe(c))
    else:
        out("Nobody from this list played in the last "
            f"{ACTIVE_MINUTES} minutes.")

    out(f"\n--- {title}: ALL ---")
    for c in candidates:
        out(describe(c))


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
        })

    candidates.sort(key=lambda c: c["rank"])
    add_activity(candidates)
    print_candidates(f"PLAYERS WITHIN ±{RANK_RANGE} RANKS", candidates)


def ranked_opponents(battles):
    """Opponents (tag + name) from Path of Legend battles only."""
    found = []
    for battle in battles:
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

    # 1) Build the pool: my opponents + their opponents
    print("\nBuilding player pool from battle logs...")
    pool = {}
    for opp in ranked_opponents(get_battlelog(player_info["tag"])):
        pool[opp["tag"].upper()] = opp

    first_degree = list(pool.values())
    for opp in first_degree:
        if len(pool) >= MAX_POOL:
            break
        try:
            battles = get_battlelog(opp["tag"])
        except (RuntimeError, requests.RequestException):
            continue
        for extra in ranked_opponents(battles):
            if len(pool) >= MAX_POOL:
                break
            pool.setdefault(extra["tag"].upper(), extra)
        time.sleep(ACTIVITY_DELAY)

    pool.pop(my_tag, None)

    # 2) Read the real ranked result of every pool player
    print(f"Reading ranked profiles of {len(pool)} players...")
    candidates = []
    for key, cand in pool.items():
        try:
            profile = get_player(cand["tag"])
        except (RuntimeError, requests.RequestException):
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
        candidates.append(cand)
        time.sleep(ACTIVITY_DELAY)

    if not candidates:
        out("\nNo players with a ranked result were found.")
        return

    # 3) Keep only the closest, then check who is active
    candidates.sort(key=lambda c: c["diff"])
    candidates = candidates[:RESULTS_COUNT]
    add_activity(candidates)

    active = [
        c for c in candidates
        if c.get("minutes") is not None and c["minutes"] <= ACTIVE_MINUTES
    ]
    active.sort(key=lambda c: c["diff"])

    def line(c):
        return (
            f"{c['name']} | {c['tag']} | ELO {c['elo']} "
            f"(diff {c['diff']}) | League {c.get('league', 'N/A')} "
            f"| Clan: {c.get('clan', 'No clan')} "
            f"| Last battle: {format_minutes(c.get('minutes'))}"
        )

    out(f"\n--- CLOSEST TO YOU: ACTIVE NOW (last {ACTIVE_MINUTES} min) ---")
    if active:
        for c in active:
            out(line(c))
    else:
        out("Nobody from the closest players played recently.")

    out(f"\n--- CLOSEST TO YOU: ALL ({len(candidates)}) ---")
    for c in candidates:
        out(line(c))


def main():
    if not API_KEY:
        print("Error: CLASH_ROYALE_API_KEY is not set.")
        print("Set your API key before running this program.")
        return

    tag = input("Enter your Clash Royale Player Tag: ").strip().upper()

    if not tag.startswith("#"):
        tag = "#" + tag

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
            "\nNote: 'Active' means the player finished a battle in the "
            f"last {ACTIVE_MINUTES} minutes. The API has no real online "
            "status, and the battle log can lag a little."
        )
        save_report()

    except requests.RequestException as error:
        print(f"\nAPI/network error: {error}")
    except (RuntimeError, ValueError, KeyError, TypeError) as error:
        print(f"\nError: {error}")


if __name__ == "__main__":
    main()