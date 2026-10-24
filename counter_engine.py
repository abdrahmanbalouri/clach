"""Counter engine: scores MY decks against an enemy deck.

Data comes from counters.json:
  - "counters":       card name -> list of cards that answer it
  - "win_conditions": cards that are the enemy's main win condition
"""
import json
from pathlib import Path

DATA = Path(__file__).resolve().parent / "counters.json"


def load_data():
    with open(DATA, encoding="utf-8") as f:
        return json.load(f)


def score_deck(my_cards, enemy_cards, data=None):
    """Return how well one of my decks handles the enemy deck."""
    if data is None:
        data = load_data()
    matrix = data["counters"]
    wincons = set(data["win_conditions"])
    spells = set(data.get("spells", []))
    my = [c.lower() for c in my_cards]

    score = 0
    max_score = 0
    answers = {}
    uncovered = []
    for ec in enemy_cards:
        if ec in spells and ec not in wincons:
            continue  # damage spells can't be "countered" -> not scored
        weight = 3 if ec in wincons else 1
        max_score += 2 * weight
        counters = {c.lower() for c in matrix.get(ec, [])}
        n = min(sum(1 for m in my if m in counters), 3)
        answers[ec] = n
        score += min(n, 2) * weight
        if n == 0:
            uncovered.append(ec)
    pct = round(100 * score / max_score) if max_score else 0
    return {"pct": pct, "answers": answers, "uncovered": uncovered}


def best_deck(my_decks, enemy_cards):
    """Rank all my decks against the enemy deck, best first."""
    if not enemy_cards:
        return []
    data = load_data()
    out = []
    for d in my_decks:
        r = score_deck(d["cards"], enemy_cards, data)
        out.append({"name": d["name"], "cards": d["cards"], **r})
    out.sort(key=lambda x: -x["pct"])
    return out
