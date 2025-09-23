#!/usr/bin/env python3
import os, json, pandas as pd
from datetime import datetime

RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

ODDS_SNAPSHOT = os.path.join("data", "odds_upcoming.csv")               # optional
ENRICHED      = os.path.join("outputs", "UPCOMING_7D_enriched.csv")     # optional fallback
API_PROBE     = os.path.join("data", "api_probe_report.json")           # optional
OUT           = os.path.join(RUN_DIR, "EXECUTION_FEASIBILITY.csv")

def read_csv(path):
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return pd.read_csv(path)
    return pd.DataFrame()

def load_api_probe(path):
    if os.path.exists(path) and os.path.getsize(path) > 0:
        try:
            return json.load(open(path))
        except Exception:
            pass
    return {}

def build_from_odds(df):
    """Compute feasibility from an odds table."""
    # expected columns: fixture_id, league, (optionally) num_books, or raw book rows you can count
    if df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "num_books" not in out.columns:
        # if odds are row-per-book, you can count per fixture_id:
        if "fixture_id" in out.columns and "book" in out.columns:
            cnt = out.groupby("fixture_id")["book"].nunique().rename("num_books")
            out = out.merge(cnt, on="fixture_id", how="left")
        else:
            out["num_books"] = 0
    if "league" not in out.columns:
        out["league"] = "unknown"
    agg = (out.groupby(["fixture_id","league"], as_index=False)["num_books"]
              .max())  # highest book coverage seen per fixture
    agg["feasible"] = (agg["num_books"] >= 2).astype(int)
    agg["note"]     = agg["num_books"].map(lambda n: "ok" if n>=2 else "thin")
    return agg

def build_from_enriched(df):
    """Compute feasibility from enriched fixtures when explicit odds CSV is missing.
       We look for *_prob or a book_count/market_sources column; otherwise we mark no_odds."""
    if df.empty:
        return pd.DataFrame()
    out = pd.DataFrame()
    out["fixture_id"] = df.get("fixture_id", pd.Series(range(len(df))))
    out["league"]     = df.get("league", "unknown")
    # infer coverage
    probs_cols = [c for c in df.columns if c.endswith("_prob")]  # home_prob/draw_prob/away_prob etc.
    if "book_count" in df.columns:
        num_books = df["book_count"]
    elif "market_sources" in df.columns:
        # market_sources might be a semi-colon list of sources
        num_books = df["market_sources"].fillna("").astype(str).map(lambda s: len([x for x in s.split(";") if x]))
    else:
        num_books = pd.Series(0, index=df.index)
    out["num_books"] = num_books.fillna(0).astype(int)

    # rules:
    #  - if *_prob exist and num_books>=2 -> feasible=1 (ok)
    #  - if *_prob exist and num_books<2  -> feasible=0 (thin)
    #  - if no *_prob -> feasible=0 (no_odds)
    have_probs = (len(probs_cols) >= 3)
    out["feasible"] = 0
    out["note"] = "no_odds"
    if have_probs:
        out.loc[out["num_books"] >= 2, ["feasible","note"]] = [1, "ok"]
        out.loc[(out["num_books"] < 2), ["feasible","note"]] = [0, "thin"]
    return out.drop_duplicates("fixture_id")

def main():
    # 1) Try dedicated odds CSV
    odds = read_csv(ODDS_SNAPSHOT)
    if not odds.empty:
        feas = build_from_odds(odds)
        source = "odds_csv"
    else:
        # 2) Fallback to enriched fixtures
        enr = read_csv(ENRICHED)
        feas = build_from_enriched(enr)
        source = "enriched"

    # 3) Optional API probe flags for observability
    probe = load_api_probe(API_PROBE)
    odds_api_ok = probe.get("odds_api_alive") or probe.get("odds_api_status") == "ok"
    feas["source"] = source
    feas["odds_api_ok"] = bool(odds_api_ok)

    # 4) If still empty, emit a non-failing placeholder so pipeline continues
    if feas.empty:
        feas = pd.DataFrame([{
            "fixture_id": None,
            "league": "unknown",
            "num_books": 0,
            "feasible": 0,
            "note": "no_fixtures_or_odds",
            "source": "none",
            "odds_api_ok": bool(odds_api_ok)
        }])

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    feas.to_csv(OUT, index=False)
    print(f"EXECUTION_FEASIBILITY.csv written: {len(feas)} rows from {source}")

if __name__ == "__main__":
    main()