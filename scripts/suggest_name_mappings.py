# scripts/suggest_name_mappings.py
# Suggests new entries for data/team_name_map.csv by comparing team names
# seen in various sources vs canonical names known to the repo.

import os, pandas as pd, difflib

DATA = "data"
MAP  = os.path.join(DATA, "team_name_map.csv")
FILES = [
    ("HIST_matches.csv", ["home_team","away_team"]),
    ("UPCOMING_fixtures.csv", ["home_team","away_team"]),
    ("xg_understat.csv", ["team"]),
    ("xg_statsbomb.csv", ["team"]),
    ("team_statsbomb_features.csv", ["team"]),
    ("sd_fbref_team_stats.csv", ["team"]),
]

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except: return pd.DataFrame()

def main():
    # canonical universe: from teams in hybrid/StatsBomb/Understat + existing map targets
    canon = set()
    hyb = safe_read(os.path.join(DATA,"xg_metrics_hybrid.csv"))
    if not hyb.empty and "team" in hyb.columns:
        canon |= set(hyb["team"].dropna().astype(str).str.strip().unique())

    tsb = safe_read(os.path.join(DATA,"team_statsbomb_features.csv"))
    if not tsb.empty and "team" in tsb.columns:
        canon |= set(tsb["team"].dropna().astype(str).str.strip().unique())

    ust = safe_read(os.path.join(DATA,"xg_understat.csv"))
    if not ust.empty and "team" in ust.columns:
        canon |= set(ust["team"].dropna().astype(str).str.strip().unique())

    # existing map
    name_map = {}
    m = safe_read(MAP)
    if not m.empty and {"raw","canonical"}.issubset(m.columns):
        name_map = {str(r.raw).strip(): str(r.canonical).strip() for _, r in m.iterrows()}
        canon |= set(name_map.values())

    # scan sources
    seen = set()
    for fn, cols in FILES:
        df = safe_read(os.path.join(DATA, fn))
        for c in cols:
            if c in df.columns:
                vals = df[c].dropna().astype(str).str.strip().unique()
                seen |= set(vals)

    # exclude already canonical/mapped
    raw_candidates = [s for s in seen if s not in canon and s not in name_map.keys()]
    if not raw_candidates:
        print("[OK] No new team names needing mapping.")
        return

    print("=== Suggested mappings (append to data/team_name_map.csv) ===")
    canon_list = list(canon)
    for raw in sorted(raw_candidates):
        best = difflib.get_close_matches(raw, canon_list, n=3, cutoff=0.6)
        if best:
            print(f"{raw} -> {best[0]}   (alts: {', '.join(best[1:])})")
        else:
            print(f"{raw} -> <ADD CANONICAL NAME>")

if __name__ == "__main__":
    main()