import os, pandas as pd

DATA = "data"
MAP_MAIN = os.path.join(DATA, "team_name_map.csv")
MAP_OVR  = os.path.join(DATA, "teams_alias_overrides.csv")

FILES = [
    ("HIST_matches.csv", ["home_team","away_team"]),
    ("UPCOMING_fixtures.csv", ["home_team","away_team"]),
    ("xg_understat.csv", ["team"]),
    ("xg_statsbomb.csv", ["team"]),
    ("team_statsbomb_features.csv", ["team"]),
    ("sd_fbref_team_stats.csv", ["team"]),
    ("xg_metrics_hybrid.csv", ["team"]),  # safe to normalize before merges that use this
]

def load_map(p):
    if not os.path.exists(p): return {}
    m = pd.read_csv(p)
    if {"raw","canonical"}.issubset(m.columns):
        return {str(r.raw).strip(): str(r.canonical).strip() for _, r in m.iterrows()}
    return {}

def apply_map(df, name_map, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().map(lambda x: name_map.get(x, x))
    return df

def main():
    nm = {}
    nm.update(load_map(MAP_MAIN))
    nm.update(load_map(MAP_OVR))  # overrides win

    for fn, cols in FILES:
        p = os.path.join(DATA, fn)
        if not os.path.exists(p): 
            print("[MISS]", fn); 
            continue
        try:
            df = pd.read_csv(p)
        except Exception as e:
            print("[ERR]", fn, e); 
            continue
        if df.empty:
            print("[EMPTY]", fn); 
            continue
        df = apply_map(df, nm, cols)
        df.to_csv(p, index=False)
        print("[OK] normalized", fn, "cols", cols)

if __name__ == "__main__":
    main()