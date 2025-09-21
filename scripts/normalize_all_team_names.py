import os, pandas as pd

DATA = "data"
MAP  = os.path.join(DATA, "team_name_map.csv")

FILES = [
    "xg_understat.csv",
    "xg_statsbomb.csv",
    "team_statsbomb_features.csv",
    "sd_fbref_team_stats.csv",
    "HIST_matches.csv",
    "UPCOMING_fixtures.csv",
]

def load_map():
    p = MAP
    if not os.path.exists(p): return {}
    m = pd.read_csv(p)
    if {"raw","canonical"}.issubset(m.columns):
        return {str(r.raw).strip(): str(r.canonical).strip() for _,r in m.iterrows()}
    return {}

def apply_map(df, name_map, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().map(lambda x: name_map.get(x, x))
    return df

def main():
    nm = load_map()
    for fn in FILES:
        p = os.path.join(DATA, fn)
        if not os.path.exists(p): 
            print("[MISS]", fn); 
            continue
        try:
            df = pd.read_csv(p)
        except Exception as e:
            print("[ERR] read", fn, e); 
            continue
        if df.empty:
            print("[EMPTY]", fn); 
            continue
        # Guess team cols per file
        team_cols = []
        if fn in ("xg_understat.csv","xg_statsbomb.csv","team_statsbomb_features.csv","sd_fbref_team_stats.csv"):
            team_cols = ["team"]
        elif fn == "HIST_matches.csv":
            team_cols = ["home_team","away_team"]
        elif fn == "UPCOMING_fixtures.csv":
            team_cols = ["home_team","away_team"]
        df = apply_map(df, nm, team_cols)
        df.to_csv(p, index=False)
        print("[OK] normalized", fn, "teams in", team_cols)

if __name__ == "__main__":
    main()