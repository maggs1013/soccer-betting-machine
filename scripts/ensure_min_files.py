# scripts/ensure_min_files.py
import os, pandas as pd
DATA="data"
os.makedirs(DATA, exist_ok=True)

def ensure(path, cols):
    if not os.path.exists(path):
        pd.DataFrame(columns=cols).to_csv(path, index=False)
        print("[OK] created", path)
        return
    try:
        df = pd.read_csv(path)
        if len(df.columns) == 0:  # headerless / empty
            raise ValueError("no header")
    except Exception:
        pd.DataFrame(columns=cols).to_csv(path, index=False)
        print("[OK] fixed", path)

ensure(os.path.join(DATA, "teams_master.csv"), ["team","gk_rating","setpiece_rating","crowd_index"])
ensure(os.path.join(DATA, "stadiums.csv"), ["team","stadium","lat","lon"])
ensure(os.path.join(DATA, "ref_baselines.csv"), ["ref_name","ref_pen_rate"])
ensure(os.path.join(DATA, "injuries.csv"), ["date","team","injury_index"])
ensure(os.path.join(DATA, "lineups.csv"), ["date","team","key_att_out","key_def_out","keeper_changed"])
ensure(os.path.join(DATA, "team_name_map.csv"), ["raw","canonical"])
# NEW: guarantee manual_odds header exists so pandas won't crash
ensure(os.path.join(DATA, "manual_odds.csv"), ["date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec"])