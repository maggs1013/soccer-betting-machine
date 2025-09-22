# scripts/00_build_teams_master.py
# Build/seed data/teams_master.csv (GK, set-piece, crowd priors) from available sources.
# - Reads team lists from hybrid xG, Understat, StatsBomb, FBref, HIST/UPCOMING
# - Applies both team_name_map.csv and teams_alias_overrides.csv
# - Derives priors:
#     gk_rating          ~ 0.75 + 0.10 * (psxg_minus_goals_sb)   -> clamped [0.55, 0.92]
#     setpiece_rating    ~ 0.55 + 0.25 * (setpiece_xg_share)     -> clamped [0.50, 0.90]
#     crowd_index        ~ default 0.70 (hand-tune later)
# - Deduplicates team names BEFORE any reindex/merge (prevents InvalidIndexError)
# - Writes data/teams_master.csv (overwrites)

import os
import pandas as pd
import numpy as np

DATA = "data"
OUT  = os.path.join(DATA, "teams_master.csv")

# Optional sources (read safely)
SRC = {
    "hybrid":          os.path.join(DATA, "xg_metrics_hybrid.csv"),
    "understat":       os.path.join(DATA, "xg_understat.csv"),
    "sb_totals":       os.path.join(DATA, "xg_statsbomb.csv"),
    "sb_features":     os.path.join(DATA, "team_statsbomb_features.csv"),
    "fbref":           os.path.join(DATA, "sd_fbref_team_stats.csv"),
    "hist":            os.path.join(DATA, "HIST_matches.csv"),
    "upcoming":        os.path.join(DATA, "UPCOMING_fixtures.csv"),
    "name_map":        os.path.join(DATA, "team_name_map.csv"),
    "alias_overrides": os.path.join(DATA, "teams_alias_overrides.csv"),
}

def safe_read_csv(path, cols=None):
    if not os.path.exists(path):
        return pd.DataFrame(columns=cols or [])
    try:
        df = pd.read_csv(path)
        if cols:
            for c in cols:
                if c not in df.columns:
                    df[c] = np.nan
        return df
    except Exception:
        return pd.DataFrame(columns=cols or [])

def clamp(v, lo, hi):
    try:
        v = float(v)
    except Exception:
        return (lo + hi) / 2.0
    return max(lo, min(hi, v))

def load_name_maps():
    nm = {}
    for p in [SRC["name_map"], SRC["alias_overrides"]]:
        df = safe_read_csv(p, ["raw","canonical"])
        if not df.empty and {"raw","canonical"}.issubset(df.columns):
            for r in df.itertuples(index=False):
                raw = str(getattr(r, "raw")).strip()
                can = str(getattr(r, "canonical")).strip()
                if raw and can:
                    nm[raw] = can  # overrides win because alias_overrides loaded last
    return nm

def apply_map(series, nm):
    if series is None: 
        return series
    s = series.astype(str).str.strip()
    return s.map(lambda x: nm.get(x, x))

def gather_team_universe(nm):
    names = []

    # Hybrid xG
    hyb = safe_read_csv(SRC["hybrid"], ["team"])
    if not hyb.empty and "team" in hyb.columns:
        names.append(apply_map(hyb["team"], nm))

    # Understat
    ust = safe_read_csv(SRC["understat"], ["team"])
    if not ust.empty and "team" in ust.columns:
        names.append(apply_map(ust["team"], nm))

    # StatsBomb totals
    sbt = safe_read_csv(SRC["sb_totals"], ["team"])
    if not sbt.empty and "team" in sbt.columns:
        names.append(apply_map(sbt["team"], nm))

    # StatsBomb features
    sbf = safe_read_csv(SRC["sb_features"], ["team"])
    if not sbf.empty and "team" in sbf.columns:
        names.append(apply_map(sbf["team"], nm))

    # FBref teams
    fbr = safe_read_csv(SRC["fbref"], ["team"])
    if not fbr.empty and "team" in fbr.columns:
        names.append(apply_map(fbr["team"], nm))

    # HIST (home/away)
    hist = safe_read_csv(SRC["hist"], ["home_team","away_team"])
    if not hist.empty:
        names.append(apply_map(hist["home_team"], nm))
        names.append(apply_map(hist["away_team"], nm))

    # UPCOMING (home/away)
    upc = safe_read_csv(SRC["upcoming"], ["date","home_team","away_team"])
    if not upc.empty:
        names.append(apply_map(upc["home_team"], nm))
        names.append(apply_map(upc["away_team"], nm))

    if not names:
        return pd.DataFrame(columns=["team"])

    # Concatenate, drop NA, strip, deduplicate
    allnames = pd.concat(names, ignore_index=True).dropna()
    allnames = allnames.astype(str).str.strip()
    allnames = pd.Series(allnames.unique(), name="team").to_frame()
    allnames = allnames[allnames["team"] != ""].reset_index(drop=True)
    return allnames

def derive_priors(allteams, nm):
    # Defaults
    pri = allteams.copy()
    pri["gk_rating"] = 0.75
    pri["setpiece_rating"] = 0.55
    pri["crowd_index"] = 0.70

    # StatsBomb features (xA, psxg_minus_goals, setpiece vs openplay) → derive GK & set-piece priors
    sbf = safe_read_csv(SRC["sb_features"], ["team","psxg_minus_goals_sb","setpiece_xg_sb","openplay_xg_sb"])
    if not sbf.empty:
        sbf["team"] = apply_map(sbf["team"], nm)
        sbf = (sbf.groupby("team", as_index=False)
                    .agg(psxg=("psxg_minus_goals_sb","mean"),
                         sp=("setpiece_xg_sb","mean"),
                         op=("openplay_xg_sb","mean")))

        # GK rating from shot-stopping
        sbf["gk_rating"] = sbf["psxg"].map(lambda v: clamp(0.75 + 0.10*float(v if pd.notna(v) else 0.0), 0.55, 0.92))
        # Set-piece rating from share of xG from set pieces
        share = np.where((sbf["sp"]+sbf["op"])>0, sbf["sp"]/(sbf["sp"]+sbf["op"]), 0.0)
        sbf["setpiece_rating"] = [clamp(0.55 + 0.25*float(x), 0.50, 0.90) for x in share]
        sbf = sbf[["team","gk_rating","setpiece_rating"]]

        pri = pri.merge(sbf, on="team", how="left", suffixes=("","_sb"))
        # prefer SB-derived columns where present
        for c in ["gk_rating","setpiece_rating"]:
            pri[c] = np.where(pri[f"{c}_sb"].notna(), pri[f"{c}_sb"], pri[c])
            pri.drop(columns=[f"{c}_sb"], inplace=True)

    # If you want, fold hybrid strength for minor nudges (optional, gentle)
    hyb = safe_read_csv(SRC["hybrid"], ["team","xgd90_hybrid"])
    if not hyb.empty:
        hyb["team"] = apply_map(hyb["team"], nm)
        # small bonus for very strong teams to set-piece rating (they likely draw more set-piece xG)
        hyb["sp_bonus"] = hyb["xgd90_hybrid"].map(lambda v: 0.02*float(v) if pd.notna(v) else 0.0)
        hyb["sp_bonus"] = hyb["sp_bonus"].clip(-0.03, 0.03)
        pri = pri.merge(hyb[["team","sp_bonus"]], on="team", how="left")
        pri["setpiece_rating"] = pri["setpiece_rating"] + pri["sp_bonus"].fillna(0.0)
        pri["setpiece_rating"] = pri["setpiece_rating"].map(lambda v: clamp(v, 0.50, 0.90))
        pri.drop(columns=["sp_bonus"], inplace=True, errors="ignore")

    # Final clamps
    pri["gk_rating"] = pri["gk_rating"].map(lambda v: clamp(v, 0.55, 0.92))
    pri["setpiece_rating"] = pri["setpiece_rating"].map(lambda v: clamp(v, 0.50, 0.90))
    pri["crowd_index"] = pri["crowd_index"].map(lambda v: clamp(v, 0.50, 0.98))

    return pri[["team","gk_rating","setpiece_rating","crowd_index"]].drop_duplicates("team").reset_index(drop=True)

def main():
    os.makedirs(DATA, exist_ok=True)

    # 1) Load name maps (main + overrides) and build canonical team list
    name_map = load_name_maps()
    allteams = gather_team_universe(name_map)
    if allteams.empty:
        # still write a valid file (headers only)
        pd.DataFrame(columns=["team","gk_rating","setpiece_rating","crowd_index"]).to_csv(OUT, index=False)
        print("[WARN] No teams found across sources; wrote header-only teams_master.csv")
        return

    # 2) Derive priors safely (no duplicate index ops)
    pri = derive_priors(allteams, name_map)

    # 3) Save
    pri.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} rows={len(pri)}")

if __name__ == "__main__":
    main()