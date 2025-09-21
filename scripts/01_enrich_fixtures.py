import argparse, json, numpy as np, pandas as pd
from util_io import read_csv_safe, write_csv
from util_textnorm import alias_canonical, normalize_team_name
from util_match import soft_join

def add_norm(df, col):
    df = df.copy()
    df["_norm"] = df[col].map(alias_canonical)
    return df

def goals_from_odds(pH, pD, pA, league_avg_g=2.5):
    # very light heuristic: split expected goals proportional to odds strengths
    # avoid div-by-zero:
    eps = 1e-9
    strength_h = (pH + 0.5*pD) / max(pA + 0.5*pD, eps)
    ratio = np.sqrt(strength_h)
    lam_h = league_avg_g * ratio / (1 + ratio)
    lam_a = league_avg_g - lam_h
    return float(lam_h), float(lam_a)

parser = argparse.ArgumentParser()
parser.add_argument("--fixtures", required=True)
parser.add_argument("--teams", required=True)
parser.add_argument("--league-xg", required=True)
parser.add_argument("--sb-xg", required=True)
parser.add_argument("--form", required=True)
parser.add_argument("--understat", required=True)
parser.add_argument("--spi", required=True)
parser.add_argument("--odds", required=True)
parser.add_argument("--out", required=True)
args = parser.parse_args()

fx = read_csv_safe(args.fixtures)
tm = read_csv_safe(args.teams)
lxg = read_csv_safe(args.league_xg)
sbxg = read_csv_safe(args.sb_xg)
frm = read_csv_safe(args.form)
ust = read_csv_safe(args.understat)
spi = read_csv_safe(args.spi)
odds = read_csv_safe(args.odds)

# Normalize team names
for col in ["home_team","away_team"]:
    fx[f"{col}_norm"] = fx[col].map(alias_canonical)

# Prepare donor tables with normalized keys
def prep(d, key):
    if d.empty or key not in d.columns:
        return d
    d = d.copy()
    d["_norm"] = d[key].map(alias_canonical)
    return d

lxg = prep(lxg, "team")
sbxg = prep(sbxg, "team")
frm = prep(frm, "team")
ust = prep(ust, "team")
spi = prep(spi, "team")
tm = prep(tm, "canonical_team")

# Join helpers for home/away from a donor table
def join_two(fx, donor, cols_prefix, donor_cols):
    # home
    out = fx.merge(donor[["_norm"]+donor_cols].add_prefix(cols_prefix+"_"),
                   left_on="home_team_norm", right_on=cols_prefix+"__norm", how="left")
    out = out.drop(columns=[cols_prefix+"__norm"])
    # away
    out = out.merge(donor[["_norm"]+donor_cols].add_prefix(cols_prefix+"_a_"),
                    left_on="away_team_norm", right_on=cols_prefix+"_a__norm", how="left")
    out = out.drop(columns=[cols_prefix+"_a__norm"])
    return out

en = fx.copy()

# 1) League xG table as primary fallback (xgf/xga or similar)
lxg_cols = [c for c in lxg.columns if c.lower() in ("xgf","xga","xg_for","xg_against","g_for","g_against","spi_off","spi_def")]
if not lxg.empty and lxg_cols:
    en = join_two(en, lxg.rename(columns={"team":"team"}), "lxg", lxg_cols)

# 2) StatsBomb features (per-90 xG if present)
sb_cols = [c for c in sbxg.columns if "xg" in c.lower() or "shot" in c.lower()]
if not sbxg.empty and sb_cols:
    en = join_two(en, sbxg, "sb", sb_cols)

# 3) Understat (fix zeros later)
ust_cols = [c for c in ust.columns if "xg" in c.lower()]
if not ust.empty and ust_cols:
    # Understat table had all zeros earlier — still join to keep structure
    en = join_two(en, ust, "u", ust_cols)

# 4) Form features (rolling xG/xGA)
frm_cols = [c for c in frm.columns if "xg" in c.lower() or "form" in c.lower()]
if not frm.empty and frm_cols:
    en = join_two(en, frm, "form", frm_cols)

# 5) SPI (strengths)
spi_cols = [c for c in spi.columns if "spi" in c.lower() or "rating" in c.lower()]
if not spi.empty and spi_cols:
    en = join_two(en, spi, "spi", spi_cols)

# 6) Add odds if we have them
if not odds.empty:
    # Expect columns: match_id or (home_team, away_team, date), and odds_home, odds_draw, odds_away OR implied probs
    # We soft-merge by normalized team names + date.
    for c in ("home_team","away_team"):
        if c in odds.columns:
            odds[c+"_norm"] = odds[c].map(alias_canonical)
    keys = [k for k in ("date","match_date") if k in en.columns and k in odds.columns]
    if keys:
        o = en.merge(odds, left_on=["home_team_norm","away_team_norm",keys[0]],
                          right_on=[col for col in ["home_team_norm","away_team_norm",keys[0]]], how="left", suffixes=("","_od"))
    else:
        o = en.merge(odds, left_on=["home_team_norm","away_team_norm"],
                          right_on=[col for col in ["home_team_norm","away_team_norm"]], how="left", suffixes=("","_od"))
    en = o

# Compute home/away expected goals from donors
def safe_first(*vals):
    for v in vals:
        if v is not None and not (isinstance(v,float) and (np.isnan(v) or np.isinf(v))):
            if isinstance(v, (int,float)) and v==0:
                continue
            return v
    return None

home_xg = []
away_xg = []

for _,row in en.iterrows():
    # try direct per-team xG (league/StatsBomb/Understat/form)
    hx = safe_first(
        row.get("sb_xg_for"), row.get("lxg_xgf"), row.get("form_xg_for"), row.get("u_xg_for")
    )
    ax = safe_first(
        row.get("sb_a_xg_for"), row.get("lxg_a_xgf"), row.get("form_a_xg_for"), row.get("u_a_xg_for")
    )

    # odds-derived if missing
    if (hx is None or ax is None):
        pH = row.get("odds_home_prob") or row.get("home_prob") or None
        pD = row.get("odds_draw_prob") or row.get("draw_prob") or None
        pA = row.get("odds_away_prob") or row.get("away_prob") or None
        if pH and pD and pA:
            lam_h, lam_a = goals_from_odds(float(pH), float(pD), float(pA))
            if hx is None: hx = lam_h
            if ax is None: ax = lam_a

    # league medians fallback
    if hx is None: hx = 1.25
    if ax is None: ax = 1.25

    home_xg.append(hx)
    away_xg.append(ax)

en["home_xg"] = np.round(home_xg, 3)
en["away_xg"] = np.round(away_xg, 3)

# sanity
en["feature_fill_ratio"] = en[["home_xg","away_xg"]].notna().mean(axis=1)

write_csv(en, args.out)
print(f"Wrote enriched fixtures to {args.out} with {len(en)} rows.")