import argparse, numpy as np, pandas as pd
from util_io import read_csv_safe, write_csv
from util_textnorm import alias_canonical

parser = argparse.ArgumentParser()
parser.add_argument("--fixtures", required=True)
parser.add_argument("--teams", required=True)
parser.add_argument("--league-xg", required=True)
parser.add_argument("--sb-xg", required=True)
parser.add_argument("--form", required=True)
parser.add_argument("--understat", required=True)
parser.add_argument("--spi", required=True)
parser.add_argument("--out", required=True)
args = parser.parse_args()

fx = read_csv_safe(args.fixtures).copy()
tm = read_csv_safe(args.teams)
lxg = read_csv_safe(args.league_xg)
sbxg = read_csv_safe(args.sb_xg)
frm = read_csv_safe(args.form)
ust = read_csv_safe(args.understat)
spi = read_csv_safe(args.spi)

for c in ["home_team","away_team"]:
    fx[c+"_norm"] = fx[c].astype(str).map(alias_canonical)

def prep(d, team_col="team"):
    if d.empty: return d
    d = d.copy()
    if team_col not in d.columns:
        return pd.DataFrame()
    d["_norm"] = d[team_col].astype(str).map(alias_canonical)
    return d

lxg = prep(lxg, "team")
sbxg = prep(sbxg, "team")
frm = prep(frm, "team")
ust = prep(ust, "team")
spi = prep(spi, "team")

def join_two(base, donor, prefix, donor_cols):
    out = base.merge(donor[["_norm"]+donor_cols].add_prefix(prefix+"_"),
                     left_on="home_team_norm", right_on=prefix+"__norm", how="left").drop(columns=[prefix+"__norm"])
    out = out.merge(donor[["_norm"]+donor_cols].add_prefix(prefix+"_a_"),
                    left_on="away_team_norm", right_on=prefix+"_a__norm", how="left").drop(columns=[prefix+"_a__norm"])
    return out

en = fx.copy()
zero_flags = []

# Helper to detect all-zero donor metric (for Understat case)
def donor_nonzero(d, cols_substr=("xg",)):
    if d.empty: return False
    cols = [c for c in d.columns if any(s in c.lower() for s in cols_substr)]
    if not cols: return False
    vals = d[cols].select_dtypes(include=[np.number]).to_numpy().ravel()
    return np.any(vals != 0)

# 1) StatsBomb first
sb_cols = [c for c in sbxg.columns if "xg" in c.lower()]
if not sbxg.empty and sb_cols:
    en = join_two(en, sbxg, "sb", sb_cols)

# 2) League table fallback
lxg_cols = [c for c in lxg.columns if c.lower() in ("xgf","xga","xg_for","xg_against","spi_off","spi_def")]
if not lxg.empty and lxg_cols:
    en = join_two(en, lxg, "lxg", lxg_cols)

# 3) Form
frm_cols = [c for c in frm.columns if "xg" in c.lower() or "form" in c.lower()]
if not frm.empty and frm_cols:
    en = join_two(en, frm, "form", frm_cols)

# 4) Understat (only if non-zero)
u_used = False
if donor_nonzero(ust):
    u_cols = [c for c in ust.columns if "xg" in c.lower()]
    if u_cols:
        en = join_two(en, ust, "u", u_cols)
        u_used = True

# 5) SPI
spi_cols = [c for c in spi.columns if "spi" in c.lower()]
if not spi.empty and spi_cols:
    en = join_two(en, spi, "spi", spi_cols)

# mark zero-source flags
zero_flags.append(f"understat_zero={'no' if u_used else 'yes' if not ust.empty else 'n/a'}")
en["zero_source_flags"] = ",".join(zero_flags)

# compute home/away xg from any donors found; fallback to medians if necessary
def first_nonzero(*vals):
    for v in vals:
        if v is None: continue
        if isinstance(v,(int,float)):
            if v != 0 and not np.isnan(v):
                return float(v)
    return None

home_xg, away_xg = [], []
for _,r in en.iterrows():
    hx = first_nonzero(
        r.get("sb_xg_for"), r.get("lxg_xgf"), r.get("form_xg_for"), r.get("u_xg_for"),
        r.get("spi_spi_off")
    )
    ax = first_nonzero(
        r.get("sb_a_xg_for"), r.get("lxg_a_xgf"), r.get("form_a_xg_for"), r.get("u_a_xg_for"),
        r.get("spi_a_spi_off")
    )
    if hx is None: hx = 1.25
    if ax is None: ax = 1.25
    home_xg.append(hx)
    away_xg.append(ax)

en["home_xg"] = np.round(home_xg,3)
en["away_xg"] = np.round(away_xg,3)
en["feature_fill_ratio"] = en[["home_xg","away_xg"]].notna().mean(axis=1)

write_csv(en, args.out)
print(f"[enrich] wrote {len(en)} rows → {args.out}")