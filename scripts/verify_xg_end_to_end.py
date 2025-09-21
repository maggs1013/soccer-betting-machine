# scripts/verify_xg_end_to_end.py
# Verify that xG made it end-to-end:
#  - hybrid xG coverage by team
#  - upcoming 7-day xG fields present
#  - league table xG fields present
# Writes: data/VERIFY_XG_REPORT.csv and prints a summary.

import os
import pandas as pd
import numpy as np

DATA = "data"
HYB  = os.path.join(DATA, "xg_metrics_hybrid.csv")
UP7  = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
LGT  = os.path.join(DATA, "LEAGUE_XG_TABLE.csv")
OUT  = os.path.join(DATA, "VERIFY_XG_REPORT.csv")

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try:
        return pd.read_csv(p)
    except Exception:
        return pd.DataFrame()

def pct(n,d): return 0.0 if d==0 else round(100.0*n/d, 1)

def main():
    hyb = safe_read(HYB)
    up7 = safe_read(UP7)
    lgt = safe_read(LGT)

    rows = []

    # Hybrid xG by team
    if not hyb.empty and {"team","xg_hybrid","xga_hybrid","xgd90_hybrid"}.issubset(hyb.columns):
        total = len(hyb)
        ok = hyb["xg_hybrid"].notna().sum()
        rows.append({"check":"hybrid_xg_teams_total","count":total,"percent":100.0})
        rows.append({"check":"hybrid_xg_teams_with_xg","count":ok,"percent":pct(ok,total)})
    else:
        rows.append({"check":"hybrid_xg_teams_total","count":0,"percent":0.0})
        rows.append({"check":"hybrid_xg_teams_with_xg","count":0,"percent":0.0})

    # Upcoming 7-day xG fields
    if not up7.empty and {"home_xg","away_xg"}.issubset(up7.columns):
        total = len(up7)
        ok = (up7["home_xg"].notna() & up7["away_xg"].notna()).sum()
        rows.append({"check":"upcoming7d_fixtures_total","count":total,"percent":100.0})
        rows.append({"check":"upcoming7d_with_xg","count":ok,"percent":pct(ok,total)})
    else:
        rows.append({"check":"upcoming7d_fixtures_total","count":0,"percent":0.0})
        rows.append({"check":"upcoming7d_with_xg","count":0,"percent":0.0})

    # League table xG fields
    if not lgt.empty and {"xgf","xga"}.issubset(lgt.columns):
        total = len(lgt)
        ok = (lgt["xgf"].notna() & lgt["xga"].notna()).sum()
        rows.append({"check":"league_table_teams_total","count":total,"percent":100.0})
        rows.append({"check":"league_table_xg_filled","count":ok,"percent":pct(ok,total)})
    else:
        rows.append({"check":"league_table_teams_total","count":0,"percent":0.0})
        rows.append({"check":"league_table_xg_filled","count":0,"percent":0.0})

    rep = pd.DataFrame(rows)
    rep.to_csv(OUT, index=False)

    print("\n=== VERIFY XG PIPELINE ===")
    for r in rows:
        print(f"{r['check']:<28}  count={r['count']:<5}  percent={r['percent']:>5.1f}%")
    print(f"[OK] wrote {OUT}")

if __name__ == "__main__":
    main()