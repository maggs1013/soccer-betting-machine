# scripts/council_briefing.py
# Builds a single Markdown dashboard for the council:
# - Headline edges (actionable with odds) and probability-only fixtures
# - Model performance (backtest Market/Elo/Blend)
# - Calibration (ECE)
# - Data coverage health
# - Context tables (league xG, recent form highlights)
#
# Output: data/COUNCIL_BRIEFING.md

import os
import pandas as pd
import numpy as np
from datetime import datetime

DATA = "data"
OUT  = os.path.join(DATA, "COUNCIL_BRIEFING.md")

# Inputs (read safely if present)
PATHS = {
    "PRED": os.path.join(DATA, "PREDICTIONS_7D.csv"),
    "ACT" : os.path.join(DATA, "ACTIONABILITY_REPORT.csv"),
    "BTS" : os.path.join(DATA, "BACKTEST_SUMMARY.csv"),
    "BTW" : os.path.join(DATA, "BACKTEST_BY_WEEK.csv"),
    "CAL" : os.path.join(DATA, "CALIBRATION_SUMMARY.csv"),
    "DQR" : os.path.join(DATA, "DATA_QUALITY_REPORT.csv"),
    "LXT" : os.path.join(DATA, "LEAGUE_XG_TABLE.csv"),
    "FORM": os.path.join(DATA, "team_form_features.csv"),
}

def safe_read_csv(p, cols=None):
    if not os.path.exists(p): 
        return pd.DataFrame(columns=cols or [])
    try:
        df = pd.read_csv(p)
        if cols:
            for c in cols:
                if c not in df.columns:
                    df[c] = np.nan
        return df
    except Exception:
        return pd.DataFrame(columns=cols or [])

def top_table(df, cols, n=10):
    if df.empty: return "*(none)*"
    return df[cols].head(n).to_markdown(index=False)

def section(title): return f"\n\n## {title}\n\n"

def main():
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    pred = safe_read_csv(PATHS["PRED"], ["date","home_team","away_team","pH","pD","pA","kelly_H","kelly_D","kelly_A"])
    act  = safe_read_csv(PATHS["ACT"],  ["date","home_team","away_team","has_odds","pH","pD","pA","kelly_H","kelly_D","kelly_A"])
    bts  = safe_read_csv(PATHS["BTS"],  ["model","metric","value"])
    btw  = safe_read_csv(PATHS["BTW"],  ["week","model","logloss","brier"])
    cal  = safe_read_csv(PATHS["CAL"],  ["metric","value"])
    dqr  = safe_read_csv(PATHS["DQR"],  ["metric","count","total","percent"])
    lxt  = safe_read_csv(PATHS["LXT"],  ["league","team","gp","pts","xgf","xga","xg_diff","clinicality","def_overperf"])
    form = safe_read_csv(PATHS["FORM"], ["team","last5_ppg","last10_ppg","last5_xgpg","last5_xgapg","last10_xgpg","last10_xgapg"])

    lines = []
    lines.append(f"# Council Briefing — {now}")

    # 1) Headline bets (actionable edges)
    lines.append(section("Headline: Actionable Edges (with odds)"))
    actionable = pd.DataFrame()
    if not act.empty:
        # Best Kelly across H/D/A
        act["top_kelly"] = act[["kelly_H","kelly_D","kelly_A"]].fillna(0).max(axis=1)
        actionable = act[act["has_odds"]==True].sort_values("top_kelly", ascending=False)
        if not actionable.empty:
            show = actionable[["date","home_team","away_team","top_kelly","pH","pD","pA"]].copy()
            show["date"] = pd.to_datetime(show["date"], errors="coerce").dt.strftime("%Y-%m-%d")
            lines.append(top_table(show, ["date","home_team","away_team","top_kelly","pH","pD","pA"], n=12))
        else:
            lines.append("*(no odds present — probability-only slate today)*")
    else:
        lines.append("*(actionability file missing or empty)*")

    # 2) Probability-only fixtures (no odds yet)
    lines.append(section("Probability-only Fixtures (no odds — monitor only)"))
    prob_only = pd.DataFrame()
    if not act.empty:
        prob_only = act[act["has_odds"]!=True].copy()
        if not prob_only.empty:
            show = prob_only.sort_values("date")
            show["date"] = pd.to_datetime(show["date"], errors="coerce").dt.strftime("%Y-%m-%d")
            lines.append(top_table(show, ["date","home_team","away_team","pH","pD","pA"], n=20))
        else:
            lines.append("*(none)*")
    else:
        lines.append("*(actionability file missing or empty)*")

    # 3) Model performance (backtest)
    lines.append(section("Model Performance — Backtest (Out-of-sample)"))
    if not bts.empty:
        # pivot summary
        piv = bts.pivot_table(index="model", columns="metric", values="value", aggfunc="mean")
        piv = piv[["logloss","brier"]] if set(["logloss","brier"]).issubset(piv.columns) else piv
        lines.append(piv.reset_index().to_markdown(index=False))
    else:
        lines.append("*(backtest summary missing)*")
    if not btw.empty:
        # last 6 weeks glance
        lines.append("\n*Last 6 weeks (logloss by model):*")
        last6 = (btw.sort_values("week")
                    .groupby("model")
                    .tail(6)
                    .sort_values(["week","model"]))
        lines.append(top_table(last6, ["week","model","logloss","brier"], n=18))

    # 4) Calibration (reliability)
    lines.append(section("Calibration — Reliability"))
    if not cal.empty:
        ece_row = cal[cal["metric"].str.contains("ECE", case=False, na=False)]
        if not ece_row.empty:
            ece_val = float(ece_row["value"].values[0])
            lines.append(f"- **ECE (home)**: `{ece_val:.4f}` (lower is better)")
        else:
            lines.append("- ECE not found.")
    else:
        lines.append("*(calibration summary missing)*")

    # 5) Data quality (coverage)
    lines.append(section("Data Coverage — Health Check"))
    if not dqr.empty:
        # show key coverage rows
        key_rows = dqr[dqr["metric"].isin([
            "odds_coverage","xg_coverage","priors_gk","priors_setpiece","injuries_index","lineup_flags_present"
        ])].copy()
        if not key_rows.empty:
            key_rows["percent"] = key_rows["percent"].map(lambda x: f"{x:.1f}%")
            lines.append(key_rows[["metric","count","total","percent"]].to_markdown(index=False))
        else:
            lines.append("*(no key rows — see DATA_QUALITY_REPORT.csv)*")
    else:
        lines.append("*(data quality report missing)*")

    # 6) Context — league xG table highlights
    lines.append(section("Context — League xG Highlights"))
    if not lxt.empty:
        # Top xG Diff
        best = lxt.sort_values("xg_diff", ascending=False).head(10)
        worst = lxt.sort_values("xg_diff", ascending=True).head(10)
        lines.append("**Top 10 xG Diff (season)**")
        lines.append(top_table(best, ["league","team","pts","xgf","xga","xg_diff"], n=10))
        lines.append("\n**Bottom 10 xG Diff (season)**")
        lines.append(top_table(worst, ["league","team","pts","xgf","xga","xg_diff"], n=10))
        # Clinicality extremes
        lines.append("\n**Clinicality (GF−xGF) extremes**")
        up = lxt.sort_values("clinicality", ascending=False).head(10)
        dn = lxt.sort_values("clinicality", ascending=True).head(10)
        lines.append(top_table(up, ["league","team","clinicality"], n=10))
        lines.append(top_table(dn, ["league","team","clinicality"], n=10))
    else:
        lines.append("*(league xG table missing)*")

    # 7) Form highlights
    lines.append(section("Form — Last 5/10 (xG & Points)"))
    if not form.empty:
        # illustrate top recent xGPG and PPG
        f1 = form.sort_values("last5_xgpg", ascending=False).head(10)
        f2 = form.sort_values("last5_ppg", ascending=False).head(10)
        lines.append("**Top 10 xG per game (last 5)**")
        lines.append(top_table(f1, ["team","last5_xgpg","last5_xgapg"], n=10))
        lines.append("\n**Top 10 PPG (last 5)**")
        lines.append(top_table(f2, ["team","last5_ppg","last10_ppg"], n=10))
    else:
        lines.append("*(form features missing)*")

    # 8) Council prompts
    lines.append(section("Council — Prompts by Seat"))
    lines.append("- **Markets**: Review `ACTIONABILITY_REPORT.csv`. Which edges survive your price sensitivity? Any steam to avoid?")
    lines.append("- **Data Science**: Are ECE and backtest metrics satisfactory today? If not, flag fixtures as “observe-only”.")
    lines.append("- **Tactics**: Use `LEAGUE_XG_TABLE.csv` + `team_statsbomb_features.csv`. Any red-flag mismatches (GK carrying, set-piece dependency)?")
    lines.append("- **Risk**: Confirm Kelly caps & daily outlay. Any cluster risk (correlated outcomes) to trim?")

    os.makedirs(DATA, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[OK] wrote {OUT}")

if __name__ == "__main__":
    main()