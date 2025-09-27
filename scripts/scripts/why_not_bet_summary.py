#!/usr/bin/env python3
"""
why_not_bet_summary.py — Group veto reasons into % breakdown by category.

Input:
  data/WHY_NOT_BET.csv   (fixture_id, home_team, away_team, league, reasons, final_stake)
Output:
  reports/WHY_NOT_BET.md (top reasons table + per-reason samples)
"""

import os, pandas as pd

DATA="data"; REP="reports"
os.makedirs(REP, exist_ok=True)
SRC=os.path.join(DATA,"WHY_NOT_BET.csv")
OUT=os.path.join(REP,"WHY_NOT_BET.md")

def main():
    if not os.path.exists(SRC):
        with open(OUT,"w",encoding="utf-8") as f:
            f.write("# WHY_NOT_BET Summary\n\n- WHY_NOT_BET.csv missing.\n")
        print("why_not_bet_summary: WHY_NOT_BET.csv missing"); return

    df = pd.read_csv(SRC)
    if df.empty or "reasons" not in df.columns:
        with open(OUT,"w",encoding="utf-8") as f:
            f.write("# WHY_NOT_BET Summary\n\n- WHY_NOT_BET.csv empty or no 'reasons' column.\n")
        print("why_not_bet_summary: empty or missing reasons"); return

    # reasons is a semicolon-delimited string; explode to rows
    toks = df["reasons"].fillna("").astype(str).str.split(";")
    reasons_exploded = []
    for i, lst in toks.items():
        for r in lst:
            r = r.strip()
            if r:
                reasons_exploded.append({"reason": r})
    rex = pd.DataFrame(reasons_exploded)
    if rex.empty:
        with open(OUT,"w",encoding="utf-8") as f:
            f.write("# WHY_NOT_BET Summary\n\n- No non-empty reasons found.\n")
        print("why_not_bet_summary: no non-empty reasons"); return

    # Aggregate
    total = len(df)
    agg = rex.value_counts("reason").reset_index(name="count")
    agg["pct"] = (agg["count"] / total * 100.0).round(1)
    agg = agg.sort_values(["count","reason"], ascending=[False, True])

    # Build report
    lines = ["# WHY_NOT_BET Summary", ""]
    lines.append(f"- Total vetoed fixtures: **{total}**")
    lines.append("")
    lines.append("| Reason | Count | % of vetoed fixtures |")
    lines.append("|---|---:|---:|")
    for _, r in agg.iterrows():
        lines.append(f"| {r['reason']} | {int(r['count'])} | {r['pct']:.1f}% |")

    # Add a few samples per top reason (up to 3)
    lines.append("\n## Samples (top reasons)")
    top_reasons = agg.head(5)["reason"].tolist()
    for reason in top_reasons:
        samp = df[df["reasons"].fillna("").str.contains(reason)].head(3)
        lines.append(f"\n### {reason}")
        if samp.empty:
            lines.append("- (no samples)")
            continue
        for _, s in samp.iterrows():
            lines.append(f"- {s.get('league','?')}: {s.get('home_team','?')} vs {s.get('away_team','?')} — stake={s.get('final_stake','')}, reasons={s.get('reasons','')}")

    with open(OUT,"w",encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"why_not_bet_summary: wrote {OUT}")

if __name__ == "__main__":
    main()