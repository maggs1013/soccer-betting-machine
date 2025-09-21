import argparse, os, glob, pandas as pd
from collections import OrderedDict
from util_io import read_csv_safe, write_csv
from util_textnorm import alias_canonical, normalize_team_name

SOURCES = OrderedDict({
    "UPCOMING_fixtures": {"path":"data/UPCOMING_fixtures.csv", "cols":["home_team","away_team","league"]},
    "HIST_matches":      {"path":"data/HIST_matches.csv",      "cols":["home_team","away_team","league"]},
    "LEAGUE_XG_TABLE":   {"path":"data/LEAGUE_XG_TABLE.csv",   "cols":["team","league"]},
    "xg_statsbomb":      {"path":"data/xg_statsbomb.csv",      "cols":["team","league"]},
    "team_statsbomb":    {"path":"data/team_statsbomb_features.csv", "cols":["team","league"]},
    "xg_understat":      {"path":"data/xg_understat.csv",      "cols":["team","league"]},
    "form_features":     {"path":"data/team_form_features.csv","cols":["team","league"]},
    "sd_538_spi":        {"path":"data/sd_538_spi.csv",        "cols":["team","league"]},
    "sd_fbref":          {"path":"data/sd_fbref_team_stats.csv","cols":["team","league"]},
})

def harvest_names():
    rows = []
    for name, spec in SOURCES.items():
        df = read_csv_safe(spec["path"])
        if df.empty: continue
        cols = [c for c in spec["cols"] if c in df.columns]
        if not cols: continue
        # collect team-like columns
        team_candidates = []
        for c in cols:
            if df[c].dtype == object:
                team_candidates.append(c)
        for c in team_candidates:
            subset = df[[c] + ([ "league"] if "league" in df.columns else [])].copy()
            subset = subset.rename(columns={c:"source_name"})
            subset["source"] = name
            rows.append(subset)
    if not rows:
        return pd.DataFrame(columns=["source_name","league","source"])
    allnames = pd.concat(rows, ignore_index=True).dropna(subset=["source_name"])
    # normalize & canonical
    allnames["norm"] = allnames["source_name"].map(normalize_team_name)
    allnames["canonical_team"] = allnames["source_name"].map(alias_canonical)
    # best-effort league fill
    if "league" not in allnames.columns:
        allnames["league"] = "unknown"
    # dedupe by canonical
    allnames = (allnames
        .sort_values(["canonical_team","source"])
        .drop_duplicates(["canonical_team"])
        .loc[:, ["source_name","canonical_team","norm","league","source"]])
    return allnames

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/teams_master.csv")
    ap.add_argument("--overrides", default="data/teams_alias_overrides.csv")
    args = ap.parse_args()

    base = harvest_names()

    # merge overrides (manual always wins)
    ov = read_csv_safe(args.overrides)
    if not ov.empty:
        ov["norm"] = ov["source_name"].map(normalize_team_name)
        base = base.drop(columns=["canonical_team"], errors="ignore")
        base = base.merge(ov[["norm","canonical_team","league"]].drop_duplicates("norm"),
                          on="norm", how="left", suffixes=("","_ov"))
        base["canonical_team"] = base["canonical_team"].fillna(base["canonical_team_ov"])
        base["league"] = base["league"].fillna(base["league_ov"])
        base = base.drop(columns=[c for c in base.columns if c.endswith("_ov")])

    # final dedupe & write
    base = base.sort_values(["canonical_team","source"]).drop_duplicates(["canonical_team"])
    write_csv(base[["source_name","canonical_team","norm","league","source"]], args.out)
    print(f"[teams_master] wrote {len(base)} rows to {args.out}")