# scripts/fetch_statsbomb_xg.py
# Robust StatsBomb Open Data fetch with coverage logging:
# - team xG (for) and xGA (against)
# - team xA (sum xG of shots assisted by that team's passes)
# - GK shot-stopping (PSxG - Goals conceded)
# - set-piece vs open-play xG split
#
# Writes:
#   data/xg_statsbomb.csv                (team xG/xGA totals)
#   data/team_statsbomb_features.csv     (xA, psxg_minus_goals, setpiece_xg, openplay_xg)

import os
import pandas as pd
from statsbombpy import sb

DATA = "data"
OUT_XG   = os.path.join(DATA, "xg_statsbomb.csv")
OUT_FEAT = os.path.join(DATA, "team_statsbomb_features.csv")

os.makedirs(DATA, exist_ok=True)

def to_df(obj):
    """Force return as DataFrame; otherwise empty DF."""
    if isinstance(obj, pd.DataFrame):
        return obj
    try:
        return pd.DataFrame(obj)
    except Exception:
        return pd.DataFrame()

def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [".".join([str(x) for x in tup if x not in (None, "")]) for tup in df.columns]
    return df

def get_series(df: pd.DataFrame, candidates):
    """Return the first existing column as a Series; else None."""
    for c in candidates:
        if c in df.columns:
            return df[c]
    return None

def safe_events(match_id):
    try:
        ev = sb.events(match_id=match_id)
        ev = to_df(ev)
        ev = flatten_columns(ev)
        return ev
    except Exception as e:
        print(f"[WARN] events failed for match {match_id}: {e}")
        return pd.DataFrame()

def main():
    # coverage counters
    comps_cnt = 0
    matches_cnt = 0
    events_cnt = 0
    shots_rows = 0
    teams_counted = set()

    try:
        comps = sb.competitions()
    except Exception as e:
        print("[WARN] StatsBomb comps fetch failed:", e)
        pd.DataFrame(columns=["team","competition","season_name","xg_sb","xga_sb"]).to_csv(OUT_XG, index=False)
        pd.DataFrame(columns=["team","competition","season_name","xa_sb","psxg_minus_goals_sb","setpiece_xg_sb","openplay_xg_sb"]).to_csv(OUT_FEAT, index=False)
        return

    rows_xg, rows_feat = [], []

    for _, c in comps.iterrows():
        comp_id   = int(c["competition_id"])
        season_id = int(c["season_id"])
        comp_name = str(c["competition_name"])
        season_nm = str(c["season_name"])
        comps_cnt += 1

        try:
            matches = sb.matches(competition_id=comp_id, season_id=season_id)
        except Exception as e:
            print(f"[WARN] matches failed for {comp_name} {season_nm}: {e}")
            continue

        team_xg   = {}  # (team, comp, season) -> {"xg":float, "xga":float}
        team_feat = {}  # -> {"xa":float, "psxg":float, "goals":float, "sp_xg":float, "op_xg":float}

        for mid in matches.get("match_id", []):
            matches_cnt += 1
            ev = safe_events(mid)
            if ev.empty:
                continue
            events_cnt += 1

            # Resolve columns (multiple variants across versions)
            s_team   = get_series(ev, ["team", "possession_team", "possession_team.name"])
            s_type   = get_series(ev, ["type", "type_name", "type.name"])
            s_xg     = get_series(ev, ["shot_statsbomb_xg", "shot.statsbomb_xg"])
            s_psxg   = get_series(ev, ["shot_post_shot_xg", "shot.post_shot_xg"])
            s_out    = get_series(ev, ["shot_outcome", "shot.outcome_name", "shot.outcome.name", "shot_outcome_name"])
            s_ppat   = get_series(ev, ["play_pattern", "play_pattern_name", "play_pattern.name"])
            s_id     = get_series(ev, ["id"])
            s_assist = get_series(ev, ["pass_assisted_shot_id", "pass.assisted_shot_id"])

            if s_team is None:
                continue

            df = ev  # shorthand
            # Identify shots: prefer "type == Shot", else non-null xG
            if s_type is not None and "Shot" in s_type.astype(str).unique():
                shots = df[s_type.astype(str) == "Shot"].copy()
            elif s_xg is not None:
                shots = df[s_xg.notna()].copy()
            else:
                shots = pd.DataFrame()

            # xG for/against + splits
            if not shots.empty and s_xg is not None:
                shots_rows += len(shots)
                # Attach resolved columns for easy access
                shots = shots.copy()
                shots["_team"] = s_team.loc[shots.index].astype(str)
                shots["_xg"]   = s_xg.loc[shots.index].astype(float)

                if s_ppat is not None:
                    shots["_ppat"] = s_ppat.loc[shots.index].astype(str)
                if s_psxg is not None:
                    shots["_psxg"] = s_psxg.loc[shots.index]
                if s_out is not None:
                    shots["_out"]  = s_out.loc[shots.index].astype(str)

                for tm in shots["_team"].dropna().unique():
                    teams_counted.add(tm)
                    s_tm  = shots.loc[shots["_team"] == tm]
                    s_opp = shots.loc[shots["_team"] != tm]

                    key = (tm, comp_name, season_nm)
                    team_xg.setdefault(key, {"xg":0.0,"xga":0.0})
                    team_xg[key]["xg"]  += float(s_tm["_xg"].sum())
                    team_xg[key]["xga"] += float(s_opp["_xg"].sum())

                    team_feat.setdefault(key, {"xa":0.0,"psxg":0.0,"goals":0.0,"sp_xg":0.0,"op_xg":0.0})
                    if "_ppat" in s_tm.columns:
                        sp_mask = s_tm["_ppat"].isin(["From Corner","From Free Kick","From Throw In"])
                        team_feat[key]["sp_xg"] += float(s_tm.loc[sp_mask, "_xg"].sum())
                        team_feat[key]["op_xg"] += float(s_tm.loc[~sp_mask, "_xg"].sum())

                    if "_psxg" in s_opp.columns and "_out" in s_opp.columns:
                        psxg_against = float(pd.to_numeric(s_opp["_psxg"], errors="coerce").fillna(0).sum())
                        goals_conceded = float((s_opp["_out"] == "Goal").sum())
                        team_feat[key]["psxg"]  += psxg_against
                        team_feat[key]["goals"] += goals_conceded

            # xA from passes that assisted a shot
            if s_assist is not None and s_id is not None and s_xg is not None:
                passes = df[df[s_assist.name].notna()].copy()
                if not passes.empty:
                    # build lookup: assisted_shot_id -> xG
                    shot_lu = df[[s_id.name, s_xg.name]].dropna(subset=[s_xg.name]).rename(columns={s_id.name:"assisted_shot_id"})
                    passes = passes.merge(shot_lu, left_on=s_assist.name, right_on="assisted_shot_id", how="left")
                    passes["_team"] = s_team.loc[passes.index].astype(str)
                    if s_xg.name in passes.columns:
                        for tm in passes["_team"].dropna().unique():
                            xA = float(pd.to_numeric(passes.loc[passes["_team"] == tm, s_xg.name], errors="coerce").fillna(0).sum())
                            key = (tm, comp_name, season_nm)
                            team_feat.setdefault(key, {"xa":0.0,"psxg":0.0,"goals":0.0,"sp_xg":0.0,"op_xg":0.0})
                            team_feat[key]["xa"] += xA

        # Write rows for this competition-season
        for (tm, comp, season), vals in team_xg.items():
            rows_xg.append({"team": tm, "competition": comp, "season_name": season, "xg_sb": vals["xg"], "xga_sb": vals["xga"]})
        for (tm, comp, season), vals in team_feat.items():
            rows_feat.append({
                "team": tm, "competition": comp, "season_name": season,
                "xa_sb": vals["xa"],
                "psxg_minus_goals_sb": vals["psxg"] - vals["goals"],
                "setpiece_xg_sb": vals["sp_xg"],
                "openplay_xg_sb": vals["op_xg"],
            })

    # Save outputs
    pd.DataFrame(rows_xg).to_csv(OUT_XG, index=False)
    pd.DataFrame(rows_feat).to_csv(OUT_FEAT, index=False)

    # Coverage logging
    print("\n=== StatsBomb coverage ===")
    print(f"Competitions processed : {comps_cnt}")
    print(f"Matches with events    : {events_cnt}/{matches_cnt}")
    print(f"Shot rows counted      : {shots_rows}")
    print(f"Distinct teams seen    : {len(teams_counted)}")
    print(f"Wrote {OUT_XG} rows={len(rows_xg)} and {OUT_FEAT} rows={len(rows_feat)}")

if __name__ == "__main__":
    main()