import os, pandas as pd
from soccerdata import FBref, FiveThirtyEight, FootballData
from datetime import datetime, timedelta

DATA="data"; os.makedirs(DATA,exist_ok=True)
SEASON=datetime.now().year
FBREF_LEAGUES=["ENG-Premier League","ESP-La Liga","ITA-Serie A","GER-Bundesliga","FRA-Ligue 1"]
FD_LEAGUES=["E0","SP1","I1","D1","F1"]

def fbref_team_stats():
    try:
        fb=FBref(leagues=FBREF_LEAGUES, seasons=SEASON)
        shooting=fb.read_team_season_stats(stat_type="shooting")
        # FBref naming sometimes "xg" columns; normalize:
        for c in shooting.columns:
            lc=c.lower()
            if lc.startswith("xg") and "against" not in lc and "for" not in lc and "diff" not in lc:
                shooting=shooting.rename(columns={c:"xg_for_fbref"})
        if "xg_against" in shooting.columns: 
            shooting=shooting.rename(columns={"xg_against":"xg_against_fbref"})
        shooting.to_csv(os.path.join(DATA,"sd_fbref_team_stats.csv"),index=False)
        print("[OK] sd_fbref_team_stats.csv",len(shooting))
    except Exception as e: print("[WARN] FBref failed:",e)

def five38():
    try:
        spi=FiveThirtyEight(leagues=["epl","liga","serie-a","bundesliga","ligue-1"]).read_team_ratings()
        spi.to_csv(os.path.join(DATA,"sd_538_spi.csv"),index=False); print("[OK] sd_538_spi.csv",len(spi))
    except Exception as e: print("[WARN] 538 failed:",e)

def fd_fixtures_next7():
    try:
        fd=FootballData(leagues=FD_LEAGUES, seasons=SEASON)
        fx=fd.read_fixtures()
        # Next 7 days window
        today=pd.Timestamp.now().normalize()
        fx["date"]=pd.to_datetime(fx["date"],errors="coerce")
        window=fx[(fx["date"]>=today)&(fx["date"]<today+pd.Timedelta(days=7))]
        window.to_csv(os.path.join(DATA,"sd_fd_fixtures.csv"),index=False)
        print("[OK] sd_fd_fixtures.csv (next 7d)",len(window))
    except Exception as e: print("[WARN] Football-Data fixtures failed:",e)

if __name__=="__main__":
    fbref_team_stats(); five38(); fd_fixtures_next7()
