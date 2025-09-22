#!/usr/bin/env python3
import os
import pandas as pd
import numpy as np
from datetime import datetime

DATA_DIR = "data"
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

ENRICHED = os.path.join(DATA_DIR, "enriched_fixtures.csv")
OUT_MU = os.path.join(RUN_DIR, "GOALS_MU.csv")

def main():
    df = pd.read_csv(ENRICHED)
    # naive µ using rolling team xG and SPI scaling; refine later
    # Expect columns: xg_for_home_5, xg_for_away_5 (rolling), maybe spi_home, spi_away
    mu_home = df["xg_for_home_5"].fillna(df.get("spi_home", 1.4)*0.02 + 1.2)
    mu_away = df["xg_for_away_5"].fillna(df.get("spi_away", 1.4)*0.02 + 1.0)
    out = df[["fixture_id","league"]].copy()
    out["mu_home"] = mu_home.clip(lower=0.2, upper=3.5)
    out["mu_away"] = mu_away.clip(lower=0.2, upper=3.5)
    out.to_csv(OUT_MU, index=False)
    print("GOALS_MU.csv written")

if __name__ == "__main__":
    main()