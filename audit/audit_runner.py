import argparse, json, pandas as pd, os
from datetime import datetime
from config import EXPECTED_SCHEMAS, FIELDS_IN_USE, PROVIDER_CAPS_BASE, FUTURE_BUCKETS
from utils import file_age_days

def load_json(p): 
    with open(p, "r", encoding="utf-8") as f: 
        return json.load(f)

def to_csv(df, path): 
    df.to_csv(path, index=False)

def file_inventory_row(path, label):
    exists = os.path.exists(path)
    rows = cols = None
    status = "MISSING"
    if exists:
        try:
            df = pd.read_csv(path)
            rows, cols = len(df), len(df.columns)
            status = "OK" if rows > 0 else "EMPTY"
        except Exception:
            status = "READ_ERR"
    return {
        "file": os.path.basename(path),
        "path": path,
        "label": label,
        "status": status,
        "rows": rows,
        "cols": cols,
        "stale_days": file_age_days(path)
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--odds", required=True)
    ap.add_argument("--soccerdata", required=True)
    ap.add_argument("--spi", required=True)
    ap.add_argument("--understat", required=True)
    ap.add_argument("--statsbomb", required=True)
    ap.add_argument("--footballdata", required=True)
    ap.add_argument("--hist", required=False)
    ap.add_argument("--fixtures", required=False)
    ap.add_argument("--enriched", required=False)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    now = datetime.utcnow().isoformat()

    odds = load_json(args.odds)
    sdata = load_json(args.soccerdata)
    spi  = load_json(args.spi)
    und  = load_json(args.understat)
    stb  = load_json(args.statsbomb)
    fdo  = load_json(args.footballdata)

    # SOURCE_CAPABILITIES.json: what’s available vs what we use
    caps = {
      "generated_at_utc": now,
      "providers": {
        "odds": {"probe": odds, "declared_caps": PROVIDER_CAPS_BASE["odds"], "fields_in_use": FIELDS_IN_USE["odds"]},
        "spi":  {"probe": spi,  "declared_caps": PROVIDER_CAPS_BASE["spi"],  "fields_in_use": FIELDS_IN_USE["spi"]},
        "fbref":{"probe": sdata, "declared_caps": PROVIDER_CAPS_BASE["fbref"],"fields_in_use": FIELDS_IN_USE["fbref"]},
        "understat":{"probe": und, "declared_caps": PROVIDER_CAPS_BASE["understat"], "fields_in_use": FIELDS_IN_USE["xg"]},
        "statsbomb":{"probe": stb,"declared_caps": PROVIDER_CAPS_BASE["statsbomb"],"fields_in_use": []},
        "footballdata":{"probe": fdo, "declared_caps": PROVIDER_CAPS_BASE["footballdata"], "fields_in_use": []}
      }
    }
    with open(f"{args.outdir}/SOURCE_CAPABILITIES.json","w",encoding="utf-8") as f:
        json.dump(caps, f, indent=2)

    # DATA_INVENTORY_REPORT.csv (expanded with staleness)
    inv = []
    if args.hist:     inv.append(file_inventory_row(args.hist, "HIST backbone"))
    if args.fixtures: inv.append(file_inventory_row(args.fixtures, "Upcoming fixtures (odds)"))
    if args.enriched: inv.append(file_inventory_row(args.enriched, "Upcoming 7d enriched"))

    # Probe-derived summaries
    inv.append({
        "file": "sd_fbref_team_stats.csv",
        "path": sdata.get("fbref",{}).get("cache_dir"),
        "label": "FBref team stats (via soccerdata)",
        "status": "OK" if sdata.get("fbref",{}).get("ok") else "EMPTY",
        "rows": sdata.get("fbref",{}).get("rows"),
        "cols": len(sdata.get("fbref",{}).get("cols",[])),
        "stale_days": sdata.get("fbref",{}).get("cache_stale_days")
    })
    inv.append({
        "file": "sd_538_spi.csv",
        "path": spi.get("cache_path"),
        "label": "SPI (538) snapshot",
        "status": "OK" if spi.get("ok") else "EMPTY",
        "rows": spi.get("rows"),
        "cols": len(spi.get("cols",[])),
        "stale_days": spi.get("cache_stale_days")
    })
    inv.append({
        "file": "xg_understat.csv",
        "path": und.get("cache_path"),
        "label": "Understat xG (cached if present)",
        "status": "OK" if (und.get("cache_stale_days") is not None) else "MISSING",
        "rows": None,
        "cols": None,
        "stale_days": und.get("cache_stale_days")
    })
    inv.append({
        "file": "xg_statsbomb.csv",
        "path": stb.get("cache_path"),
        "label": "StatsBomb xG (cached if present)",
        "status": "OK" if (stb.get("cache_stale_days") is not None) else "MISSING",
        "rows": None,
        "cols": None,
        "stale_days": stb.get("cache_stale_days")
    })
    to_csv(pd.DataFrame(inv), f"{args.outdir}/DATA_INVENTORY_REPORT.csv")

    # FUTURE_ODDS_REPORT.csv – per-league & aggregate
    rows = []
    agg = odds.get("future_odds_counts", {})
    for k, v in (agg or {}).items():
        rows.append({"league":"__ALL__", "bucket": k, "events": v})
    for lg, st in (odds.get("leagues") or {}).items():
        for B in FUTURE_BUCKETS:
            rows.append({"league": lg, "bucket": f"{B}d", "events": st["by_bucket"][f"{B}d"]})
        rows.append({"league": lg, "bucket": "dispersion_avg", "events": st.get("bookmaker_dispersion_avg")})
        rows.append({"league": lg, "bucket": "dispersion_median", "events": st.get("bookmaker_dispersion_median")})
        rows.append({"league": lg, "bucket": "has_opening_odds", "events": st.get("has_opening_odds")})
        rows.append({"league": lg, "bucket": "has_closing_odds", "events": st.get("has_closing_odds")})
    to_csv(pd.DataFrame(rows), f"{args.outdir}/FUTURE_ODDS_REPORT.csv")

    # SCHEMA_DRIFT_REPORT.csv – expected vs probed
    drift = []
    fb_cols = sdata.get("fbref",{}).get("cols",[])
    if fb_cols:
        exp = set(EXPECTED_SCHEMAS["FBREF_TEAM_STATS"])
        got = set(fb_cols)
        drift.append({"dataset":"FBREF_TEAM_STATS","missing":",".join(sorted(exp-got)),"unexpected":",".join(sorted(got-exp))})
    spi_cols = set(spi.get("cols",[]))
    if spi_cols:
        exp = set(EXPECTED_SCHEMAS["SPI"])
        drift.append({"dataset":"SPI","missing":",".join(sorted(exp-spi_cols)),"unexpected":",".join(sorted(spi_cols-exp))})
    to_csv(pd.DataFrame(drift), f"{args.outdir}/SCHEMA_DRIFT_REPORT.csv")

    # MISSING_DATA_REPORT.csv – provider up/down snapshot + staleness summaries
    miss = []
    miss.append({"provider":"odds","ok":odds.get("ok",False),"note":"See FUTURE_ODDS_REPORT.csv for per-league reach"})
    miss.append({"provider":"fbref","ok":sdata.get("fbref",{}).get("ok",False),"note":""})
    miss.append({"provider":"spi","ok":spi.get("ok",False),"note":f"cache_stale_days={spi.get('cache_stale_days')}"})
    miss.append({"provider":"understat","ok":und.get("ok",True),"note":f"cache_stale_days={und.get('cache_stale_days')}"})
    miss.append({"provider":"statsbomb","ok":stb.get("ok",True),"note":f"cache_stale_days={stb.get('cache_stale_days')}"})
    miss.append({"provider":"football-data.org","ok":fdo.get("ok",False),"note":fdo.get("note","")})
    to_csv(pd.DataFrame(miss), f"{args.outdir}/MISSING_DATA_REPORT.csv")

if __name__ == "__main__":
    main()