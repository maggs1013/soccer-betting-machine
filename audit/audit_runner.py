import argparse, json, pandas as pd
from datetime import datetime
from config import EXPECTED_SCHEMAS, FIELDS_IN_USE, PROVIDER_CAPS_BASE

def load_json(p): 
    with open(p, "r", encoding="utf-8") as f: 
        return json.load(f)

def to_csv(df, path): 
    df.to_csv(path, index=False)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--odds", required=True)
    ap.add_argument("--soccerdata", required=True)
    ap.add_argument("--spi", required=True)
    ap.add_argument("--understat", required=True)
    ap.add_argument("--statsbomb", required=True)
    ap.add_argument("--footballdata", required=True)
    ap.add_argument("--hist", required=False)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    now = datetime.utcnow().isoformat()

    odds = load_json(args.odds)
    sdata = load_json(args.soccerdata)
    spi  = load_json(args.spi)
    und  = load_json(args.understat)
    stb  = load_json(args.statsbomb)
    fdo  = load_json(args.footballdata)

    # SOURCE_CAPABILITIES.json
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

    # DATA_INVENTORY_REPORT.csv (expanded stub – adapt to your real file paths)
    inv = []
    inv.append({"file":"UPCOMING_fixtures.csv","label":"Upcoming fixtures (odds)","status":"UNKNOWN","rows":None,"cols":None})
    inv.append({"file":"UPCOMING_7D_enriched.csv","label":"Upcoming 7d enriched","status":"UNKNOWN","rows":None,"cols":None})
    inv.append({"file":"sd_fbref_team_stats.csv","label":"FBref team stats","status":"UNKNOWN","rows":sdata.get("fbref",{}).get("rows",0),"cols":len(sdata.get("fbref",{}).get("cols",[]))})
    inv.append({"file":"sd_538_spi.csv","label":"SPI","status":"UNKNOWN","rows":spi.get("rows",0),"cols":len(spi.get("cols",[]))})
    if args.hist:
        try:
            hist_df = pd.read_csv(args.hist)
            inv.append({"file":"HIST_matches.csv","label":"HIST backbone","status":"OK","rows":len(hist_df),"cols":len(hist_df.columns)})
        except Exception as e:
            inv.append({"file":"HIST_matches.csv","label":"HIST backbone","status":"MISSING","rows":0,"cols":0})
    to_csv(pd.DataFrame(inv), f"{args.outdir}/DATA_INVENTORY_REPORT.csv")

    # FUTURE_ODDS_REPORT.csv
    future_counts = odds.get("future_odds_counts", {"24h":None,"72h":None,"7d":None})
    to_csv(pd.DataFrame([{"bucket":k,"events":v} for k,v in future_counts.items()]),
           f"{args.outdir}/FUTURE_ODDS_REPORT.csv")

    # SCHEMA_DRIFT_REPORT.csv – compare expected vs probed columns where possible
    schema_rows = []
    fb_cols = sdata.get("fbref",{}).get("cols",[])
    if fb_cols:
        expected = set(EXPECTED_SCHEMAS["FBREF_TEAM_STATS"])
        got = set(fb_cols)
        schema_rows.append({"dataset":"FBREF_TEAM_STATS","missing":",".join(sorted(expected-got)),"unexpected":",".join(sorted(got-expected))})
    spi_cols = set(spi.get("cols",[]))
    if spi_cols:
        expected = set(EXPECTED_SCHEMAS["SPI"])
        schema_rows.append({"dataset":"SPI","missing":",".join(sorted(expected-spi_cols)),"unexpected":",".join(sorted(spi_cols-expected))})
    to_csv(pd.DataFrame(schema_rows), f"{args.outdir}/SCHEMA_DRIFT_REPORT.csv")

    # MISSING_DATA_REPORT.csv – high-level: which provider is down / thin
    rows = []
    rows.append({"provider":"odds","ok":odds.get("ok",False),"note":"future_odds_counts inspected"})
    rows.append({"provider":"fbref","ok":sdata.get("fbref",{}).get("ok",False),"note":""})
    rows.append({"provider":"spi","ok":spi.get("ok",False),"note":""})
    rows.append({"provider":"understat","ok":und.get("ok",True),"note":und.get("note","")})
    rows.append({"provider":"statsbomb","ok":stb.get("ok",True),"note":stb.get("note","")})
    rows.append({"provider":"football-data.org","ok":fdo.get("ok",False),"note":fdo.get("note","")})
    to_csv(pd.DataFrame(rows), f"{args.outdir}/MISSING_DATA_REPORT.csv")

if __name__ == "__main__":
    main()