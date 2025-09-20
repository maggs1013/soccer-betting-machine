# scripts/scrape_oddsportal.py
# CI-safe wrapper: does NOT scrape. Tells you where to drop files.

import os
RAW_DIR = "data/oddsportal/raw"

def main():
    os.makedirs(RAW_DIR, exist_ok=True)
    print("[INFO] This wrapper is CI-safe and does not scrape by itself.")
    print("[INFO] Run an open-source OddsPortal scraper LOCALLY (e.g., gingeleski or mg30)")
    print("[INFO] Then drop CSV/JSON files into:", RAW_DIR)
    print("[INFO] Each file should contain at least:")
    print("       date, home_team, away_team, home_odds_dec, draw_odds_dec, away_odds_dec, source")
    print("[INFO] Next steps in CI: test_scraper_schema.py → merge_scraped_odds.py → consolidate_odds.py")

if __name__ == "__main__":
    main()