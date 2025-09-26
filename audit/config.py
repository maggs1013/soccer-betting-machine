# --- Which leagues to probe in the odds API ---
TARGET_LEAGUES = [
    # Keys should match your odds provider keys
    "soccer_epl",
    "soccer_spain_la_liga",
    "soccer_italy_serie_a",
    "soccer_germany_bundesliga",
    "soccer_france_ligue_one",
    "soccer_uefa_champs_league",
    "soccer_uefa_europa_league",
    "soccer_uefa_europa_conference_league",
    "soccer_usa_mls",
    "soccer_netherlands_eredivisie",
    "soccer_portugal_primeira_liga",
    "soccer_belgium_first_div",
]

# Horizon buckets (days) for future-odds reach
FUTURE_BUCKETS = [1, 3, 7]

# Expected schemas used to detect schema drift
EXPECTED_SCHEMAS = {
    "UPCOMING_fixtures": [
        "date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec","league"
    ],
    "SPI": ["team","spi_off","spi_def","spi","league","date"],
    "FBREF_TEAM_STATS": [
        "team","league","season","shots","shots_against","xg","xga","possession","date"
    ],
    "UNDERSTAT_XG": ["match_id","date","home_team","away_team","xg_home","xg_away","league"],
    "STATSBOMB_XG": ["match_id","date","home_team","away_team","xg_home","xg_away","league"],
    "FD_HIST": ["date","league","home_team","away_team","fthg","ftag","ftr","odd_h","odd_d","odd_a"]
}

# What the pipeline currently uses (to highlight underutilized fields)
FIELDS_IN_USE = {
    "odds": ["home_odds_dec","draw_odds_dec","away_odds_dec"],
    "spi": ["spi_off","spi_def"],
    "fbref": ["shots","shots_against","xg","xga","possession"],
    "xg": ["xg_home","xg_away"]
}

# Broad provider capabilities (used to compute "available - used")
PROVIDER_CAPS_BASE = {
    "odds": ["h2h_odds","ou_lines","btts","spreads","open_close","bookmaker_count"],
    "spi":  ["team","league","spi","spi_off","spi_def","rank","conf_int_low","conf_int_high","date"],
    "fbref":["shots","shots_against","xg","xga","possession","pass_pct","pressures","set_pieces","cards","gk_psxg","gk_psxg_prevented"],
    "understat":["xg_shot_level","xg_assists","deep_completions","key_passes","set_piece_xg"],
    "statsbomb":["shot_events","shot_freeze_frames","keeper_actions","pressures","pass_networks"],
    "footballdata":["cards","corners","ht_scores","referee","odds_many_books"]
}