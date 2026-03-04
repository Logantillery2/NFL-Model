# Fetch ESPN Scoreboard -> data/raw/scoreboard_raw.csv

import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests
import requests_cache

requests_cache.install_cache("nfl_cache", expire_after=3600)

SCOREBOARD_URL = ("https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard")


def fetch_scoreboard(season: int = 2023, seasontype: int = 2, week: int = 1) -> dict:
    resp = requests.get(
        SCOREBOARD_URL,
        params={"dates": str(season), "seasontype": seasontype, "week": week},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def normalize_scoreboard(scoreboard_json: dict, season: int, seasontype: int, week: int) -> pd.DataFrame:
    rows = []

    events = scoreboard_json.get("events", [])
    for ev in events:
        game_id = ev.get("id")
        date = ev.get("date")

        competitions = ev.get("competitions", [])
        if not competitions:
            continue
        comp = competitions[0]


        venue = comp.get("venue") or {}
        venue_id = venue.get("id")
        venue_name = venue.get("fullName") or venue.get("name")

        competitors = comp.get("competitors", [])

        if len(competitors) < 2:
            continue


        by_side = {c.get("homeAway"): c for c in competitors}
        home = by_side.get("home", competitors[0])
        away = by_side.get("away", competitors[1] if len(competitors) > 1 else competitors[0])

        def get_team_id(c):
            team = c.get("team") or {}
            return team.get("id")

        def get_score(c):
            
            return c.get("score")

        home_team_id = get_team_id(home)
        away_team_id = get_team_id(away)

        home_score = get_score(home)
        away_score = get_score(away)

        # home row
        rows.append(
            {
                "game_id": game_id,
                "date": date,
                "team_id": home_team_id,
                "opponent_id": away_team_id,
                "is_home": 1,
                "team_score": home_score,
                "opponent_score": away_score,
                "venue_id": venue_id,
                "venue_name": venue_name,
                "season": season,
                "seasontype": seasontype,
                "week": week,
            }
        )
        # away row
        rows.append(
            {
                "game_id": game_id,
                "date": date,
                "team_id": away_team_id,
                "opponent_id": home_team_id,
                "is_home": 0,
                "team_score": away_score,
                "opponent_score": home_score,
                "venue_id": venue_id,
                "venue_name": venue_name,
                "season": season,
                "seasontype": seasontype,
                "week": week
            }
        )

    return pd.DataFrame(rows)


def main():
    out = Path("data/raw")
    out.mkdir(parents=True, exist_ok=True)

    seasons = [2020, 2021, 2022, 2023, 2024, 2025]
    seasontype = 2
    weeks = range(1, 18)
    
    all_parts = []
    
    
    for season in seasons:
        for week in weeks:
            print(f"Fetching season={season} week={week}")
            try:
                data = fetch_scoreboard(season=season, seasontype=seasontype, week=week)
                df_week = normalize_scoreboard(data, season=season, seasontype=seasontype, week=week)
                all_parts.append(df_week)
            except Exception as e:
                print("Failed:", season, week, e)

    df_all = pd.concat(all_parts, ignore_index=True)

    out_path = out / "scoreboard_raw.csv"
    df_all.to_csv(out_path, index=False)
    print("Saved:", out_path)
    print("Rows:", len(df_all))
    print("Seasons in file:\n", df_all["season"].value_counts())


if __name__ == "__main__":
    main()
