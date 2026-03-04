# Fetch ESPN Teams -> data/raw/teams_raw.csv

from pathlib import Path
import pandas as pd
import requests
import requests_cache

requests_cache.install_cache("nfl_cache", expire_after=3600)

TEAMS_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams"


def fetch_teams() -> dict:
    resp = requests.get(TEAMS_URL, timeout=30)
    resp.raise_for_status()
    return resp.json()


def normalize_teams(teams_json: dict) -> pd.DataFrame:
    rows = []

    sports = teams_json.get("sports", [])
    if not sports:
        return pd.DataFrame([])

    leagues = sports[0].get("leagues", [])
    if not leagues:
        return pd.DataFrame([])

    teams_list = leagues[0].get("teams", [])
    for t in teams_list:
        team = t.get("team") or {}
        team_id = team.get("id")
        rows.append(
            {
                "team_id": team_id,
                "team_name": team.get("displayName"),
                "team_abbreviation": team.get("abbreviation"),
                "team_location": team.get("location"),
                "team_color": team.get("color"),
                "team_alternate_color": team.get("alternateColor"),
                "team_logo": (team.get("logos") or [{}])[0].get("href"),
            }
        )

    return pd.DataFrame(rows)


def main():
    out = Path("data/raw")
    out.mkdir(parents=True, exist_ok=True)

    data = fetch_teams()
    df = normalize_teams(data)

    out_path = out / "teams_raw.csv"
    df.to_csv(out_path, index=False)
    print(f"Saved raw teams: {out_path} rows={len(df)}")


if __name__ == "__main__":
    main()
