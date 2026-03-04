# Join cleaned API outputs into ONE unified dataset
from pathlib import Path
import pandas as pd


def main():
    clean_dir = Path("data/clean")
    out_dir = Path("data/unified")
    out_dir.mkdir(parents=True, exist_ok=True)

    scoreboard = pd.read_csv(clean_dir / "scoreboard_clean.csv")
    teams = pd.read_csv(clean_dir / "teams_clean.csv")
    venues = pd.read_csv(clean_dir / "venues_clean.csv")


    scoreboard = scoreboard.merge(
        teams.add_prefix("team_"),
        left_on="team_id",
        right_on="team_team_id",
        how="left"
    ).drop(columns=["team_team_id"])

    scoreboard = scoreboard.merge(
        teams.add_prefix("opp_"),
        left_on="opponent_id",
        right_on="opp_team_id",
        how="left"
    ).drop(columns=["opp_team_id"])


    if "venue_id" in scoreboard.columns and "venue_id" in venues.columns:
        scoreboard = scoreboard.merge(venues, on="venue_id", how="left")

    out_path = out_dir / "nfl_unified.csv"
    scoreboard.to_csv(out_path, index=False)
    print(f"✅ Unified dataset saved: {out_path} rows={len(scoreboard)} cols={len(scoreboard.columns)}")


if __name__ == "__main__":
    main()
