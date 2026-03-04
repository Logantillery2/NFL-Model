# pipeline.py
import argparse
import subprocess
import sys


def run(cmd: list[str]) -> None:
    print("\n>>", " ".join(cmd))
    subprocess.run(cmd, check=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2023)
    parser.add_argument("--seasontype", type=int, default=2)
    parser.add_argument("--week", type=int, default=1)
    args = parser.parse_args()

    # Fetch
    run([sys.executable, "fetch_teams.py"])
    run([sys.executable, "fetch_venues.py"])
    run([sys.executable, "fetch_scoreboard.py"]) 

    # Clean
    run([sys.executable, "clean_teams_pipeline.py"])
    run([sys.executable, "clean_venues_pipeline.py"])
    run([sys.executable, "clean_scoreboard_pipeline.py"])

    # Join
    run([sys.executable, "join_unified.py"])

    # Validate final output
    run([sys.executable, "validate_output.py"])

    print("\n✅ Pipeline finished successfully.")

if __name__ == "__main__":
    main()
    