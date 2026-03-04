# pipeline.py
import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent

def run(cmd):
    print("\n>>", " ".join(cmd))
    p = subprocess.run(cmd, cwd=str(REPO_ROOT), text=True, capture_output=True)

    if p.stdout:
        print("\n--- STDOUT ---\n", p.stdout)
    if p.stderr:
        print("\n--- STDERR ---\n", p.stderr)

    if p.returncode != 0:
        raise SystemExit(p.returncode)

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

    

