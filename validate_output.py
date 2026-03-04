# scripts/validate_output.py
from pathlib import Path
import pandas as pd

OUT = Path("data/unified/nfl_unified.csv")

REQUIRED_COLS = [
    "game_id","date","team_id","opponent_id","is_home",
    "team_score","opponent_score","season","seasontype","week"
]

def main():
    if not OUT.exists():
        raise FileNotFoundError(f"Missing output file: {OUT}")

    df = pd.read_csv(OUT)
    if len(df) == 0:
        raise ValueError("Output CSV exists but has 0 rows.")

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Output missing required columns: {missing}")

    # quick sanity checks
    if df["team_score"].isna().any():
        raise ValueError("team_score contains nulls.")
    if df["opponent_score"].isna().any():
        raise ValueError("opponent_score contains nulls.")

    # confirm week looks reasonable
    if not df["week"].between(1, 18).all():
        raise ValueError("week has values outside 1..18 (check fetch/cleaning).")

    print(f"✅ validate_output passed: rows={len(df)} cols={len(df.columns)}")

if __name__ == "__main__":
    main()
