# Clean Pipeline
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

@dataclass
class RunLog:
    metrics: dict = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    def add(self, key, value):
        self.metrics[key] = value

    def note(self, msg):
        self.notes.append(msg)


def standardize_columns(df: pd.DataFrame, log: RunLog) -> pd.DataFrame:
    df = df.copy()
    before = list(df.columns)

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
    )
    log.note(f"Standardized columns: {before} -> {list(df.columns)}")
    return df


def coerce_types(df: pd.DataFrame, log: RunLog, numeric_cols: list[str], date_cols: list[str] | None = None) -> pd.DataFrame:
    df = df.copy()

    if date_cols:
        for dcol in date_cols:
            if dcol in df.columns:
                df[dcol] = pd.to_datetime(df[dcol], errors="coerce")

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def clean_missingness(df: pd.DataFrame, log: RunLog, required_non_null: list[str]) -> pd.DataFrame:
    df = df.copy()
    before = len(df)


    present_required = [c for c in required_non_null if c in df.columns]
    if present_required:
        df = df.dropna(subset=present_required)

    dropped = before - len(df)
    log.add("dropped_rows_missing_required_values", dropped)
    return df


def dedupe(df: pd.DataFrame, log: RunLog, subset: list[str] | None = None) -> pd.DataFrame:
    df = df.copy()
    before = len(df)

    if subset:
        present = [c for c in subset if c in df.columns]
        if present:
            df = df.drop_duplicates(subset=present, keep="first")

    log.add("dropped_duplicate_rows", before - len(df))
    return df


def validate_schema(df: pd.DataFrame, schema: dict, stage: str):
    required = schema.get("required_columns", [])
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        raise ValueError(f"[{stage}] Missing required columns: {missing_cols}")

    non_nullable = schema.get("non_nullable", [])
    for c in non_nullable:
        if c in df.columns:
            n_null = int(df[c].isna().sum())
            if n_null > 0:
                raise ValueError(f"[{stage}] Column '{c}' has {n_null} nulls but is non-nullable.")


def run_pipeline(raw_path: Path, out_dir: Path, schema_path: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "clean").mkdir(parents=True, exist_ok=True)
    (out_dir / "reports").mkdir(parents=True, exist_ok=True)

    log = RunLog()
    log.add("run_timestamp", datetime.utcnow().isoformat() + "Z")
    log.add("raw_path", str(raw_path))
    log.add("schema_path", str(schema_path))

    df = pd.read_csv(raw_path)
    log.add("raw_rows", len(df))
    log.add("raw_columns", list(df.columns))

    schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))

    df = standardize_columns(df, log)
    validate_schema(df, schema, stage="after_standardize")


    numeric_cols = schema.get("numeric_columns", [])
    date_cols = schema.get("date_columns", [])
    df = coerce_types(df, log, numeric_cols=numeric_cols, date_cols=date_cols)

    df = clean_missingness(df, log, required_non_null=schema.get("non_nullable", []))
    df = dedupe(df, log, subset=schema.get("dedupe_subset", []))

    validate_schema(df, schema, stage="after_cleaning")

    clean_path = out_dir / "clean" / schema.get("output_filename", "clean.csv")
    df.to_csv(clean_path, index=False)

    log.add("clean_rows", len(df))
    log.add("clean_path", str(clean_path))

    (out_dir / "reports" / (clean_path.stem + "_metrics.json")).write_text(
        json.dumps(log.metrics, indent=2),
        encoding="utf-8"
    )
    (out_dir / "reports" / (clean_path.stem + "_notes.txt")).write_text(
        "\n".join(log.notes) + "\n",
        encoding="utf-8"
    )

    print("✅ Cleaning complete.")
    print(f"Saved: {clean_path}")


if __name__ == "__main__":

    run_pipeline(
        raw_path=ROOT / "scripts" / "data" / "raw" / "teams_raw.csv",
        out_dir=ROOT / "data",
        schema_path=ROOT / "scripts" / "schemas" / "teams_schema.json",

    )
