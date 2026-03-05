import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
 
# -----------------------------
# Config
# -----------------------------
st.set_page_config(page_title="NFL Data Explorer", layout="wide")
DATA_PATH = "data/unified/nfl_unified.csv"

# -----------------------------
# Helpers
# -----------------------------
@st.cache_data
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    for col in ["season", "week", "team_score", "opponent_score"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "team_score" in df.columns and "opponent_score" in df.columns:
        df["point_diff"] = df["team_score"] - df["opponent_score"]
        df["is_win"] = (df["point_diff"] > 0).astype(int)
    else:
        df["point_diff"] = np.nan
        df["is_win"] = np.nan

    if "week" in df.columns:
        df["week"] = df["week"].astype("Int64")

    return df


def nice_metric(label: str, value):
    st.metric(label, value if value is not None else "—")


def safe_unique(df, col):
    if col in df.columns:
        vals = df[col].dropna().unique().tolist()
        vals.sort()
        return vals
    return []


# -----------------------------
# Load
# -----------------------------
st.title("🏈 NFL Data Explorer Dashboard")
st.caption("Interactive EDA dashboard powered by your cleaned pipeline output.")

try:
    df = load_data(DATA_PATH)
except FileNotFoundError:
    st.error(f"Could not find `{DATA_PATH}`. Make sure your pipeline output exists.")
    st.stop()

required_cols = ["season", "week", "team_team_name", "opponent_score", "team_score", "is_home"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    st.warning(
        "Your dataset is missing some expected columns. "
        f"Missing: {missing}. The dashboard will still run but some charts may be limited."
    )

# -----------------------------
# Sidebar Filters
# -----------------------------
st.sidebar.header("Filters")

# --- Season RANGE slider ---
if "season" in df.columns and df["season"].dropna().size > 0:
    season_min = int(df["season"].min())
    season_max = int(df["season"].max())

    season_range = st.sidebar.slider(
        "Season Range",
        season_min,
        season_max,
        (season_min, season_max)
    )
else:
    season_range = None

# Start filtered df
f = df.copy()

# Apply season range filter first
if season_range is not None and "season" in f.columns:
    f = f[f["season"].between(season_range[0], season_range[1])]

# Season type filter (use f, not df)
seasontypes = safe_unique(f, "seasontype")
if seasontypes:
    seasontype = st.sidebar.multiselect("Season Type", seasontypes, default=seasontypes)
    if "seasontype" in f.columns:
        f = f[f["seasontype"].isin(seasontype)]
else:
    seasontype = None

# Week slider should be based on season-filtered data (f)
weeks = safe_unique(f, "week")
if weeks:
    wk_min, wk_max = int(min(weeks)), int(max(weeks))
    week_range = st.sidebar.slider("Week Range", wk_min, wk_max, (wk_min, wk_max))
    if "week" in f.columns:
        f = f[(f["week"] >= week_range[0]) & (f["week"] <= week_range[1])]
else:
    week_range = None

teams = safe_unique(df, "team_team_name")
team_mode = st.sidebar.radio("Team Mode", ["Single team", "Compare two teams", "All teams"], index=0)

team_a = None
team_b = None
if team_mode == "Single team":
    team_a = st.sidebar.selectbox("Team", teams) if teams else None
elif team_mode == "Compare two teams":
    col1, col2 = st.sidebar.columns(2)
    with col1:
        team_a = st.selectbox("Team A", teams, key="teamA") if teams else None
    with col2:
        team_b = st.selectbox("Team B", teams, key="teamB") if teams else None

home_filter = st.sidebar.selectbox("Home/Away", ["All", "Home only", "Away only"], index=0)

show_table = st.sidebar.checkbox("Show filtered table", value=False)

# Home/Away filter
if home_filter != "All" and "is_home" in f.columns:
    if home_filter == "Home only":
        f = f[f["is_home"] == True]
    else:
        f = f[f["is_home"] == False]

# Team filter
if team_mode == "Single team" and team_a and "team_team_name" in f.columns:
    f = f[f["team_team_name"] == team_a]
elif team_mode == "Compare two teams" and team_a and team_b and "team_team_name" in f.columns:
    f = f[f["team_team_name"].isin([team_a, team_b])]

if len(f) == 0:
    st.warning("No rows match your filters. Try widening the season/week range or switching filters.")
    st.stop()

# -----------------------------
# Top KPIs
# -----------------------------
kpi1, kpi2, kpi3, kpi4 = st.columns(4)

games = len(f)
avg_pts_for = float(f["team_score"].mean()) if "team_score" in f.columns else None
avg_pts_against = float(f["opponent_score"].mean()) if "opponent_score" in f.columns else None
win_rate = float(f["is_win"].mean()) if "is_win" in f.columns else None

with kpi1:
    nice_metric("Games (filtered)", games)
with kpi2:
    nice_metric("Avg Points For", f"{avg_pts_for:.2f}" if avg_pts_for is not None else None)
with kpi3:
    nice_metric("Avg Points Against", f"{avg_pts_against:.2f}" if avg_pts_against is not None else None)
with kpi4:
    nice_metric("Win Rate", f"{win_rate:.3f}" if win_rate is not None else None)

st.divider()

# -----------------------------
# Charts
# -----------------------------
st.subheader("Average Points Scored by Week")
if "week" in f.columns and "team_score" in f.columns:
    by_week = f.groupby("week", dropna=True)["team_score"].mean().sort_index()
    fig, ax = plt.subplots()
    ax.plot(by_week.index.astype(int), by_week.values)
    ax.set_xlabel("Week")
    ax.set_ylabel("Avg Points For")
    ax.set_title("Avg Points For by Week (Filtered)")
    ax.grid(True)
    st.pyplot(fig)
else:
    st.info("Need `week` and `team_score` columns for this chart.")

st.subheader("Average Points Allowed Per Week")
if "week" in f.columns and "opponent_score" in f.columns:
    points_against = f.groupby("week")["opponent_score"].mean().sort_index()
    fig, ax = plt.subplots()
    ax.plot(points_against.index.astype(int), points_against.values, marker="o")
    ax.set_xlabel("Week")
    ax.set_ylabel("Average Points Allowed")
    ax.set_title("Average Points Allowed Per Week (Filtered)")
    ax.grid(True)
    st.pyplot(fig)
else:
    st.info("Need `week` and `opponent_score` columns for this chart.")

st.subheader("Points Scored Distribution")
if "team_score" in f.columns:
    fig, ax = plt.subplots()
    ax.hist(f["team_score"].dropna(), bins=15)
    ax.set_xlabel("Points Scored")
    ax.set_ylabel("Count")
    ax.set_title("Distribution of Points Scored (Filtered)")
    st.pyplot(fig)
else:
    st.info("Need `team_score` for this chart.")

st.subheader("Offense vs Defense")
if all(c in f.columns for c in ["team_team_name", "team_score", "opponent_score"]):
    summary_od = f.groupby("team_team_name").agg(
        points_for=("team_score", "mean"),
        points_allowed=("opponent_score", "mean")
    )

    fig, ax = plt.subplots()
    ax.scatter(summary_od["points_for"], summary_od["points_allowed"])
    ax.set_xlabel("Avg Points Scored")
    ax.set_ylabel("Avg Points Allowed")
    ax.set_title("Offense vs Defense by Team (Filtered)")
    ax.grid(True)
    st.pyplot(fig)
else:
    st.info("Need team + score columns to show this chart.")

st.subheader("Home Field Advantage")
if "is_home" in f.columns and "team_score" in f.columns:
    home = f[f["is_home"] == True]["team_score"].mean()
    away = f[f["is_home"] == False]["team_score"].mean()

    fig, ax = plt.subplots()
    ax.bar(["Home", "Away"], [home, away])
    ax.set_ylabel("Average Points")
    ax.set_title("Home vs Away Scoring (Filtered)")
    st.pyplot(fig)
else:
    st.info("Need `is_home` and `team_score` columns for this chart.")

st.subheader("Team Rankings")
if all(c in f.columns for c in ["team_team_name", "team_score", "opponent_score", "is_win"]):
    rank = f.groupby("team_team_name").agg(
        games=("team_score", "count"),
        avg_points=("team_score", "mean"),
        avg_allowed=("opponent_score", "mean"),
        win_rate=("is_win", "mean")
    )
    rank["point_diff"] = rank["avg_points"] - rank["avg_allowed"]
    rank = rank.sort_values("point_diff", ascending=False)
    st.dataframe(rank)
else:
    st.info("Need team + score + win columns to show team rankings.")

st.subheader("League Scoring Trend")
if "week" in f.columns and "team_score" in f.columns:
    weekly = f.groupby("week")["team_score"].mean().sort_index()
    fig, ax = plt.subplots()
    ax.plot(weekly.index.astype(int), weekly.values)
    ax.set_xlabel("Week")
    ax.set_ylabel("Average Points")
    ax.set_title("League Scoring Trend Over Season (Filtered)")
    ax.grid(True)
    st.pyplot(fig)
else:
    st.info("Need `week` and `team_score` columns for this chart.")

# -----------------------------
# Team Summary Table (EDA)
# -----------------------------
st.subheader("Team Summary Table (EDA)")
cols_needed = ["team_team_name", "team_score", "opponent_score", "is_win"]
if all(c in f.columns for c in cols_needed):
    summary = (
        f.groupby("team_team_name")
        .agg(
            games=("team_team_name", "count"),
            avg_points_for=("team_score", "mean"),
            avg_points_against=("opponent_score", "mean"),
            win_rate=("is_win", "mean"),
            avg_point_diff=("point_diff", "mean"),
        )
        .sort_values(["win_rate", "avg_point_diff"], ascending=False)
        .reset_index()
    )
    st.dataframe(summary, use_container_width=True)
else:
    st.info("Need team + score + win columns to show the summary table.")

# -----------------------------
# Optional: filtered raw data preview
# -----------------------------
if show_table:
    st.subheader("Filtered Rows")
    st.dataframe(f.head(200), use_container_width=True)

st.caption("Tip: if your pipeline changes column names, update the column references near the top of this file.")