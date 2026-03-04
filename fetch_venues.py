# Fetch ESPN Venues -> data/raw/venues_raw.csv

from pathlib import Path
import pandas as pd
import requests
import requests_cache

requests_cache.install_cache("nfl_cache", expire_after=3600)

VENUES_URL = "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/venues"
METEO_URL = "https://api.open-meteo.com/v1/forecast"


def safe_get(url: str, params: dict | None = None) -> dict:
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_venues_list(limit: int = 500) -> dict:
    return safe_get(VENUES_URL, params={"limit": limit})


def fetch_venue_detail(item) -> dict | None:

    if item is None:
        return None
    if isinstance(item, str):
        return safe_get(item)
    if isinstance(item, dict) and "$ref" in item:
        return safe_get(item["$ref"])
    if isinstance(item, dict):
        # might already be the full object
        return item
    return None


def fetch_timezone(lat: float, lon: float) -> str | None:
    try:
        data = safe_get(
            METEO_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "timezone": "auto",
                "current": "temperature_2m",
            },
        )
        return data.get("timezone")
    except Exception:
        return None


def normalize_venues(venues_list_json: dict, fetch_meteo: bool = True) -> pd.DataFrame:
    rows = []
    items = venues_list_json.get("items", [])

    for item in items:
        detail = fetch_venue_detail(item)
        if not detail:
            continue

        venue_id = detail.get("id")

        address = detail.get("address") or {}
        city = address.get("city")
        state = address.get("state")
        zip_code = address.get("zipCode")

        geo = detail.get("geo") or {}
        lat = geo.get("latitude")
        lon = geo.get("longitude")

        tz = None
        if fetch_meteo and lat is not None and lon is not None:
            tz = fetch_timezone(lat, lon)

        rows.append(
            {
                "venue_id": venue_id,
                "venue_name": detail.get("fullName") or detail.get("name"),
                "venue_city": city,
                "venue_state": state,
                "venue_zip": zip_code,
                "venue_capacity": detail.get("capacity"),
                "venue_grass": detail.get("grass"),
                "venue_lat": lat,
                "venue_lon": lon,
                "timezone": tz,
            }
        )

    return pd.DataFrame(rows)


def main():
    out = Path("data/raw")
    out.mkdir(parents=True, exist_ok=True)

    venues_list = fetch_venues_list(limit=500)
    df = normalize_venues(venues_list, fetch_meteo=True)

    out_path = out / "venues_raw.csv"
    df.to_csv(out_path, index=False)
    print(f"Saved raw venues: {out_path} rows={len(df)}")


if __name__ == "__main__":
    main()
