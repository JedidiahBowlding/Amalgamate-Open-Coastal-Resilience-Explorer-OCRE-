from pathlib import Path
import json

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI(title="Open-Coastal-Resilience-Explorer-OCRE API", version="v0.2")

LOCATIONS_PATH = Path("config/locations.json")
DATA_DIR = Path("pilot-backend/data")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5500",
        "http://127.0.0.1:5500",
        "https://your-production-domain.example",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _load_locations() -> dict[str, dict]:
    if not LOCATIONS_PATH.exists():
        raise HTTPException(status_code=500, detail="Location config not found")
    with LOCATIONS_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise HTTPException(status_code=500, detail="Invalid location config format")
    return data


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "version": "v0.2"}


def _load_mvp(location_key: str) -> dict:
    file_path = DATA_DIR / f"{location_key}_mvp.json"
    if not file_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"MVP payload for '{location_key}' not found. Run build_all_locations.py first.",
        )

    with file_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_timeseries(location_key: str) -> list[dict]:
    file_path = DATA_DIR / f"{location_key}_timeseries.json"
    if not file_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Time series for '{location_key}' not found. Run build_all_locations.py first.",
        )

    with file_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


@app.get("/locations")
def get_locations() -> dict[str, dict[str, dict]]:
    return {"locations": _load_locations()}


@app.get("/location/{location_key}")
def get_location(location_key: str) -> dict:
    locations = _load_locations()
    if location_key not in locations:
        raise HTTPException(status_code=404, detail=f"Unknown location key: {location_key}")
    return _load_mvp(location_key)


@app.get("/location/{location_key}/summary")
def get_location_summary(location_key: str) -> dict[str, str]:
    locations = _load_locations()
    if location_key not in locations:
        raise HTTPException(status_code=404, detail=f"Unknown location key: {location_key}")

    payload = _load_mvp(location_key)
    summary = payload.get("interpretive_summary")
    if isinstance(summary, str) and summary.strip():
        return {"summary": summary}

    location = payload.get("location", "the selected location")
    data_range = payload.get("data_range", "the study period")
    trend = payload.get("mean_trend_mm_per_year")
    trend_text = f"{float(trend):.1f} mm per year" if isinstance(trend, (int, float)) else "an unknown rate"
    return {
        "summary": (
            f"Between {data_range}, modeled water levels near {location} increased at an average rate of {trend_text}."
        )
    }


@app.get("/location/{location_key}/timeseries")
def get_location_timeseries(location_key: str) -> list[dict]:
    locations = _load_locations()
    if location_key not in locations:
        raise HTTPException(status_code=404, detail=f"Unknown location key: {location_key}")
    return _load_timeseries(location_key)
