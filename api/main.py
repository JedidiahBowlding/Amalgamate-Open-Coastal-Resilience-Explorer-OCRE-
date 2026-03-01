from pathlib import Path
import json

from fastapi import FastAPI, HTTPException


app = FastAPI(title="Open-Coastal-Resilience-Explorer-OCRE API", version="0.1.0")

MVP_PATH = Path("pilot-backend/annapolis_mvp.json")
TIMESERIES_PATH = Path("pilot-backend/annapolis_timeseries.json")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def _load_mvp() -> dict:
    if not MVP_PATH.exists():
        raise HTTPException(status_code=404, detail="MVP payload not found. Run extract_ocre_data.py first.")

    with MVP_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


@app.get("/pilot/annapolis")
def get_annapolis_summary() -> dict:
    return _load_mvp()


@app.get("/annapolis")
def get_annapolis() -> dict:
    return _load_mvp()


@app.get("/annapolis/summary")
def get_annapolis_interpretive_summary() -> dict[str, str]:
    payload = _load_mvp()
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


@app.get("/pilot/annapolis/timeseries")
def get_annapolis_timeseries() -> list[dict]:
    if not TIMESERIES_PATH.exists():
        raise HTTPException(status_code=404, detail="Time series not found. Run extract_ocre_data.py first.")

    with TIMESERIES_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)
