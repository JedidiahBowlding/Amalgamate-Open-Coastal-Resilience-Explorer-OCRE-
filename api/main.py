from pathlib import Path
import json

from fastapi import FastAPI, HTTPException


app = FastAPI(title="Open-Coastal-Resilience-Explorer-OCRE API", version="0.1.0")

MVP_PATH = Path("pilot-backend/annapolis_mvp.json")
TIMESERIES_PATH = Path("pilot-backend/annapolis_timeseries.json")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/pilot/annapolis")
def get_annapolis_summary() -> dict:
    if not MVP_PATH.exists():
        raise HTTPException(status_code=404, detail="MVP payload not found. Run extract_ocre_data.py first.")

    with MVP_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


@app.get("/pilot/annapolis/timeseries")
def get_annapolis_timeseries() -> list[dict]:
    if not TIMESERIES_PATH.exists():
        raise HTTPException(status_code=404, detail="Time series not found. Run extract_ocre_data.py first.")

    with TIMESERIES_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)
