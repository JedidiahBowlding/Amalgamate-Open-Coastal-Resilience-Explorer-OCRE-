# Data Management Plan (Pilot)

## Scope
- Pilot location: Annapolis, MD (38.9784, -76.4922)
- Data source: NOAA CORA v1.1 water level (`fort.63`) time series
- Time range: 1979-2022

## Data Products
- `pilot-backend/annapolis_timeseries.json`: hourly modeled water levels
- `pilot-backend/annapolis_mvp.json`: API-ready summary payload for frontend

## Quality and Validation
- Primary dataset: CORA modeled water levels
- Optional validation: compare with nearest NWLON station observations
- Wave datasets are excluded in this pilot due to known v1.1 anomaly notice

## Access and Reproducibility
- Backend extraction script: `pilot-backend/extract_cora_data.py`
- API layer: `api/main.py`
- Environment: `environment.yml`
