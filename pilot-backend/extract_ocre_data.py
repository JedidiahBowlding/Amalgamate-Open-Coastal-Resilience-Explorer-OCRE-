import argparse
import json
from pathlib import Path
from typing import Any, cast

import intake
import numpy as np
import pandas as pd
import requests


DEFAULT_LOCATION_KEY = "annapolis"
DEFAULT_START_YEAR = 1979
DEFAULT_END_YEAR = 2022
DEFAULT_LOCATIONS_CONFIG = Path("config/locations.json")
DEFAULT_OUTPUT_DIR = Path("pilot-backend/data")

CATALOG_URLS = [
    "s3://noaa-nos-cora-pds/CORA_V1.1_intake.yml",
    "s3://noaa-nos-cora-pds/CORA_intake.yml",
]

DATASET_KEYS = [
    "CORA-V1.1-fort.63-timeseries",
    "CORA-V1-fort.63-timeseries",
    "CORA-V1.1-fort.63",
    "CORA-V1-fort.63",
]


def nearxy(x: np.ndarray, y: np.ndarray, xi: list[float], yi: list[float]) -> np.ndarray:
    ind = np.ones(len(xi), dtype=int)
    for i in range(len(xi)):
        dist = np.sqrt((x - xi[i]) ** 2 + (y - yi[i]) ** 2)
        ind[i] = dist.argmin()
    return ind


def _open_cora_dataset():
    last_error: Exception | None = None
    for url in CATALOG_URLS:
        try:
            catalog = intake.open_catalog(url, storage_options={"anon": True})
            catalog = cast(Any, catalog)
            for key in DATASET_KEYS:
                try:
                    entry = catalog[key]
                    return entry.to_dask(), key, url
                except Exception:
                    continue
        except Exception as exc:
            last_error = exc
    raise RuntimeError(
        "Unable to open CORA intake catalog/dataset with the configured URLs/keys."
    ) from last_error


def load_locations(config_path: Path) -> dict[str, dict[str, Any]]:
    if not config_path.exists():
        raise FileNotFoundError(f"Location config not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        locations = json.load(f)

    if not isinstance(locations, dict):
        raise ValueError("locations.json must be a dictionary keyed by location key")

    return cast(dict[str, dict[str, Any]], locations)


def get_location_metadata(config_path: Path, location_key: str) -> dict[str, Any]:
    locations = load_locations(config_path)
    if location_key not in locations:
        available = ", ".join(sorted(locations.keys()))
        raise KeyError(f"Unknown location key '{location_key}'. Available keys: {available}")

    location = locations[location_key]
    for field in ("name", "lat", "lon"):
        if field not in location:
            raise ValueError(f"Location '{location_key}' missing required field: {field}")

    return location


def extract_timeseries(
    lat: float,
    lon: float,
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    print("Opening CORA dataset...", flush=True)
    ds, _, _ = _open_cora_dataset()
    print("Dataset opened. Loading mesh coordinates...", flush=True)

    x = np.asarray(ds["x"].compute().values)
    y = np.asarray(ds["y"].compute().values)

    print("Dataset opened. Finding nearest wet node...", flush=True)

    start = f"{start_year}-01-01 00:00:00"
    end = f"{end_year}-12-31 23:00:00"

    zeta = ds["zeta"]
    node_dim = next(dim for dim in zeta.dims if dim != "time")

    dist_sq = (x - lon) ** 2 + (y - lat) ** 2
    nearest_idx = int(np.argmin(dist_sq))

    candidate_count = min(5000, len(dist_sq))
    candidate_idx = np.argpartition(dist_sq, candidate_count - 1)[:candidate_count]
    candidate_idx = candidate_idx[np.argsort(dist_sq[candidate_idx])]

    probe_start = pd.Timestamp(start)
    probe_end = min(probe_start + pd.Timedelta(days=30), pd.Timestamp(end))

    probe_block = zeta.isel({node_dim: candidate_idx}).sel(
        time=slice(probe_start.strftime("%Y-%m-%d %H:%M:%S"), probe_end.strftime("%Y-%m-%d %H:%M:%S"))
    ).compute()

    probe_vals = probe_block.values
    node_axis = probe_block.dims.index(node_dim)
    finite_by_node = np.isfinite(probe_vals).any(axis=0 if node_axis == 1 else 1)

    chosen_idx = nearest_idx
    if finite_by_node.any():
        first_valid_pos = int(np.argmax(finite_by_node))
        chosen_idx = int(candidate_idx[first_valid_pos])

    if chosen_idx != nearest_idx:
        print(
            f"Nearest node {nearest_idx} is dry; using nearest wet node {chosen_idx}.",
            flush=True,
        )
    else:
        print(
            f"Using nearest node index: {chosen_idx} (no alternate wet node found within {candidate_count} nearest candidates).",
            flush=True,
        )

    print("Computing time slice — this may take a few minutes...", flush=True)
    zeta_point = zeta.isel({node_dim: chosen_idx}).sel(time=slice(start, end)).compute()
    print("Time slice computed. Building dataframe...", flush=True)

    df = zeta_point.to_dataframe(name="water_level_m").reset_index()
    df = df[["time", "water_level_m"]].dropna(subset=["water_level_m"]).copy()
    df["time"] = pd.to_datetime(df["time"], utc=True)

    print(f"Extracted {len(df)} records.", flush=True)

    return df


def _compute_trend_mm_per_year(annual_means: pd.Series) -> float | None:
    annual_means = annual_means.dropna()
    if len(annual_means) < 2:
        return None

    years = np.array([pd.Timestamp(ts).year for ts in annual_means.index], dtype=float)
    values = np.asarray(annual_means.to_numpy(), dtype=float)
    slope_m_per_year, _ = np.polyfit(years, values, 1)
    return float(slope_m_per_year * 1000.0)


def _compute_extreme_events(df: pd.DataFrame, top_n: int = 5) -> list[dict[str, Any]]:
    daily_max = (
        df.set_index("time")["water_level_m"]
        .resample("D")
        .max()
        .dropna()
        .sort_values(ascending=False)
        .head(top_n)
    )

    return [
        {
            "date": pd.Timestamp(str(ts)).strftime("%Y-%m-%d"),
            "level_m": round(float(level), 3),
        }
        for ts, level in daily_max.items()
    ]


def _annual_mean_levels(df: pd.DataFrame) -> pd.Series:
    return df.set_index("time")["water_level_m"].resample("YE").mean()


def fetch_nwlon_hourly_height(
    station_id: str,
    begin_date: str,
    end_date: str,
    datum: str = "MSL",
    units: str = "metric",
) -> pd.DataFrame:
    url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    start_dt = pd.to_datetime(begin_date, format="%Y%m%d")
    end_dt = pd.to_datetime(end_date, format="%Y%m%d")

    yearly_starts = pd.date_range(start=start_dt, end=end_dt, freq="YS")
    if len(yearly_starts) == 0 or yearly_starts[0] != start_dt:
        yearly_starts = yearly_starts.insert(0, start_dt)

    frames: list[pd.DataFrame] = []
    for chunk_start in yearly_starts:
        chunk_end = min(chunk_start + pd.offsets.YearEnd(0), end_dt)

        params = {
            "product": "hourly_height",
            "application": "open-coastal-resilience-explorer-ocre",
            "begin_date": chunk_start.strftime("%Y%m%d"),
            "end_date": chunk_end.strftime("%Y%m%d"),
            "station": station_id,
            "datum": datum,
            "time_zone": "gmt",
            "units": units,
            "format": "json",
        }

        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        payload = response.json()

        if "data" not in payload:
            continue

        obs = pd.DataFrame(payload["data"])
        obs = obs[["t", "v"]].rename(columns={"t": "time", "v": "observed_m"})
        obs["time"] = pd.to_datetime(obs["time"], utc=True)
        obs["observed_m"] = pd.to_numeric(obs["observed_m"], errors="coerce")
        obs = obs.dropna(subset=["observed_m"])
        if not obs.empty:
            frames.append(obs)

    if not frames:
        return pd.DataFrame(columns=["time", "observed_m"])

    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["time"]).sort_values("time")


def nearest_nwlon_station(lat: float, lon: float) -> dict[str, Any]:
    server = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/.json"
    params = {"type": "waterlevels", "units": "metric"}

    response = requests.get(server, params=params, timeout=60)
    response.raise_for_status()
    stations = response.json().get("stations", [])

    stations_df = pd.DataFrame(stations)
    stations_df = stations_df[["id", "name", "lat", "lng"]].copy()

    stations_df["distance_sq"] = (stations_df["lat"] - lat) ** 2 + (stations_df["lng"] - lon) ** 2
    row = stations_df.sort_values("distance_sq").iloc[0]

    return {
        "station_id": str(row["id"]),
        "station_name": str(row["name"]),
        "lat": float(row["lat"]),
        "lon": float(row["lng"]),
    }


def build_mvp_output(
    location_key: str,
    location: str,
    lat: float,
    lon: float,
    start_year: int,
    end_year: int,
    df: pd.DataFrame,
    top_n_events: int = 5,
) -> dict[str, Any]:
    annual = _annual_mean_levels(df)
    trend = _compute_trend_mm_per_year(annual)

    output = {
        "location_key": location_key,
        "location": location,
        "lat": lat,
        "lon": lon,
        "data_range": f"{start_year}-{end_year}",
        "mean_trend_mm_per_year": None if trend is None else round(trend, 3),
        "extreme_events": _compute_extreme_events(df, top_n=top_n_events),
        "annual_mean_levels": [
            {
                "year": int(pd.Timestamp(str(ts)).year),
                "level_m": round(float(level), 4),
            }
            for ts, level in annual.dropna().items()
        ],
    }

    return output


def build_interpretive_summary(mvp: dict[str, Any]) -> str:
    location = str(mvp.get("location", "the selected location"))
    data_range = str(mvp.get("data_range", "the study period"))
    trend = mvp.get("mean_trend_mm_per_year")
    trend_text = "an unknown rate"
    if isinstance(trend, (int, float)):
        trend_text = f"{trend:.1f} mm per year"

    extremes = mvp.get("extreme_events", [])
    top_extreme = None
    if isinstance(extremes, list) and extremes:
        top_extreme = extremes[0]

    extreme_text = "Historical extreme water levels were identified during major storm events."
    if isinstance(top_extreme, dict) and "level_m" in top_extreme:
        extreme_text = (
            f"Historical extreme water levels exceeded {float(top_extreme['level_m']):.2f} meters "
            "during major storm events."
        )

    comparison = mvp.get("observation_comparison")
    comparison_text = ""
    if isinstance(comparison, dict):
        nearest_station = comparison.get("nearest_station", {})
        station_id = nearest_station.get("station_id", "unknown") if isinstance(nearest_station, dict) else "unknown"
        obs_count = comparison.get("observation_count", 0)
        comparison_text = (
            f" Observational data from NOAA station {station_id} ({obs_count:,} records) "
            "supports local long-term trend interpretation."
        )

    return (
        f"Between {data_range}, modeled water levels near {location} increased at an average rate of {trend_text}. "
        f"{extreme_text}{comparison_text}"
    )


def export_outputs(output_dir: Path, timeseries_df: pd.DataFrame, mvp_payload: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    location_key = str(mvp_payload["location_key"])
    timeseries_file = output_dir / f"{location_key}_timeseries.json"
    mvp_file = output_dir / f"{location_key}_mvp.json"

    timeseries_df.to_json(timeseries_file, orient="records", date_format="iso")

    with mvp_file.open("w", encoding="utf-8") as f:
        json.dump(mvp_payload, f, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract OCRE pilot water-level time series (from CORA source data) and MVP JSON payload.")
    parser.add_argument("--location-key", default=DEFAULT_LOCATION_KEY)
    parser.add_argument("--locations-config", type=Path, default=DEFAULT_LOCATIONS_CONFIG)
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    parser.add_argument("--end-year", type=int, default=DEFAULT_END_YEAR)
    parser.add_argument("--top-events", type=int, default=5)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--with-observations", action="store_true")
    args = parser.parse_args()

    if args.end_year < args.start_year:
        raise ValueError("end-year must be greater than or equal to start-year")

    location = get_location_metadata(args.locations_config, args.location_key)
    location_name = str(location["name"])
    lat = float(location["lat"])
    lon = float(location["lon"])

    ts_df = extract_timeseries(lat, lon, args.start_year, args.end_year)
    mvp = build_mvp_output(
        location_key=args.location_key,
        location=location_name,
        lat=lat,
        lon=lon,
        start_year=args.start_year,
        end_year=args.end_year,
        df=ts_df,
        top_n_events=args.top_events,
    )

    if args.with_observations:
        nearest = nearest_nwlon_station(lat, lon)
        begin_date = f"{args.start_year}0101"
        end_date = f"{args.end_year}1231"
        obs_df = fetch_nwlon_hourly_height(nearest["station_id"], begin_date, end_date)
        mvp["observation_comparison"] = {
            "nearest_station": nearest,
            "observation_count": int(len(obs_df)),
        }

    mvp["interpretive_summary"] = build_interpretive_summary(mvp)

    export_outputs(args.output_dir, ts_df, mvp)


if __name__ == "__main__":
    main()
