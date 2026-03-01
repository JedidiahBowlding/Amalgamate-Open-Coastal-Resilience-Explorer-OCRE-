import argparse
import json
import subprocess
import sys
from pathlib import Path


DEFAULT_LOCATIONS_CONFIG = Path("config/locations.json")
DEFAULT_OUTPUT_DIR = Path("pilot-backend/data")


def load_locations(config_path: Path) -> dict[str, dict]:
    with config_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("locations.json must be a dictionary keyed by location key")
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description="Build OCRE output JSON for all configured locations.")
    parser.add_argument("--locations-config", type=Path, default=DEFAULT_LOCATIONS_CONFIG)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start-year", type=int, default=1979)
    parser.add_argument("--end-year", type=int, default=2022)
    parser.add_argument("--with-observations", action="store_true")
    args = parser.parse_args()

    locations = load_locations(args.locations_config)
    extractor = Path("pilot-backend/extract_ocre_data.py")

    for location_key, meta in locations.items():
        command = [
            sys.executable,
            str(extractor),
            "--location-key",
            location_key,
            "--locations-config",
            str(args.locations_config),
            "--output-dir",
            str(args.output_dir),
            "--start-year",
            str(args.start_year),
            "--end-year",
            str(args.end_year),
        ]
        if args.with_observations:
            command.append("--with-observations")

        print(f"Building {location_key} ({meta.get('name', location_key)})...")
        subprocess.run(command, check=True)

        mvp_path = args.output_dir / f"{location_key}_mvp.json"
        ts_path = args.output_dir / f"{location_key}_timeseries.json"

        with mvp_path.open("r", encoding="utf-8") as f:
            mvp = json.load(f)

        events = len(mvp.get("extreme_events", []))
        obs = mvp.get("observation_comparison", {}).get("observation_count")
        obs_text = f", observations={obs:,}" if isinstance(obs, int) else ""
        print(
            f"  Done: trend={mvp.get('mean_trend_mm_per_year')} mm/yr, "
            f"extreme_events={events}{obs_text}, timeseries_file={ts_path.name}"
        )


if __name__ == "__main__":
    main()
