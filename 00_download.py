"""Download every raw input the pipeline needs into data/.

Run this first, before 01_load_and_clean.py:

    python 00_download.py

Re-running is cheap - files that are already present and pass their integrity
check are skipped. Use --force to re-download everything.

Nothing under data/ is tracked by git: these three sources are the only inputs,
and every other file in data/ is regenerated from them by scripts 01-05.
"""

import argparse
import calendar
import json
import sys
import time
from pathlib import Path

import requests

# Months of yellow-taxi data to analyse. Extend this list to widen the range -
# the weather request derives its date span from it, so both sides stay in sync.
MONTHS = ["2024-01", "2024-02", "2024-03"]

# Lower Manhattan, the reference point for the weather lookup.
LATITUDE, LONGITUDE = 40.71, -74.01

# Pickup timestamps in the trip data are NYC local time, so the weather series
# has to be requested in the same zone or the hourly join silently misaligns.
TIMEZONE = "America/New_York"

TRIP_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"

DATA_DIR = Path(__file__).resolve().parent / "data"

CHUNK = 1 << 20  # 1 MiB
TIMEOUT = 60
ATTEMPTS = 3
BACKOFF = 3  # seconds, doubled each retry


def date_span(months):
    """First and last calendar day covered by months, as ISO dates."""
    if not months:
        raise ValueError("MONTHS is empty - list at least one month as 'YYYY-MM'")
    first, last = min(months), max(months)
    year, month = (int(part) for part in last.split("-"))
    return f"{first}-01", f"{last}-{calendar.monthrange(year, month)[1]:02d}"


def download(url, dest, params=None):
    """Stream url to dest, writing through a .part file.

    The rename at the end is atomic, so an interrupted or truncated download
    never leaves a half-written file behind that a later run would mistake for
    a complete one.
    """
    tmp = dest.parent / (dest.name + ".part")
    live = sys.stdout.isatty()
    last_error = None

    for attempt in range(1, ATTEMPTS + 1):
        try:
            with requests.get(url, params=params, stream=True, timeout=TIMEOUT) as response:
                response.raise_for_status()
                expected = int(response.headers.get("Content-Length") or 0)
                written = 0
                with open(tmp, "wb") as handle:
                    for chunk in response.iter_content(CHUNK):
                        handle.write(chunk)
                        written += len(chunk)
                        # Only animate on a terminal; in a log a carriage return
                        # just produces one line per megabyte.
                        if live:
                            total = f" / {expected / 1e6:.1f}" if expected else ""
                            print(f"\r    {written / 1e6:7.1f}{total} MB", end="", flush=True)
            if live:
                print("\r", end="")
            print(f"    {written / 1e6:7.1f} MB downloaded")

            # Content-Length mismatch means the connection dropped mid-body;
            # requests does not raise on a short read.
            if expected and written != expected:
                raise OSError(f"expected {expected} bytes, got {written}")

            tmp.replace(dest)
            return
        except (requests.RequestException, OSError) as error:
            last_error = error
            tmp.unlink(missing_ok=True)

            # A 4xx means the URL itself is wrong (bad month, renamed file):
            # retrying cannot fix it, so fail straight away.
            response = getattr(error, "response", None)
            if response is not None and 400 <= response.status_code < 500:
                break

            if attempt < ATTEMPTS:
                # Wait before retrying: the common transient failures here are a
                # DNS stub hiccup or a dropped connection, and neither recovers
                # within the microsecond an immediate retry would allow.
                pause = BACKOFF * 2 ** (attempt - 1)
                print(f"    attempt {attempt} failed ({error})")
                print(f"    retrying in {pause}s")
                time.sleep(pause)

    raise RuntimeError(f"could not download {url}: {last_error}") from last_error


def check_parquet(path):
    """Verify the Parquet magic number at both ends of the file."""
    size = path.stat().st_size
    if size < 8:
        raise ValueError("file is too small to be Parquet")
    with open(path, "rb") as handle:
        head = handle.read(4)
        handle.seek(-4, 2)
        foot = handle.read(4)
    if head != b"PAR1" or foot != b"PAR1":
        raise ValueError("missing PAR1 magic number - file is truncated or not Parquet")
    return f"{size / 1e6:.1f} MB"


def check_zone_csv(path):
    """Verify the lookup table has the columns 04_zone_enrichment.py joins on."""
    with open(path, encoding="utf-8") as handle:
        header = handle.readline().strip().replace('"', "").split(",")
        rows = sum(1 for _ in handle)
    for column in ("LocationID", "Borough", "Zone"):
        if column not in header:
            raise ValueError(f"column {column!r} missing from header {header}")
    return f"{rows} zones"


def check_weather(path, start, end):
    """Verify the hourly arrays are present, parallel, and span the date range."""
    with open(path, encoding="utf-8") as handle:
        payload = json.load(handle)

    if payload.get("error"):
        raise ValueError(f"API returned an error: {payload.get('reason')}")

    hourly = payload.get("hourly")
    if not hourly:
        raise ValueError("response has no 'hourly' block")

    # 03_weather_enrichment.py reads all three of these by name.
    series = {}
    for key in ("time", "temperature_2m", "precipitation"):
        if key not in hourly:
            raise ValueError(f"hourly.{key} missing from response")
        series[key] = hourly[key]

    lengths = {key: len(values) for key, values in series.items()}
    if len(set(lengths.values())) != 1:
        raise ValueError(f"hourly arrays have different lengths: {lengths}")

    times = series["time"]
    if not times[0].startswith(start) or not times[-1].startswith(end):
        raise ValueError(f"range is {times[0]}..{times[-1]}, expected {start}..{end}")

    if payload.get("timezone") != TIMEZONE:
        raise ValueError(f"timezone is {payload.get('timezone')!r}, expected {TIMEZONE!r}")

    return f"{len(times)} hours, {times[0]} .. {times[-1]}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-download files that are already present",
    )
    args = parser.parse_args()

    start, end = date_span(MONTHS)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    jobs = [
        (
            DATA_DIR / f"yellow_tripdata_{month}.parquet",
            TRIP_URL.format(month=month),
            None,
            check_parquet,
        )
        for month in MONTHS
    ]
    jobs.append((DATA_DIR / "taxi_zone_lookup.csv", ZONE_URL, None, check_zone_csv))
    jobs.append(
        (
            # Named 'archive' because that is the path 03_weather_enrichment.py reads.
            DATA_DIR / "archive",
            WEATHER_URL,
            {
                "latitude": LATITUDE,
                "longitude": LONGITUDE,
                "start_date": start,
                "end_date": end,
                "hourly": "temperature_2m,precipitation",
                "timezone": TIMEZONE,
            },
            lambda path: check_weather(path, start, end),
        )
    )

    print(f"Downloading {len(jobs)} files into {DATA_DIR}")
    failures = []

    for dest, url, params, validate in jobs:
        print(f"\n{dest.name}")

        if dest.exists() and not args.force:
            try:
                summary = validate(dest)
            except (OSError, ValueError, json.JSONDecodeError) as error:
                print(f"    present but failed its check ({error}) - re-downloading")
            else:
                print(f"    already present, skipping ({summary})")
                continue

        try:
            download(url, dest, params)
            print(f"    ok ({validate(dest)})")
        except (RuntimeError, OSError, ValueError, json.JSONDecodeError) as error:
            print(f"    FAILED: {error}")
            failures.append(dest.name)

    if failures:
        print(f"\n{len(failures)} file(s) failed: {', '.join(failures)}")
        print("Re-run this script to retry just those.")
        return 1

    print("\nAll raw inputs are in place. Next: python 01_load_and_clean.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
