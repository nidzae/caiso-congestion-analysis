"""
download_oasis.py
Downloads 12 months of DAM Nomogram/Branch Shadow Prices from CAISO OASIS.
Outputs one consolidated CSV.
"""
import requests
import zipfile
import io
import os
import time
from datetime import datetime, timedelta

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "oasis_raw")
CONSOLIDATED_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "shadow_prices_12mo.csv")

# 12 months back from today
END_DATE = datetime(2026, 4, 1)
START_DATE = datetime(2025, 4, 1)

BASE_URL = "http://oasis.caiso.com/oasisapi/SingleZip"

MAX_RETRIES = 3
RETRY_DELAY = 30


def fetch_month(start_dt, end_dt):
    """Fetch one chunk of shadow price data (max 31 days)."""
    params = {
        "queryname": "PRC_NOMOGRAM",
        "market_run_id": "DAM",
        "startdatetime": start_dt.strftime("%Y%m%dT07:00-0000"),
        "enddatetime": end_dt.strftime("%Y%m%dT07:00-0000"),
        "resultformat": "6",
        "version": "12",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"  Fetching {start_dt.date()} to {end_dt.date()} (attempt {attempt})...")
        try:
            resp = requests.get(BASE_URL, params=params, timeout=120)

            if resp.status_code != 200:
                print(f"  WARNING: HTTP {resp.status_code} for {start_dt.date()}-{end_dt.date()}")
                if attempt < MAX_RETRIES:
                    print(f"  Retrying in {RETRY_DELAY}s...")
                    time.sleep(RETRY_DELAY)
                    continue
                return None

            # Response is a ZIP containing one CSV
            z = zipfile.ZipFile(io.BytesIO(resp.content))
            csv_name = z.namelist()[0]
            return z.read(csv_name).decode("utf-8")

        except Exception as e:
            print(f"  WARNING: Request failed: {e}")
            if attempt < MAX_RETRIES:
                print(f"  Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
                continue
            return None

    return None


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_rows = []
    header = None

    # Chunk into ~30-day windows
    current_start = START_DATE
    chunk_num = 0

    while current_start < END_DATE:
        current_end = min(current_start + timedelta(days=30), END_DATE)

        csv_text = fetch_month(current_start, current_end)

        if csv_text:
            lines = csv_text.strip().split("\n")
            if chunk_num == 0:
                header = lines[0]
                all_rows.extend(lines)  # include header for first chunk
            else:
                all_rows.extend(lines[1:])  # skip header on subsequent chunks

            # Save raw chunk
            chunk_file = os.path.join(
                OUTPUT_DIR,
                f"chunk_{chunk_num:02d}_{current_start.strftime('%Y%m%d')}_{current_end.strftime('%Y%m%d')}.csv",
            )
            with open(chunk_file, "w") as f:
                f.write(csv_text)
            print(f"  Saved chunk {chunk_num}: {len(lines)-1} data rows")
        else:
            print(f"  FAILED: No data for {current_start.date()} to {current_end.date()}")

        chunk_num += 1
        current_start = current_end
        time.sleep(5)  # Be polite to OASIS servers

    # Write consolidated file
    consolidated_path = os.path.abspath(CONSOLIDATED_FILE)
    os.makedirs(os.path.dirname(consolidated_path), exist_ok=True)
    with open(consolidated_path, "w") as f:
        f.write("\n".join(all_rows))

    print(f"\nDone. {len(all_rows)-1} data rows written to {consolidated_path}")


if __name__ == "__main__":
    main()
