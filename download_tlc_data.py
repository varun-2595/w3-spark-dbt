"""
Downloads REAL NYC TLC Yellow Taxi trip data (Parquet) and the taxi zone lookup CSV.

Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
Files are hosted on the TLC CloudFront CDN.

Usage:
    python download_tlc_data.py                  # downloads default months (2024-01, 2024-02)
    python download_tlc_data.py 2024-03 2024-04  # downloads specific months
"""

import os
import sys
import urllib.request

from src.config import DATA_DIR, ZONE_LOOKUP_PATH, TLC_MONTHS

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
CHUNK_SIZE = 1024 * 1024  # 1 MB


def download_file(url: str, dest_path: str) -> None:
    """Streams a remote file to disk with progress reporting."""
    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
        print(f"[SKIP] Already exists: {dest_path} ({os.path.getsize(dest_path):,} bytes)")
        return

    print(f"[GET ] {url}")
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request) as response:
        total = int(response.headers.get("Content-Length", 0))
        downloaded = 0
        tmp_path = dest_path + ".part"
        with open(tmp_path, "wb") as f:
            while True:
                chunk = response.read(CHUNK_SIZE)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded * 100 // total
                    print(f"\r       {downloaded:,}/{total:,} bytes ({pct}%)", end="", flush=True)
        print()
        os.replace(tmp_path, dest_path)
    print(f"[ OK ] Saved: {dest_path} ({os.path.getsize(dest_path):,} bytes)")


def main() -> None:
    months = sys.argv[1:] if len(sys.argv) > 1 else TLC_MONTHS
    os.makedirs(DATA_DIR, exist_ok=True)

    for month in months:
        filename = f"yellow_tripdata_{month}.parquet"
        download_file(f"{TLC_BASE_URL}/{filename}", os.path.join(DATA_DIR, filename))

    download_file(ZONE_LOOKUP_URL, ZONE_LOOKUP_PATH)
    print("\nAll downloads complete. Real NYC TLC data is ready in ./data/")


if __name__ == "__main__":
    main()
