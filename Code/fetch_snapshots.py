"""Download archived CMS SNF Quality snapshots (fykj-qjee).

Usage (from repo root):
  python Code/fetch_snapshots.py

By default this pulls the December snapshots for 2024/2023/2022 and
writes them to data/archive/. Adjust `ARCHIVE_MONTHS` if you want other months.

Note: This does network I/O. If you’re offline or behind a proxy, update
the URLs accordingly or download manually.
"""

from pathlib import Path
import sys
import requests

DATASET_ID = "fykj-qjee"
ARCHIVE_MONTHS = ["2024_12", "2023_12", "2022_12"]  # adjust as needed


def download_file(url: str, dest: Path) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with requests.get(url, stream=True, timeout=60) as r:
            if r.status_code != 200:
                print(f"❌ {url} -> HTTP {r.status_code}")
                return False
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print(f"✓ downloaded {dest}")
        return True
    except Exception as exc:
        print(f"❌ {url} -> {exc}")
        return False


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    archive_dir = root / "data" / "archive"

    successes = 0
    for month in ARCHIVE_MONTHS:
        url = f"https://data.cms.gov/provider-data/sites/default/files/archive/{DATASET_ID}_{month}.csv"
        dest = archive_dir / f"{DATASET_ID}_{month}.csv"
        if dest.exists():
            print(f"… skipping existing {dest}")
            successes += 1
            continue
        if download_file(url, dest):
            successes += 1

    print(f"Done. {successes}/{len(ARCHIVE_MONTHS)} files present.")
    return 0 if successes else 1


if __name__ == "__main__":
    sys.exit(main())
