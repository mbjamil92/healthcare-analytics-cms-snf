"""
CMS SNF Quality – Multi-Year Loader & Trend Summarizer

Author: Bilal Jamil - Data Scientist
Description:
    This script processes publicly available CMS Skilled Nursing Facility (SNF)
    Quality Reporting Program (QRP) datasets across multiple archived years and
    optionally includes the latest CMS API snapshot (dataset: fykj-qjee).

Functionality:
    - Loads all archived CMS SNF QRP CSVs located under data/archive/
      (e.g., Skilled_Nursing_Facility_Quality_Reporting_Program_Provider_Data_*.csv)
    - Optionally pulls the most recent CMS snapshot via API
    - Standardizes county/state metadata and filters to a user-provided list
      of counties (via --county)
    - Pivots target quality and cost measures to create:
        outputs/snf_multi_year_quality.csv
        outputs/snf_trend_summary.csv

Usage (run from repo root):
    python Code/snf_multi_year_runner.py \
        --county data/raw/counties.csv \
        --archive-dir data/archive \
        --output-dir outputs \
        --include-live

Notes:
    - Requires CMS archive CSVs to be downloaded/unzipped into data/archive/
    - Works for any region; simply update counties.csv as needed.
"""


from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import requests


# Dataset configuration
DATASET_ID = "fykj-qjee"  # SNF Quality Reporting Program dataset ID

# Measures of interest (Kyahn's set)
MEASURE_MAP = {
    "S_038_02_ADJ_RATE": "Pressure Ulcer Rate",
    "S_013_02_OBS_RATE": "Fall with Major Injury Rate",
    "S_004_01_PPR_PD_RSRR": "Preventable Readmission Rate",
    "S_006_01_MSPB_SCORE": "Medicare Spending Per Beneficiary (MSPB)",
    "S_005_02_DTC_RS_RATE": "Discharge to Community Rate",
    "S_039_01_HAI_RS_RATE": "Healthcare-Associated Infection Rate",
    "S_007_02_OBS_RATE": "Medication Review Rate",
    "S_024_05_OBS_RATE": "Self-Care at Discharge",
    "S_025_05_OBS_RATE": "Mobility at Discharge",
}


def load_counties(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df["County"] = df["County"].str.upper()
    df["StateCode"] = df["StateCode"].str.upper()
    return df


def fetch_current_snapshot(dataset_id: str) -> pd.DataFrame:
    meta_url = f"https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/{dataset_id}?show-reference-ids=true"
    resp = requests.get(meta_url)
    resp.raise_for_status()
    meta = resp.json()
    download_url = meta["distribution"][0]["data"]["downloadURL"]
    try:
        df = pd.read_csv(download_url, low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(download_url, low_memory=False, encoding="latin1")
    df["snapshot_year"] = pd.Timestamp.now().year
    df["snapshot_label"] = "current_api"
    return df


def load_archives(archive_dir: Path) -> List[pd.DataFrame]:
    frames: List[pd.DataFrame] = []
    for path in sorted(archive_dir.glob("Skilled_Nursing_Facility_Quality_Reporting_Program_Provider_Data_*.csv")):
        year_token = path.stem.split("_")[-1]
        try:
            snap_year = int(year_token[-4:])
        except ValueError:
            snap_year = None
        try:
            df = pd.read_csv(path, low_memory=False, encoding="utf-8", encoding_errors="replace")
        except TypeError:
            df = pd.read_csv(path, low_memory=False, encoding="utf-8", errors="replace")
        df["snapshot_year"] = snap_year
        df["snapshot_label"] = path.name
        frames.append(df)
    print(f"Found {len(frames)} archive files in {archive_dir}")
    return frames


def normalize_county(val: Any) -> str:
    s = str(val).upper().strip()
    for suffix in (" COUNTY", " PARISH"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s


def filter_to_counties(df: pd.DataFrame, counties: pd.DataFrame) -> pd.DataFrame:
    county_cols = [c for c in df.columns if c.lower() in ["county name", "county/parish", "county"]]
    state_cols = [c for c in df.columns if c.lower() in ["provider state", "state"]]
    if not county_cols or not state_cols:
        print("No county/state cols found; skipping filter")
        return df

    county_std = df[county_cols[0]].copy()
    for c in county_cols[1:]:
        county_std = county_std.fillna(df[c])

    state_std = df[state_cols[0]].copy()
    for c in state_cols[1:]:
        state_std = state_std.fillna(df[c])

    df = df.copy()
    df["county_std"] = county_std.apply(normalize_county)
    df["state_std"] = state_std.astype(str).str.strip().str.upper()

    counties_norm = counties.copy()
    counties_norm["County"] = counties_norm["County"].apply(normalize_county)
    counties_norm["StateCode"] = counties_norm["StateCode"].astype(str).str.strip().str.upper()

    allowed = set(zip(counties_norm["County"], counties_norm["StateCode"]))
    df["county_state_key"] = list(zip(df["county_std"], df["state_std"]))
    return df[df["county_state_key"].isin(allowed)].copy()


def pivot_measures(df: pd.DataFrame) -> pd.DataFrame:
    # Try explicit, then fallback
    measure_col = 'Measure Code' if 'Measure Code' in df.columns else next((c for c in df.columns if 'measure' in c.lower() and 'code' in c.lower()), None)
    provider_col = 'CMS Certification Number (CCN)' if 'CMS Certification Number (CCN)' in df.columns else next((c for c in df.columns if 'provider' in c.lower() and 'number' in c.lower()), None)
    score_col = 'Score' if 'Score' in df.columns else next((c for c in df.columns if c.lower() == 'score'), None)
    if not (measure_col and provider_col and score_col):
        print('⚠️ Could not locate measure/provider/score columns')
        return pd.DataFrame()

    subset = df[df[measure_col].isin(MEASURE_MAP.keys())].copy()
    subset['score_numeric'] = pd.to_numeric(subset[score_col], errors='coerce')
    pivot = subset.pivot_table(
        index=[provider_col, 'snapshot_year', 'snapshot_label'],
        columns=measure_col,
        values='score_numeric',
        aggfunc='first'
    ).reset_index()
    pivot = pivot.rename(columns=MEASURE_MAP)
    pivot = pivot.rename(columns={provider_col: 'provider_id'})
    return pivot

def build_facility_table(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        "provider_id": ["CMS Certification Number (CCN)", "Federal Provider Number", "Provider Number", "provider_number"],
        "facility_name": ["Provider Name", "Facility Name", "facility_name", "provider_name"],
        "address": ["Address Line 1", "Address", "address", "Street Address"],
        "city": ["City/Town", "City", "city"],
        "state": ["State", "state", "Provider State"],
        "zip_code": ["ZIP Code", "zip_code", "Zip Code", "zip"],
        "county_name": ["County/Parish", "County Name", "county_name", "County", "county"],
        "phone_number": ["Telephone Number", "Phone Number", "phone_number", "Phone", "phone"],
    }
    col_map: Dict[str, str] = {}
    for target, candidates in mapping.items():
        for c in candidates:
            if c in df.columns:
                col_map[target] = c
                break
    if "provider_id" not in col_map:
        print("⚠️ Provider/CCN column not found")
        return pd.DataFrame()
    keep = list(col_map.values()) + ["snapshot_year", "snapshot_label"]
    keep = [c for c in keep if c in df.columns]
    facilities = df[keep].drop_duplicates(subset=[col_map["provider_id"], "snapshot_year"])
    facilities = facilities.rename(columns={v: k for k, v in col_map.items()})
    return facilities


def summarize_trends(quality_wide: pd.DataFrame) -> pd.DataFrame:
    if quality_wide.empty:
        return quality_wide
    id_cols = [c for c in ["provider_id", "snapshot_year", "snapshot_label"] if c in quality_wide.columns]
    long_df = quality_wide.melt(id_vars=id_cols, var_name="measure", value_name="score").dropna()
    trend = (
        long_df.groupby(["measure", "snapshot_year"])
        .agg(facilities=("provider_id", "nunique"), avg_score=("score", "mean"), median_score=("score", "median"))
        .reset_index()
        .sort_values(["measure", "snapshot_year"])
    )
    return trend


def run(county_csv: Path, archive_dir: Path, output_dir: Path, include_live: bool = True) -> None:
    counties = load_counties(county_csv)
    archive_frames = load_archives(archive_dir)
    frames = archive_frames.copy()
    if include_live:
        try:
            frames.append(fetch_current_snapshot(DATASET_ID))
            print("✓ Pulled current CMS snapshot via API")
        except Exception as exc:
            print(f"⚠️ Live API pull failed: {exc}")

    if not frames:
        raise SystemExit("No data loaded. Add archives or enable live API.")

    snf_raw = pd.concat(frames, ignore_index=True)
    print("Value Counts before filter:\n", snf_raw["snapshot_year"].value_counts())
    snf_filtered = filter_to_counties(snf_raw, counties)
    print(f"Rows after county filter: {len(snf_filtered):,}")
    print("Value Counts after filter:\n", snf_filtered["snapshot_year"].value_counts())

    facilities = build_facility_table(snf_filtered)
    quality = pivot_measures(snf_filtered)
    merged = pd.merge(quality, facilities, how="left", on=["provider_id", "snapshot_year", "snapshot_label"])

    measure_cols = [m for m in MEASURE_MAP.values() if m in merged.columns]
    if measure_cols:
        merged["composite_raw"] = merged[measure_cols].rank(pct=True, na_option="bottom", axis=0).mean(axis=1)

    trend_summary = summarize_trends(quality)

    output_dir.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_dir / "snf_multi_year_quality.csv", index=False)
    trend_summary.to_csv(output_dir / "snf_trend_summary.csv", index=False)
    print(f"Exports written to {output_dir}/snf_multi_year_quality.csv and snf_trend_summary.csv")


def main() -> None:
    parser = argparse.ArgumentParser(description="SNF multi-year analysis")
    parser.add_argument("--county", type=Path, default=Path("data/raw/Ballad-counties.csv"), help="Path to county CSV")
    parser.add_argument("--archive-dir", type=Path, default=Path("data/archive"), help="Directory with provider CSVs")
    parser.add_argument("--output-dir", type=Path, default=Path("outputs"), help="Where to write outputs")
    parser.add_argument("--include-live", action="store_true", help="Include current CMS API snapshot")
    args = parser.parse_args()

    run(args.county, args.archive_dir, args.output_dir, include_live=args.include_live)


if __name__ == "__main__":
    main()
