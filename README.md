## SNF Multi-Year Runner
Runs a consolidated set of CMS Skilled Nursing Facility (SNF) quality measures across all archived provider files and an optional live CMS API pull.

How to run (from repo root):
python Code/snf_multi_year_runner.py \
  --county data/raw/counties.csv \
  --archive-dir data/archive \
  --output-dir outputs \
  --include-live   # drop this flag to skip the live API pull

What it does:
- Loads all provider CSVs matching Skilled_Nursing_Facility_Quality_Reporting_Program_Provider_Data_*.csv under data/archive/.
- Optionally pulls the current CMS snapshot (fykj-qjee).
- Coalesces county/state columns and filters to counties in the data/archive/counties.csv file
- Pivots target measures, builds per-facility tables, and writes:
  - outputs/snf_multi_year_quality.csv
  - outputs/snf_trend_summary.csv

How to see output:
1. Make sure below output files are present:
    - outputs/snf_multi_year_quality.csv
    - outputs/snf_trend_summary.csv (optional)
2. From the repo root, start Streamlit:
    - streamlit run webapp/app.py
