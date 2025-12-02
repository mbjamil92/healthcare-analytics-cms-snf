## SNF Multi-Year Runner
Runs Kyahn’s measure set across all provider archives and an optional live CMS pull.

How to run (from repo root):
python Code/snf_multi_year_runner.py \
  --county data/raw/Ballad-counties.csv \
  --archive-dir data/archive \
  --output-dir outputs \
  --include-live   # drop this flag to skip the live API pull

What it does:
- Loads all provider CSVs matching Skilled_Nursing_Facility_Quality_Reporting_Program_Provider_Data_*.csv under data/archive/.
- Optionally pulls the current CMS snapshot (fykj-qjee).
- Coalesces county/state columns and filters to Ballad counties.
- Pivots target measures, builds per-facility tables, and writes:
  - outputs/snf_multi_year_quality.csv
  - outputs/snf_trend_summary.csv

How to see output:
1. Make sure below output files are present:
    - outputs/snf_multi_year_quality.csv
    - outputs/snf_trend_summary.csv (optional)
2. From the repo root, start Streamlit:
    - streamlit run webapp/app.py