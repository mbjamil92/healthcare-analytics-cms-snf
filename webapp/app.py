"""
CMS SNF Quality Explorer
Interactive Streamlit app for exploring Skilled Nursing Facility (SNF)
quality and cost metrics using publicly available CMS datasets.

Usage:
    streamlit run webapp/app.py

The app automatically loads:
    - snf_multi_year_quality.csv (required)
    - snf_trend_summary.csv (optional)
from the ../outputs directory, or you may upload CSV files manually.
"""

from pathlib import Path
from typing import Optional, List

import altair as alt
import pandas as pd
import streamlit as st


def try_load_default(file_name: str) -> Optional[pd.DataFrame]:
    """Attempt to load a default CSV from ../outputs/."""
    root = Path(__file__).resolve().parents[1]
    path = root / "outputs" / file_name
    if path.exists():
        try:
            return pd.read_csv(path)
        except Exception:
            return None
    return None


def load_uploaded(label: str, help_text: str) -> Optional[pd.DataFrame]:
    upload = st.file_uploader(label, type=["csv"], help=help_text)
    if upload is not None:
        return pd.read_csv(upload)
    return None


def get_latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    if "snapshot_year" not in df.columns:
        return df.copy()
    latest = df["snapshot_year"].max()
    return df[df["snapshot_year"] == latest].copy()


def numeric_columns(df: pd.DataFrame) -> List[str]:
    return [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

def plot_top_bottom(df: pd.DataFrame, measure: str) -> alt.Chart:
    latest = get_latest_snapshot(df)
    cols = ["facility_name", "city", "state", measure]
    latest = latest.dropna(subset=[measure])
    top = latest.nsmallest(10, measure)
    bot = latest.nlargest(10, measure)
    combined = pd.concat([top.assign(rank_group="Top 10 (best)"), bot.assign(rank_group="Bottom 10 (worst)")])
    return (
        alt.Chart(combined)
        .mark_bar()
        .encode(
            x=alt.X(measure, title=measure),
            y=alt.Y("facility_name", sort="-x", title="Facility"),
            color="rank_group",
            tooltip=cols + ["rank_group"],
        )
        .properties(height=500)
    )


def plot_trend(trend_df: pd.DataFrame, measure: str) -> alt.Chart:
    filtered = trend_df[trend_df["measure"] == measure]
    return (
        alt.Chart(filtered)
        .mark_line(point=True)
        .encode(
            x=alt.X("snapshot_year:O", title="Year"),
            y=alt.Y("avg_score:Q", title="Average score"),
            tooltip=["snapshot_year", "avg_score", "median_score", "facilities"],
        )
        .properties(height=300)
    )


def scatter_cost_vs_quality(df: pd.DataFrame) -> alt.Chart:
    cost = "Medicare Spending Per Beneficiary (MSPB)"
    readm = "Preventable Readmission Rate"
    for col in (cost, readm):
        if col not in df.columns:
            raise ValueError("Required columns missing for scatter plot")
    latest = get_latest_snapshot(df).dropna(subset=[cost, readm])
    return (
        alt.Chart(latest)
        .mark_circle(size=80, opacity=0.7)
        .encode(
            x=alt.X(cost, title="MSPB (cost)"),
            y=alt.Y(readm, title="Preventable Readmission Rate"),
            tooltip=["facility_name", "city", "state", cost, readm],
        )
        .properties(height=400)
    )

def main() -> None:
    st.title("CMS SNF Quality Explorer")
    st.write("Upload the notebook outputs or let the app read defaults from ../outputs/ if present.")

    with st.expander("Data sources", expanded=True):
        st.markdown(
            """
            - Required: `snf_multi_year_quality.csv`
            - Optional: `snf_trend_summary.csv` for year-over-year lines
            - Defaults: if you already ran the notebook and saved to `outputs/`, the app will pick those up automatically.
            """
        )

    # Load data: uploaded first, fallback to defaults
    quality_df = load_uploaded(
        "Upload snf_multi_year_quality.csv",
        "Output from the notebook run (per-facility measures, multi-year)",
    ) or try_load_default("snf_multi_year_quality.csv")

    trend_df = load_uploaded(
        "Upload snf_trend_summary.csv (optional)",
        "Optional trend summary from the notebook",
    ) or try_load_default("snf_trend_summary.csv")

    if quality_df is None:
        st.warning("Please upload snf_multi_year_quality.csv or place it in ../outputs/.")
        return

    st.success(
        f"Loaded quality data: {len(quality_df):,} rows | columns: {len(quality_df.columns)}"
    )

    num_cols = numeric_columns(quality_df)
    if not num_cols:
        st.error("No numeric columns found to plot.")
        return

    measure = st.selectbox(
        "Choose a measure to rank",
        options=[
            m
            for m in [
                "Preventable Readmission Rate",
                "Pressure Ulcer Rate",
                "Fall with Major Injury Rate",
                "Healthcare-Associated Infection Rate",
                "Discharge to Community Rate",
                "Medicare Spending Per Beneficiary (MSPB)",
            ]
            if m in quality_df.columns
        ]
        or num_cols,
    )

    st.subheader("Top/Bottom facilities (latest snapshot)")
    st.altair_chart(plot_top_bottom(quality_df, measure), use_container_width=True)

    if trend_df is not None and not trend_df.empty and "measure" in trend_df.columns:
        if measure in trend_df["measure"].unique():
            st.subheader("Trend across years")
            st.altair_chart(plot_trend(trend_df, measure), use_container_width=True)

    # Cost vs readmission scatter
    try:
        st.subheader("Cost vs readmission (latest snapshot)")
        st.altair_chart(scatter_cost_vs_quality(quality_df), use_container_width=True)
    except ValueError:
        st.info("Cost vs readmission plot skipped (required columns missing).")


if __name__ == "__main__":
    main()
