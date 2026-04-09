"""
Script: natura_analysis/06_uncertainty_metrics_gbif_natura.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Description:
Computes uncertainty-aware descriptive metrics from the GBIF–Natura
uncertainty-enriched dataset produced by the spatial join workflow.

This script focuses on the uncertainty branch of the analysis and
summarises:

1) Overall uncertainty-class distribution
   - number of occurrences per uncertainty class
   - percentage per uncertainty class

2) Member State-level uncertainty metrics (per MS and uncertainty_class)
   - number of occurrences
   - number of distinct Natura sites
   - number of distinct species

3) Site-type-level uncertainty metrics (per SITETYPE and uncertainty_class)
   - number of occurrences
   - number of distinct Natura sites
   - number of distinct species

4) Summary statistics for uncertainty-related numeric fields
   - coordinate uncertainty
   - distance to Natura boundary
   - distance to nearest Natura polygon

Input:
- Uncertainty-enriched GBIF–Natura dataset (GeoPackage), produced by:
  03_spatial_join_gbif_natura.py

  Location:
  data/processed/<DATASET_NAME>/GBIF_<DATASET_NAME>_natura_spatial_uncertainty.gpkg

Outputs:
- Overall uncertainty summary:
  results/natura_analysis/<DATASET_NAME>/
      GBIF_<DATASET_NAME>_uncertainty_summary_metrics.csv

- Member State-level uncertainty metrics:
  results/natura_analysis/<DATASET_NAME>/
      GBIF_<DATASET_NAME>_uncertainty_ms_metrics.csv

- Site-type-level uncertainty metrics:
  results/natura_analysis/<DATASET_NAME>/
      GBIF_<DATASET_NAME>_uncertainty_sitetype_metrics.csv

- Numeric uncertainty field summaries:
  results/natura_analysis/<DATASET_NAME>/
      GBIF_<DATASET_NAME>_uncertainty_numeric_summary.csv

Notes:
- The script is dataset-agnostic. By switching DATASET_NAME
  it can be applied to BIRDS / HABITATS / IAS.
- Geometry is not used for aggregation and is dropped for efficiency.
- This script operates only on the uncertainty-enriched output.
  Baseline metrics are computed separately.
"""

from pathlib import Path
from datetime import datetime
import logging

import geopandas as gpd
import pandas as pd

# ----------------------------------------------------------------------
# Project-level paths
# ----------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]  # .../BMD Implementation

DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_BASE = DATA_DIR / "processed"
RESULTS_ROOT = PROJECT_ROOT / "results" / "natura_analysis"


def rel(path: Path) -> str:
    """
    Return path as a string relative to the project root,
    so that absolute system paths do not appear in logs.
    """
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except Exception:
        return str(path)


# ----------------------------------------------------------------------
# User configuration
# ----------------------------------------------------------------------
# Dataset to analyse: "BIRDS", "HABITATS" or "IAS"
DATASET_NAME = "IAS"

# Uncertainty-enriched GBIF–Natura dataset
PROCESSED_DIR = PROCESSED_BASE / DATASET_NAME / "natura_analysis"
UNCERT_GPKG = PROCESSED_DIR / f"GBIF_{DATASET_NAME}_natura_spatial_uncertainty.gpkg"

# Results directory for this dataset
RESULTS_DIR = RESULTS_ROOT / DATASET_NAME
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

UNCERT_SUMMARY_CSV = RESULTS_DIR / f"GBIF_{DATASET_NAME}_uncertainty_summary_metrics.csv"
UNCERT_MS_CSV = RESULTS_DIR / f"GBIF_{DATASET_NAME}_uncertainty_ms_metrics.csv"
UNCERT_SITETYPE_CSV = RESULTS_DIR / f"GBIF_{DATASET_NAME}_uncertainty_sitetype_metrics.csv"
UNCERT_NUMERIC_CSV = RESULTS_DIR / f"GBIF_{DATASET_NAME}_uncertainty_numeric_summary.csv"

# ----------------------------------------------------------------------
# Logging setup
# ----------------------------------------------------------------------
LOG_DIR = PROJECT_ROOT / "logs" / "natura_analysis" / "06_uncertainty_metrics_gbif_natura"
LOG_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M")
logfile = LOG_DIR / f"uncertainty_metrics_{DATASET_NAME}_{timestamp}.log"

logger = logging.getLogger(f"uncertainty_metrics_{DATASET_NAME}")
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

file_handler = logging.FileHandler(logfile, mode="a", encoding="utf-8")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)


def log(msg: str) -> None:
    """Log message to stdout and to the uncertainty metrics log file."""
    print(msg)
    logger.info(msg)


def cast_int_columns(df: pd.DataFrame, columns) -> pd.DataFrame:
    """
    Convert numeric columns (counts/IDs) to nullable integer (Int64)
    so that values are stored as plain integers in CSV files.
    """
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


# ----------------------------------------------------------------------
# Sanity checks
# ----------------------------------------------------------------------
log("=== Uncertainty metrics computation for GBIF–Natura dataset started ===")
log(f"Dataset:                         {DATASET_NAME}")
log(f"Uncertainty input GPKG:         {rel(UNCERT_GPKG)}")
log(f"Overall summary output:         {rel(UNCERT_SUMMARY_CSV)}")
log(f"MS uncertainty output:          {rel(UNCERT_MS_CSV)}")
log(f"Site-type uncertainty output:   {rel(UNCERT_SITETYPE_CSV)}")
log(f"Numeric summary output:         {rel(UNCERT_NUMERIC_CSV)}")

if not UNCERT_GPKG.exists():
    raise FileNotFoundError(f"Uncertainty GBIF–Natura file not found: {UNCERT_GPKG}")

# ----------------------------------------------------------------------
# Load uncertainty dataset
# ----------------------------------------------------------------------
log("\nLoading uncertainty-enriched GBIF–Natura dataset...")
gdf_unc = gpd.read_file(UNCERT_GPKG)

log(f"Loaded joined table with {len(gdf_unc)} records.")
log(f"Columns: {', '.join(gdf_unc.columns.astype(str))}")

# Drop geometry for aggregation
df = pd.DataFrame(gdf_unc.drop(columns="geometry", errors="ignore"))

# ----------------------------------------------------------------------
# Column configuration
# ----------------------------------------------------------------------
SITE_CODE_COL = "SITECODE"
MS_COL = "MS"
SITETYPE_COL = "SITETYPE"
SPECIES_COL = "scientificName"
TAXON_KEY_COL = "taxonKey"
UNC_CLASS_COL = "uncertainty_class"

UNC_M_COL = "uncertainty_m"
DIST_BOUND_COL = "dist_to_boundary_m"
DIST_POLY_COL = "dist_to_polygon_m"

missing_cols = [
    col
    for col in [
        UNC_CLASS_COL,
        SITE_CODE_COL,
        MS_COL,
        SITETYPE_COL,
        SPECIES_COL,
        TAXON_KEY_COL,
    ]
    if col not in df.columns
]

if missing_cols:
    log("WARNING: Some expected columns are missing in the uncertainty dataset:")
    for c in missing_cols:
        log(f"  - {c}")
    log("Metrics will be computed using available columns only.")

if UNC_CLASS_COL not in df.columns:
    raise KeyError(
        f"Required column '{UNC_CLASS_COL}' not found in uncertainty dataset."
    )

# ----------------------------------------------------------------------
# 1) Overall uncertainty summary
# ----------------------------------------------------------------------
log("\nComputing overall uncertainty-class summary...")

uncert_summary = (
    df[UNC_CLASS_COL]
    .value_counts(dropna=False)
    .rename_axis("uncertainty_class")
    .reset_index(name="n_occurrences")
)

uncert_summary["percentage"] = (
    uncert_summary["n_occurrences"] / len(df) * 100
).round(2)

uncert_summary = cast_int_columns(uncert_summary, ["n_occurrences"])

uncert_summary.to_csv(UNCERT_SUMMARY_CSV, index=False)
log(f"Overall uncertainty summary saved to: {rel(UNCERT_SUMMARY_CSV)}")

# ----------------------------------------------------------------------
# 2) Member State-level uncertainty metrics
# ----------------------------------------------------------------------
log("\nComputing Member State-level uncertainty metrics...")

if MS_COL in df.columns:
    df_ms = df[df[MS_COL].notna()].copy()

    if not df_ms.empty:
        group_ms = df_ms.groupby([MS_COL, UNC_CLASS_COL], dropna=True)

        ms_metrics = group_ms.agg(
            n_occurrences=(TAXON_KEY_COL, "size"),
            n_sites=(SITE_CODE_COL, "nunique"),
            n_species=(SPECIES_COL, "nunique"),
        ).reset_index()

        # Add percentages within each Member State
        ms_totals = (
            ms_metrics.groupby(MS_COL)["n_occurrences"]
            .sum()
            .rename("ms_total_occurrences")
            .reset_index()
        )

        ms_metrics = ms_metrics.merge(ms_totals, on=MS_COL, how="left")
        ms_metrics["percentage_within_ms"] = (
            ms_metrics["n_occurrences"] / ms_metrics["ms_total_occurrences"] * 100
        ).round(2)

        ms_metrics = cast_int_columns(
            ms_metrics,
            ["n_occurrences", "n_sites", "n_species", "ms_total_occurrences"]
        )

        ms_metrics.to_csv(UNCERT_MS_CSV, index=False)
        log(f"Member State-level uncertainty metrics saved to: {rel(UNCERT_MS_CSV)}")
    else:
        log("WARNING: No non-null Member State values found – MS-level metrics were not produced.")
else:
    log("WARNING: MS column not found – MS-level uncertainty metrics were not produced.")

# ----------------------------------------------------------------------
# 3) Site-type-level uncertainty metrics
# ----------------------------------------------------------------------
log("\nComputing site-type-level uncertainty metrics...")

if SITETYPE_COL in df.columns:
    df_type = df[df[SITETYPE_COL].notna()].copy()

    if not df_type.empty:
        group_type = df_type.groupby([SITETYPE_COL, UNC_CLASS_COL], dropna=True)

        sitetype_metrics = group_type.agg(
            n_occurrences=(TAXON_KEY_COL, "size"),
            n_sites=(SITE_CODE_COL, "nunique"),
            n_species=(SPECIES_COL, "nunique"),
        ).reset_index()

        # Add percentages within each site type
        type_totals = (
            sitetype_metrics.groupby(SITETYPE_COL)["n_occurrences"]
            .sum()
            .rename("sitetype_total_occurrences")
            .reset_index()
        )

        sitetype_metrics = sitetype_metrics.merge(
            type_totals, on=SITETYPE_COL, how="left"
        )
        sitetype_metrics["percentage_within_sitetype"] = (
            sitetype_metrics["n_occurrences"] / sitetype_metrics["sitetype_total_occurrences"] * 100
        ).round(2)

        sitetype_metrics = cast_int_columns(
            sitetype_metrics,
            ["n_occurrences", "n_sites", "n_species", "sitetype_total_occurrences"]
        )

        sitetype_metrics.to_csv(UNCERT_SITETYPE_CSV, index=False)
        log(f"Site-type-level uncertainty metrics saved to: {rel(UNCERT_SITETYPE_CSV)}")
    else:
        log("WARNING: No non-null SITETYPE values found – site-type metrics were not produced.")
else:
    log("WARNING: SITETYPE column not found – site-type uncertainty metrics were not produced.")

# ----------------------------------------------------------------------
# 4) Numeric uncertainty field summaries
# ----------------------------------------------------------------------
log("\nComputing numeric uncertainty field summaries...")

numeric_fields = [
    UNC_M_COL,
    DIST_BOUND_COL,
    DIST_POLY_COL,
]

numeric_summary_rows = []

for col in numeric_fields:
    if col in df.columns:
        s = pd.to_numeric(df[col], errors="coerce")

        numeric_summary_rows.append(
            {
                "field": col,
                "count_non_null": int(s.notna().sum()),
                "mean": s.mean(),
                "std": s.std(),
                "min": s.min(),
                "median": s.median(),
                "max": s.max(),
            }
        )
    else:
        log(f"INFO: Numeric field '{col}' not found – skipped.")

numeric_summary = pd.DataFrame(numeric_summary_rows)

if not numeric_summary.empty:
    numeric_summary = cast_int_columns(numeric_summary, ["count_non_null"])
    numeric_summary.to_csv(UNCERT_NUMERIC_CSV, index=False)
    log(f"Numeric uncertainty summary saved to: {rel(UNCERT_NUMERIC_CSV)}")
else:
    log("WARNING: No numeric uncertainty fields were available – numeric summary was not produced.")

log("\n=== Uncertainty metrics computation completed successfully ===")