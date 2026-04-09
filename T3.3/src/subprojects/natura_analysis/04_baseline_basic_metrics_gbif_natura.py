"""
Script: natura_analysis/04_baseline_basic_metrics_gbif_natura.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Description:
Computes baseline descriptive metrics from the spatially joined
GBIF–Natura dataset produced by the baseline point-in-polygon workflow.

The script summarises the joined dataset at multiple levels and produces:

1) Overall baseline summary
   - total number of occurrences
   - number and percentage of occurrences inside Natura sites
   - number and percentage of occurrences outside Natura sites

2) Natura site-level metrics (per SITECODE)
   - number of occurrences inside each site
   - number of distinct species
   - temporal coverage (min/max year, number of years), if 'year' is available

3) Member State-level metrics (per MS)
   - number of occurrences inside Natura sites
   - number of sites with data
   - number of distinct species
   - temporal coverage, if 'year' is available

4) Site-type-level summary (per SITETYPE)
   - number of occurrences inside Natura sites
   - number of distinct Natura sites
   - number of distinct species
   - temporal coverage, if 'year' is available

Input:
- Baseline joined GBIF–Natura dataset (GeoPackage), produced by:
  03_spatial_join_gbif_natura.py

  Location:
  data/processed/<DATASET_NAME>/GBIF_<DATASET_NAME>_natura_spatial_baseline.gpkg

Outputs:
- Overall baseline summary:
  results/natura_analysis/<DATASET_NAME>/GBIF_<DATASET_NAME>_baseline_basic_summary.csv

- Site-level metrics:
  results/natura_analysis/<DATASET_NAME>/GBIF_<DATASET_NAME>_baseline_sites_metrics.csv

- Member-State-level metrics:
  results/natura_analysis/<DATASET_NAME>/GBIF_<DATASET_NAME>_baseline_ms_metrics.csv

- Site-type-level summary:
  results/natura_analysis/<DATASET_NAME>/GBIF_<DATASET_NAME>_baseline_sitetype_summary.csv

Notes:
- The script is dataset-agnostic. By switching DATASET_NAME
  it can be applied to BIRDS / HABITATS / IAS.
- Geometry is not used for aggregation and is dropped for efficiency.
- This script summarises only the baseline point-in-polygon results.
  Uncertainty-aware metrics should be handled separately.
"""

from pathlib import Path
from datetime import datetime

import geopandas as gpd
import pandas as pd
import logging

# ----------------------------------------------------------------------
# Project-level paths
# ----------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]  # .../BMD Implementation

DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_BASE = DATA_DIR / "processed"
RESULTS_ROOT = PROJECT_ROOT / "results" / "natura_analysis"

# Helper: relative paths for logging
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
DATASET_NAME = "IAS"  # Change as needed

# Baseline joined GBIF–Natura dataset
PROCESSED_DIR = PROCESSED_BASE / DATASET_NAME / "natura_analysis"
JOINED_GPKG = PROCESSED_DIR / f"GBIF_{DATASET_NAME}_natura_spatial_baseline.gpkg"

# Results directory for this dataset
RESULTS_DIR = RESULTS_ROOT / DATASET_NAME
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


BASELINE_SUMMARY_CSV = RESULTS_DIR / f"GBIF_{DATASET_NAME}_baseline_basic_summary.csv"
SITES_METRICS_CSV = RESULTS_DIR / f"GBIF_{DATASET_NAME}_baseline_sites_metrics.csv"
MS_METRICS_CSV = RESULTS_DIR / f"GBIF_{DATASET_NAME}_baseline_ms_metrics.csv"
SITETYPE_SUMMARY_CSV = RESULTS_DIR / f"GBIF_{DATASET_NAME}_baseline_sitetype_summary.csv"

# ----------------------------------------------------------------------
# Logging setup
# ----------------------------------------------------------------------
LOG_DIR = PROJECT_ROOT / "logs" / "natura_analysis" / "04_baseline_basic_metrics_gbif_natura"
LOG_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M")
logfile = LOG_DIR / f"baseline_basic_metrics_{DATASET_NAME}_{timestamp}.log"

logger = logging.getLogger(f"baseline_metrics_{DATASET_NAME}")
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

file_handler = logging.FileHandler(logfile, mode="a", encoding="utf-8")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)


def log(msg: str) -> None:
    """Log message to stdout and to the baseline metrics log file."""
    print(msg)
    logger.info(msg)


def cast_int_columns(df: pd.DataFrame, columns) -> pd.DataFrame:
    """
    Convert numeric columns (e.g. years, counts) to nullable integer (Int64)
    so that values are stored as plain integers in CSV files.
    """
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


# ----------------------------------------------------------------------
# Sanity checks
# ----------------------------------------------------------------------
log("=== Baseline metrics computation for GBIF–Natura joined dataset started ===")
log(f"Dataset:                    {DATASET_NAME}")
log(f"Baseline input GPKG:        {rel(JOINED_GPKG)}")
log(f"Baseline summary output:    {rel(BASELINE_SUMMARY_CSV)}")
log(f"Site metrics output:        {rel(SITES_METRICS_CSV)}")
log(f"MS metrics output:          {rel(MS_METRICS_CSV)}")
log(f"Site-type summary output:   {rel(SITETYPE_SUMMARY_CSV)}")

if not JOINED_GPKG.exists():
    raise FileNotFoundError(f"Joined GBIF–Natura file not found: {JOINED_GPKG}")

# ----------------------------------------------------------------------
# Load joined dataset
# ----------------------------------------------------------------------
log("\nLoading baseline joined GBIF–Natura dataset...")
gdf_joined = gpd.read_file(JOINED_GPKG)

log(f"Loaded joined table with {len(gdf_joined)} records.")
log(f"Columns: {', '.join(gdf_joined.columns.astype(str))}")

# Drop geometry for aggregation (not needed for counts)
df = pd.DataFrame(gdf_joined.drop(columns="geometry", errors="ignore"))

# ----------------------------------------------------------------------
# Column configuration
# ----------------------------------------------------------------------
# Natura columns
SITE_CODE_COL = "SITECODE"
SITE_NAME_COL = "SITENAME"
MS_COL = "MS"
SITETYPE_COL = "SITETYPE"

# GBIF columns
SPECIES_COL = "scientificName"
YEAR_COL = "year"
TAXON_KEY_COL = "taxonKey"
GBIF_ID_COL = "gbifID"

# Check presence of expected columns
missing_cols = [
    col for col in [
        SITE_CODE_COL,
        SITE_NAME_COL,
        MS_COL,
        SITETYPE_COL,
        SPECIES_COL,
        TAXON_KEY_COL,
        GBIF_ID_COL,
    ]
    if col not in df.columns
]

if missing_cols:
    log("WARNING: Some expected columns are missing in the joined dataset:")
    for c in missing_cols:
        log(f"  - {c}")
    log("Baseline metrics will be computed using available columns only.")

has_year = YEAR_COL in df.columns
if not has_year:
    log("INFO: 'year' column not found – temporal metrics will be skipped.")

# ----------------------------------------------------------------------
# Overall baseline summary
# ----------------------------------------------------------------------
log("\nComputing overall baseline summary...")

n_total = len(df)

if SITE_CODE_COL in df.columns:
    n_inside = int(df[SITE_CODE_COL].notna().sum())
    n_outside = int(df[SITE_CODE_COL].isna().sum())
else:
    n_inside = 0
    n_outside = n_total
    log("WARNING: SITECODE column not found – inside/outside counts default to 0 / total.")

inside_pct = round((n_inside / n_total) * 100, 2) if n_total > 0 else 0.0
outside_pct = round((n_outside / n_total) * 100, 2) if n_total > 0 else 0.0

baseline_summary = pd.DataFrame([
    {
        "dataset_name": DATASET_NAME,
        "n_total_occurrences": n_total,
        "n_occurrences_inside_natura": n_inside,
        "n_occurrences_outside_natura": n_outside,
        "pct_inside_natura": inside_pct,
        "pct_outside_natura": outside_pct,
    }
])

baseline_summary = cast_int_columns(
    baseline_summary,
    ["n_total_occurrences", "n_occurrences_inside_natura", "n_occurrences_outside_natura"]
)

baseline_summary.to_csv(BASELINE_SUMMARY_CSV, index=False)
log(f"Baseline summary saved to: {rel(BASELINE_SUMMARY_CSV)}")
log(f"Total occurrences: {n_total}")
log(f"Inside Natura: {n_inside} ({inside_pct:.2f}%)")
log(f"Outside Natura: {n_outside} ({outside_pct:.2f}%)")

# ----------------------------------------------------------------------
# Filter: occurrences that are inside a Natura site
# ----------------------------------------------------------------------
log("\nFiltering occurrences linked to Natura sites...")

if SITE_CODE_COL in df.columns:
    df_in_site = df[df[SITE_CODE_COL].notna()].copy()
else:
    df_in_site = df.iloc[0:0].copy()
    log("WARNING: No SITECODE column found – inside-site metrics will be empty.")

log(f"Occurrences inside Natura sites: {len(df_in_site)}")

if df_in_site.empty:
    log("No occurrences linked to Natura sites – only baseline summary was produced.")
    log("=== Baseline metrics computation completed (summary only) ===")
    raise SystemExit(0)

# ----------------------------------------------------------------------
# Site-level metrics (per SITECODE)
# ----------------------------------------------------------------------
log("\nComputing site-level metrics (per SITECODE)...")

group_site = df_in_site.groupby(SITE_CODE_COL, dropna=True)

agg_dict = {
    "site_name": (SITE_NAME_COL, "first"),
    "ms": (MS_COL, "first"),
    "site_type": (SITETYPE_COL, "first"),
    "n_occurrences": (TAXON_KEY_COL, "size"),
    "n_species": (SPECIES_COL, "nunique"),
}

if has_year:
    agg_dict.update(
        {
            "year_min": (YEAR_COL, "min"),
            "year_max": (YEAR_COL, "max"),
            "n_years": (YEAR_COL, "nunique"),
        }
    )

sites_metrics = group_site.agg(**agg_dict).reset_index()

sites_metrics = cast_int_columns(
    sites_metrics,
    ["n_occurrences", "n_species", "year_min", "year_max", "n_years"]
)

log(f"Computed site-level metrics for {len(sites_metrics)} Natura sites.")
sites_metrics.to_csv(SITES_METRICS_CSV, index=False)
log(f"Site-level metrics saved to: {rel(SITES_METRICS_CSV)}")

# ----------------------------------------------------------------------
# Member State-level metrics (per MS)
# ----------------------------------------------------------------------
log("\nComputing Member State-level metrics (per MS)...")

if MS_COL in df_in_site.columns:
    group_ms = df_in_site.groupby(MS_COL, dropna=True)

    ms_agg = {
        "n_occurrences": (TAXON_KEY_COL, "size"),
        "n_sites": (SITE_CODE_COL, "nunique"),
        "n_species": (SPECIES_COL, "nunique"),
    }

    if has_year:
        ms_agg.update(
            {
                "year_min": (YEAR_COL, "min"),
                "year_max": (YEAR_COL, "max"),
                "n_years": (YEAR_COL, "nunique"),
            }
        )

    ms_metrics = group_ms.agg(**ms_agg).reset_index()

    ms_metrics = cast_int_columns(
        ms_metrics,
        ["n_occurrences", "n_sites", "n_species", "year_min", "year_max", "n_years"]
    )

    log(f"Computed MS-level metrics for {len(ms_metrics)} Member States.")
    ms_metrics.to_csv(MS_METRICS_CSV, index=False)
    log(f"MS-level metrics saved to: {rel(MS_METRICS_CSV)}")
else:
    log("WARNING: MS column not found – MS-level metrics were not produced.")

# ----------------------------------------------------------------------
# Site-type-level summary (per SITETYPE)
# ----------------------------------------------------------------------
log("\nComputing site-type-level summary (per SITETYPE)...")

if SITETYPE_COL in df_in_site.columns:
    group_type = df_in_site.groupby(SITETYPE_COL, dropna=True)

    sitetype_agg = {
        "n_occurrences": (TAXON_KEY_COL, "size"),
        "n_sites": (SITE_CODE_COL, "nunique"),
        "n_species": (SPECIES_COL, "nunique"),
    }

    if has_year:
        sitetype_agg.update(
            {
                "year_min": (YEAR_COL, "min"),
                "year_max": (YEAR_COL, "max"),
                "n_years": (YEAR_COL, "nunique"),
            }
        )

    sitetype_summary = group_type.agg(**sitetype_agg).reset_index()

    sitetype_summary = cast_int_columns(
        sitetype_summary,
        ["n_occurrences", "n_sites", "n_species", "year_min", "year_max", "n_years"]
    )

    log(f"Computed site-type-level summary for {len(sitetype_summary)} site types.")
    sitetype_summary.to_csv(SITETYPE_SUMMARY_CSV, index=False)
    log(f"Site-type-level summary saved to: {rel(SITETYPE_SUMMARY_CSV)}")
else:
    log("WARNING: SITETYPE column not found – site-type summary was not produced.")

log("\n=== Baseline metrics computation completed successfully ===")