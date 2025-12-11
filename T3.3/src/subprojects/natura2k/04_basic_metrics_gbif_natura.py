"""
Script: 04_basic_metrics_gbif_natura.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Description:
Computes basic summary metrics from the spatially joined
GBIF–Natura2k dataset.

The script aggregates occurrence-level data to:

1) Natura site level (per SITECODE)
   - number of occurrences inside each site
   - number of distinct species
   - temporal coverage (min/max year, number of years), if 'year' is available

2) Member State level (per MS)
   - number of occurrences inside Natura2k sites
   - number of sites with data
   - number of distinct species
   - temporal coverage, if 'year' is available

Input:
- Joined GBIF–Natura2k dataset (GeoPackage), produced by:
  03_spatial_join_gbif_natura.py

  Location:
  data/processed/<DATASET_NAME>/GBIF_<DATASET_NAME>_with_natura_sites.gpkg

Output:
- Site-level metrics (per SITECODE):
  results/natura2k/<DATASET_NAME>/sites_metrics_<DATASET_NAME>.csv

- Member-State-level metrics (per MS):
  results/natura2k/<DATASET_NAME>/ms_metrics_<DATASET_NAME>.csv

Notes:
- The script is dataset-agnostic. By switching DATASET_NAME
  it can be applied to BIRDS / HABITATS / IAS.
- Geometry is not used for aggregation and is dropped for efficiency.
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
RESULTS_ROOT = PROJECT_ROOT / "results" / "natura2k"

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
DATASET_NAME = "BIRDS"

# Joined GBIF–Natura2k dataset (output of 03_spatial_join_gbif_natura.py)
PROCESSED_DIR = PROCESSED_BASE / DATASET_NAME
JOINED_GPKG = PROCESSED_DIR / f"GBIF_{DATASET_NAME}_with_natura_sites.gpkg"

# Results directory for this dataset
RESULTS_DIR = RESULTS_ROOT / DATASET_NAME
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

SITES_METRICS_CSV = RESULTS_DIR / f"sites_metrics_{DATASET_NAME}.csv"
MS_METRICS_CSV = RESULTS_DIR / f"ms_metrics_{DATASET_NAME}.csv"

# ----------------------------------------------------------------------
# Logging setup
# ----------------------------------------------------------------------
LOG_DIR = PROJECT_ROOT / "logs" / "natura2k" / "04_basic_metrics_gbif_natura"
LOG_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M")
logfile = LOG_DIR / f"basic_metrics_{DATASET_NAME}_{timestamp}.log"

logger = logging.getLogger(f"basic_metrics_{DATASET_NAME}")
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

file_handler = logging.FileHandler(logfile, mode="a", encoding="utf-8")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)


def log(msg: str) -> None:
    """Log message to stdout and to the basic metrics log file."""
    print(msg)
    logger.info(msg)

def cast_int_columns(df: pd.DataFrame, columns) -> pd.DataFrame:
    """
    Convert numeric columns (e.g. years, counts) to nullable integer (Int64)
    so that values are stored as plain integers in the CSV (e.g. 1998, not 1998.0).
    """
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df

# ----------------------------------------------------------------------
# Sanity checks
# ----------------------------------------------------------------------
log("=== Basic metrics computation for GBIF–Natura2k joined dataset started ===")
log(f"Dataset:                {DATASET_NAME}")
log(f"Joined input GPKG:      {rel(JOINED_GPKG)}")
log(f"Site metrics output:    {rel(SITES_METRICS_CSV)}")
log(f"MS metrics output:      {rel(MS_METRICS_CSV)}")

if not JOINED_GPKG.exists():
    raise FileNotFoundError(f"Joined GBIF–Natura2k file not found: {JOINED_GPKG}")

# ----------------------------------------------------------------------
# Load joined dataset
# ----------------------------------------------------------------------
log("\nLoading joined GBIF–Natura2k dataset...")
gdf_joined = gpd.read_file(JOINED_GPKG)

log(f"Loaded joined table with {len(gdf_joined)} records.")
log(f"Columns: {', '.join(gdf_joined.columns.astype(str))}")

# Drop geometry for aggregation (not needed for counts)
df = pd.DataFrame(gdf_joined.drop(columns="geometry", errors="ignore"))

# ----------------------------------------------------------------------
# Column configuration
# ----------------------------------------------------------------------
# Natura2k columns (from prepared layer and spatial join)
SITE_CODE_COL = "SITECODE"
SITE_NAME_COL = "SITENAME"
MS_COL = "MS"          # Member State code
SITETYPE_COL = "SITETYPE"

# GBIF columns
SPECIES_COL = "scientificName"
YEAR_COL = "year"
TAXON_KEY_COL = "taxonKey"

# Check presence of expected columns
missing_cols = [
    col
    for col in [
        SITE_CODE_COL,
        SITE_NAME_COL,
        MS_COL,
        SITETYPE_COL,
        SPECIES_COL,
        TAXON_KEY_COL,
    ]
    if col not in df.columns
]

if missing_cols:
    log("WARNING: Some expected columns are missing in the joined dataset:")
    for c in missing_cols:
        log(f"  - {c}")
    log("Basic metrics will be computed using available columns only.")

has_year = YEAR_COL in df.columns
if not has_year:
    log("INFO: 'year' column not found – temporal metrics will be skipped.")

# ----------------------------------------------------------------------
# Filter: occurrences that are inside a Natura2k site
# ----------------------------------------------------------------------
log("\nFiltering occurrences that have a Natura2k site code...")

if SITE_CODE_COL in df.columns:
    df_in_site = df[df[SITE_CODE_COL].notna()].copy()
else:
    df_in_site = df.iloc[0:0].copy()  # empty
    log("WARNING: No SITECODE column found – df_in_site will be empty.")

log(f"Total occurrences:               {len(df)}")
log(f"Occurrences inside Natura sites: {len(df_in_site)}")

if df_in_site.empty:
    log("No occurrences linked to Natura2k sites – no metrics will be produced.")
    log("=== Basic metrics computation completed (no data) ===")
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

# Cast numeric year-related columns to integers (if present)
sites_metrics = cast_int_columns(
    sites_metrics,
    ["year_min", "year_max", "n_years"]
)


log(f"Computed site-level metrics for {len(sites_metrics)} Natura2k sites.")

# Save site metrics
sites_metrics.to_csv(SITES_METRICS_CSV, index=False)
log(f"Site-level metrics saved to: {rel(SITES_METRICS_CSV)}")

# ----------------------------------------------------------------------
# Member State (MS) level metrics
# ----------------------------------------------------------------------
log("\nComputing Member State (MS) level metrics...")

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

    # Cast numeric year-related columns to integers (if present)
    ms_metrics = cast_int_columns(
    ms_metrics,
    ["year_min", "year_max", "n_years"]
    )

    log(f"Computed MS-level metrics for {len(ms_metrics)} Member States.")

    ms_metrics.to_csv(MS_METRICS_CSV, index=False)
    log(f"MS-level metrics saved to: {rel(MS_METRICS_CSV)}")
else:
    log("WARNING: MS column not found – MS-level metrics will not be produced.")

log("\n=== Basic metrics computation completed successfully ===")