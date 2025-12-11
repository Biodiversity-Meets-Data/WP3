"""
Script: 05_advanced_metrics_gbif_natura.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Description:
Computes advanced metrics from the spatially joined GBIF–Natura2k dataset.

This script builds on top of the basic metrics and focuses on:

1) Species-level metrics (per taxonKey)
   - number of occurrences inside Natura2k sites
   - number of distinct Natura2k sites (SITECODE)
   - number of Member States (MS)
   - temporal coverage (min/max year, number of years)
   - temporal span (max - min)
   - temporal completeness (fraction of observed years within the span)

2) Site-type-level metrics (per SITETYPE)
   - number of Natura2k sites with at least one occurrence
   - number of occurrences inside Natura2k sites
   - number of distinct species
   - temporal coverage

3) Temporal gaps per Natura2k site (per SITECODE)
   - min/max year with data
   - number of years with observations
   - expected number of years (continuous interval)
   - missing years (gap)
   - gap fraction (missing / expected)

Input:
- Joined GBIF–Natura2k dataset (GeoPackage), produced by:
  03_spatial_join_gbif_natura.py

  Location:
  data/processed/<DATASET_NAME>/GBIF_<DATASET_NAME>_with_natura_sites.gpkg

Output:
- Species-level metrics:
  results/natura2k/<DATASET_NAME>/species_metrics_<DATASET_NAME>.csv

- Site-type-level metrics:
  results/natura2k/<DATASET_NAME>/sitetype_metrics_<DATASET_NAME>.csv

- Site-level temporal gaps:
  results/natura2k/<DATASET_NAME>/sites_temporal_gaps_<DATASET_NAME>.csv

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

SPECIES_METRICS_CSV = RESULTS_DIR / f"species_metrics_{DATASET_NAME}.csv"
SITETYPE_METRICS_CSV = RESULTS_DIR / f"sitetype_metrics_{DATASET_NAME}.csv"
SITES_GAPS_CSV = RESULTS_DIR / f"sites_temporal_gaps_{DATASET_NAME}.csv"

# ----------------------------------------------------------------------
# Logging setup
# ----------------------------------------------------------------------
LOG_DIR = PROJECT_ROOT / "logs" / "natura2k" / "05_advanced_metrics_gbif_natura"
LOG_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M")
logfile = LOG_DIR / f"advanced_metrics_{DATASET_NAME}_{timestamp}.log"

logger = logging.getLogger(f"advanced_metrics_{DATASET_NAME}")
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

file_handler = logging.FileHandler(logfile, mode="a", encoding="utf-8")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)


def log(msg: str) -> None:
    """Log message to stdout and to the advanced metrics log file."""
    print(msg)
    logger.info(msg)

def cast_int_columns(df: pd.DataFrame, columns) -> pd.DataFrame:
    """
    Convert numeric columns (e.g. IDs, years, counts) to nullable integer (Int64)
    so that values are stored as plain integers in the CSV (e.g. 1998, not 1998.0).
    """
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df

# ----------------------------------------------------------------------
# Sanity checks
# ----------------------------------------------------------------------
log("=== Advanced metrics computation for GBIF–Natura2k joined dataset started ===")
log(f"Dataset:                     {DATASET_NAME}")
log(f"Joined input GPKG:           {rel(JOINED_GPKG)}")
log(f"Species metrics output:      {rel(SPECIES_METRICS_CSV)}")
log(f"Site-type metrics output:    {rel(SITETYPE_METRICS_CSV)}")
log(f"Site temporal gaps output:   {rel(SITES_GAPS_CSV)}")

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
    log("Advanced metrics will be computed using available columns only.")

has_year = YEAR_COL in df.columns
if not has_year:
    log("INFO: 'year' column not found – temporal metrics will be limited.")

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
    log("No occurrences linked to Natura2k sites – no advanced metrics will be produced.")
    log("=== Advanced metrics computation completed (no data) ===")
    raise SystemExit(0)

# ----------------------------------------------------------------------
# 1) Species-level metrics (per taxonKey)
# ----------------------------------------------------------------------
log("\nComputing species-level metrics (per taxonKey)...")

if TAXON_KEY_COL in df_in_site.columns:
    group_species = df_in_site.groupby(TAXON_KEY_COL, dropna=True)

    species_agg = {
        "scientificName": (SPECIES_COL, "first"),
        "n_occurrences_natura": (TAXON_KEY_COL, "size"),
        "n_sites": (SITE_CODE_COL, "nunique"),
        "n_ms": (MS_COL, "nunique"),
    }

    if has_year:
        species_agg.update(
            {
                "year_min": (YEAR_COL, "min"),
                "year_max": (YEAR_COL, "max"),
                "n_years": (YEAR_COL, "nunique"),
            }
        )

    species_metrics = group_species.agg(**species_agg).reset_index()

    # Add temporal span and completeness if year info is available
    if has_year:
        species_metrics["temporal_span"] = (
            species_metrics["year_max"] - species_metrics["year_min"]
        )
        # Avoid division by zero: span 0 -> completeness = 1 if only one year
        span = species_metrics["temporal_span"].replace(0, pd.NA)
        species_metrics["temporal_completeness"] = species_metrics["n_years"] / (
            span + 1
        )
        # If span was originally 0 (single year), set completeness to 1.0
        species_metrics.loc[
            species_metrics["temporal_span"] == 0, "temporal_completeness"
        ] = 1.0

    # Cast integer-like columns (IDs, years, span) to Int64 for clean CSV
    cols_to_int = [TAXON_KEY_COL]
    if has_year:
        cols_to_int += ["year_min", "year_max", "n_years", "temporal_span"]
    species_metrics = cast_int_columns(species_metrics, cols_to_int)

    log(f"Computed species-level metrics for {len(species_metrics)} taxa.")
    species_metrics.to_csv(SPECIES_METRICS_CSV, index=False)
    log(f"Species-level metrics saved to: {rel(SPECIES_METRICS_CSV)}")
else:
    log("WARNING: taxonKey column not found – species-level metrics will not be produced.")

# ----------------------------------------------------------------------
# 2) Site-type-level metrics (per SITETYPE)
# ----------------------------------------------------------------------
log("\nComputing site-type-level metrics (per SITETYPE)...")

if SITETYPE_COL in df_in_site.columns:
    group_type = df_in_site.groupby(SITETYPE_COL, dropna=True)

    sitetype_agg = {
        "n_sites": (SITE_CODE_COL, "nunique"),
        "n_occurrences": (TAXON_KEY_COL, "size"),
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

    sitetype_metrics = group_type.agg(**sitetype_agg).reset_index()

    # Cast year-related columns to Int64 for clean CSV
    if has_year:
        sitetype_metrics = cast_int_columns(
            sitetype_metrics,
            ["year_min", "year_max", "n_years"]
        )

    log(f"Computed site-type-level metrics for {len(sitetype_metrics)} site types.")
    sitetype_metrics.to_csv(SITETYPE_METRICS_CSV, index=False)
    log(f"Site-type-level metrics saved to: {rel(SITETYPE_METRICS_CSV)}")
else:
    log("WARNING: SITETYPE column not found – site-type metrics will not be produced.")

# ----------------------------------------------------------------------
# 3) Temporal gaps per Natura2k site (per SITECODE)
# ----------------------------------------------------------------------
log("\nComputing temporal gaps per Natura2k site (per SITECODE)...")

if has_year and SITE_CODE_COL in df_in_site.columns:
    # Group by site and compute basic temporal stats
    group_site_year = df_in_site.groupby(SITE_CODE_COL, dropna=True)

    gaps_agg = {
        "site_name": (SITE_NAME_COL, "first"),
        "ms": (MS_COL, "first"),
        "site_type": (SITETYPE_COL, "first"),
        "year_min": (YEAR_COL, "min"),
        "year_max": (YEAR_COL, "max"),
        "n_years": (YEAR_COL, "nunique"),
        "n_occurrences": (TAXON_KEY_COL, "size"),
    }

    sites_gaps = group_site_year.agg(**gaps_agg).reset_index()

    # Compute expected years and gaps
    sites_gaps["expected_years"] = (
        sites_gaps["year_max"] - sites_gaps["year_min"] + 1
    )
    sites_gaps["missing_years"] = (
        sites_gaps["expected_years"] - sites_gaps["n_years"]
    )

    # Gap fraction: missing / expected (clip at [0,1])
    sites_gaps["gap_fraction"] = (
        sites_gaps["missing_years"] / sites_gaps["expected_years"]
    )
    sites_gaps["gap_fraction"] = sites_gaps["gap_fraction"].clip(lower=0, upper=1)

    # Cast year and count columns to Int64 for clean CSV
    sites_gaps = cast_int_columns(
        sites_gaps,
        ["year_min", "year_max", "n_years", "expected_years", "missing_years"]
    )

    log(f"Computed temporal gap metrics for {len(sites_gaps)} Natura2k sites.")
    sites_gaps.to_csv(SITES_GAPS_CSV, index=False)
    log(f"Site temporal gaps saved to: {rel(SITES_GAPS_CSV)}")
else:
    if not has_year:
        log("INFO: 'year' column not available – temporal gap metrics will not be produced.")
    else:
        log("WARNING: SITECODE column not found – temporal gap metrics will not be produced.")

log("\n=== Advanced metrics computation completed successfully ===")