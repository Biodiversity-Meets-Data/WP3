"""
Script: 03_spatial_join_gbif_natura.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Description:
Performs a spatial join between filtered GBIF occurrence points
and prepared Natura 2000 site polygons (in WGS84).
Each GBIF record is assigned to a Natura site if its coordinates
fall inside the site's polygon.

Input:
- Filtered GBIF dataset (CSV) from:
  data/filtered/GBIF_<DATASET_NAME>_filtered_occurrences.csv
- Prepared Natura sites layer (WGS84) from:
  data/external/natura2k/Natura2000_sites_prepared.gpkg

Output:
- Enriched occurrence dataset with Natura attributes:
  data/processed/GBIF_<DATASET_NAME>_with_natura_sites.gpkg

Notes:
This script is dataset-agnostic. By switching DATASET_NAME,
the same logic can be applied to BIRDS / HABITATS / IAS.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd
import logging
from datetime import datetime
import zipfile

# ----------------------------------------------------------------------
# Project-level paths
# ----------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]  # .../BMD Implementation

DATA_DIR = PROJECT_ROOT / "data"
FILTERED_DIR = DATA_DIR / "filtered"
EXTERNAL_DIR = DATA_DIR / "external"
NATURA_DIR = EXTERNAL_DIR / "natura2k"
PROCESSED_DIR = DATA_DIR / "processed"

# Helper: relative paths for logging
def rel(path: Path) -> str:
    """
    Return path as string relative to the project root,
    so that absolute system paths do not appear in logs.
    """
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except Exception:
        return str(path)

# ----------------------------------------------------------------------
# User configuration
# ----------------------------------------------------------------------
# Dataset to use: "BIRDS", "HABITATS" or "IAS"
DATASET_NAME = "IAS"  # Change as needed

# each dataset own subfolder inside filtered/
FILTERED_SUBDIR = FILTERED_DIR / DATASET_NAME

# ZIP file is located inside subfolder (π.χ. data/filtered/BIRDS/GBIF_BIRDS_filtered_occurrences.zip)
FILTERED_ZIP = FILTERED_SUBDIR / f"GBIF_{DATASET_NAME}_filtered_occurrences.zip"

# Inner CSV name inside the ZIP (same base name, .csv)
INNER_CSV = f"GBIF_{DATASET_NAME}_filtered_occurrences.csv"

# Prepared Natura sites (output of 02_prepare_natura_sites.py)
NATURA_GPKG = NATURA_DIR / "Natura2000_sites_prepared.gpkg"
NATURA_LAYER = "natura_sites_epsg4326"  # layer name used in 02_prepare_natura_sites

# Output: enriched GBIF occurrences with Natura attributes
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_GPKG = PROCESSED_DIR / f"GBIF_{DATASET_NAME}_with_natura_sites.gpkg"

# ----------------------------------------------------------------------
# Logging setup
# ----------------------------------------------------------------------
LOG_DIR = PROJECT_ROOT / "logs" / "natura2k" / "03_spatial_join_gbif_natura"
LOG_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M")
logfile = LOG_DIR / f"spatial_join_{DATASET_NAME}_{timestamp}.log"

logger = logging.getLogger(f"spatial_join_{DATASET_NAME}")
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

file_handler = logging.FileHandler(logfile, mode="a", encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

def log(msg: str):
    print(msg)
    logger.info(msg)

log("=== Spatial join GBIF ↔ Natura started ===")
log(f"Dataset:              {DATASET_NAME}")
log(f"Filtered GBIF ZIP:    {rel(FILTERED_ZIP)}")
log(f"Natura GPKG:          {rel(NATURA_GPKG)}")
log(f"Output GPKG:          {rel(OUTPUT_GPKG)}")

# ----------------------------------------------------------------------
# Sanity checks
# ----------------------------------------------------------------------
if not FILTERED_ZIP.exists():
    raise FileNotFoundError(f"Filtered GBIF CSV not found: {FILTERED_ZIP}")

if not NATURA_GPKG.exists():
    raise FileNotFoundError(f"Natura GPKG not found: {NATURA_GPKG}")

# ----------------------------------------------------------------------
# Load GBIF filtered occurrences (points) from ZIP
# ----------------------------------------------------------------------
log("\nLoading filtered GBIF occurrences from ZIP...")

with zipfile.ZipFile(str(FILTERED_ZIP), "r") as z:
    if INNER_CSV not in z.namelist():
        raise FileNotFoundError(
            f"Inner CSV '{INNER_CSV}' not found inside ZIP: {FILTERED_ZIP}"
        )
    with z.open(INNER_CSV) as f:
        df_points = pd.read_csv(f)

log(f"Loaded GBIF table with {len(df_points)} records.")
log(f"GBIF columns: {', '.join(df_points.columns.astype(str))}")


# Column names for coordinates
COL_LAT = "decimalLatitude"
COL_LON = "decimalLongitude"

if COL_LAT not in df_points.columns or COL_LON not in df_points.columns:
    raise KeyError(f"Expected columns {COL_LAT}, {COL_LON} not found in filtered GBIF CSV.")

# Convert DataFrame to GeoDataFrame with POINT geometry
log("Converting GBIF table to GeoDataFrame (POINTS)...")
gdf_points = gpd.GeoDataFrame(
    df_points,
    geometry=gpd.points_from_xy(df_points[COL_LON], df_points[COL_LAT]),
    crs="EPSG:4326"
)
log("GeoDataFrame with points created.")

# ----------------------------------------------------------------------
# Load prepared Natura sites (polygons)
# ----------------------------------------------------------------------
log("\nLoading prepared Natura 2000 sites...")
gdf_natura = gpd.read_file(NATURA_GPKG, layer=NATURA_LAYER)

log(f"Loaded Natura sites: {len(gdf_natura)} polygons.")
log(f"Natura CRS: {gdf_natura.crs}")
log(f"Natura columns: {', '.join(gdf_natura.columns.astype(str))}")

# Ensure Natura layer is also in EPSG:4326
if gdf_natura.crs is None:
    raise ValueError("Natura layer has no CRS defined.")

if gdf_natura.crs.to_epsg() != 4326:
    log("Reprojecting Natura sites to EPSG:4326...")
    gdf_natura = gdf_natura.to_crs(epsg=4326)
    log("Reprojection completed.")

# ----------------------------------------------------------------------
# Select Natura attributes to bring into the join
# ----------------------------------------------------------------------
# Adjust these column names based on your prepared layer
SITE_CODE_COL = "SITECODE"
SITE_NAME_COL = "SITENAME"
COUNTRY_COL   = "COUNTRY"
TYPE_COL      = "SITETYPE"

missing_natura_cols = [
    col for col in [SITE_CODE_COL, SITE_NAME_COL, COUNTRY_COL, TYPE_COL]
    if col not in gdf_natura.columns
]

if missing_natura_cols:
    log("WARNING: Some expected Natura columns are missing:")
    for c in missing_natura_cols:
        log(f"  - {c}")
    log("Proceeding with all available columns.")
    gdf_natura_join = gdf_natura.copy()
else:
    keep_cols = [SITE_CODE_COL, SITE_NAME_COL, COUNTRY_COL, TYPE_COL, "geometry"]
    gdf_natura_join = gdf_natura[keep_cols].copy()

# ----------------------------------------------------------------------
# Spatial join: points within Natura polygons
# ----------------------------------------------------------------------
log("\nRunning spatial join (points within Natura sites)...")
gdf_joined = gpd.sjoin(
    gdf_points,
    gdf_natura_join,
    how="left",
    predicate="within"
)

log(f"Spatial join completed. Joined table has {len(gdf_joined)} records.")

# How many occurrences matched a Natura site?
if SITE_CODE_COL in gdf_joined.columns:
    n_matched = gdf_joined[SITE_CODE_COL].notna().sum()
    match_ratio = n_matched / len(gdf_joined) * 100 if len(gdf_joined) > 0 else 0.0
    log(f"Occurrences inside Natura sites: {n_matched} ({match_ratio:.2f}%)")
else:
    log("No site code column found after join; please check Natura column names.")

# ----------------------------------------------------------------------
# Save enriched occurrences
# ----------------------------------------------------------------------
log(f"\nSaving enriched occurrences with Natura attributes to: {rel(OUTPUT_GPKG)}")
gdf_joined.to_file(
    OUTPUT_GPKG,
    layer=f"gbif_{DATASET_NAME.lower()}_with_natura",
    driver="GPKG"
)

log("=== Spatial join GBIF ↔ Natura completed ===")