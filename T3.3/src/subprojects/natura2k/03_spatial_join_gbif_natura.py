"""
Script: 03_spatial_join_gbif_natura.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Description:
Performs a spatial join between filtered GBIF occurrence points
and prepared Natura 2000 site polygons.

The script implements two analytical branches:

1) Baseline Spatial Join (point-in-polygon)
   Each GBIF occurrence is assigned to a Natura 2000 site
   if its coordinates fall within the site's polygon.

2) Uncertainty-Aware Spatial Classification
   Incorporates coordinateUncertaintyInMeters to evaluate
   positional uncertainty relative to Natura site boundaries.

   Each record is classified into one of four categories:

   - Certainly Inside
   - Inside – Positional Uncertainty
   - Outside – Positional Uncertainty
   - Certainly Outside

   Classification is based on:
   - Baseline within status (point-in-polygon result)
   - Distance to Natura boundary (for inside points)
   - Distance to nearest Natura polygon (for outside points)
   - Coordinate uncertainty radius (meters)

   Distance calculations are performed in a projected
   metric CRS (ETRS89 / LAEA Europe, EPSG:3035).

Input:
- Filtered GBIF dataset (ZIP containing CSV) from:
  data/filtered/<DATASET_NAME>/GBIF_<DATASET_NAME>_filtered_occurrences.zip

- Prepared Natura 2000 sites layer (WGS84) from:
  data/external/natura2k/Natura2000_sites_prepared.gpkg

Outputs:

1) Baseline enriched dataset:
   data/processed/<DATASET_NAME>/
       GBIF_<DATASET_NAME>_with_natura_sites.gpkg

2) Uncertainty-enriched dataset (baseline + uncertainty fields):
   data/processed/<DATASET_NAME>/
       GBIF_<DATASET_NAME>_with_natura_sites_uncertainty.gpkg

3) Uncertainty summary table:
   results/natura2k/<DATASET_NAME>/
       GBIF_<DATASET_NAME>_uncertainty_summary.csv

4) Uncertainty mapping table (record-level diagnostics):
   results/natura2k/<DATASET_NAME>/
       GBIF_<DATASET_NAME>_uncertainty_mapping.csv

Notes:
- gbifID is preserved and used as the stable primary key for merging
  uncertainty results back to the baseline spatial layer.
- The script is dataset-agnostic. By switching DATASET_NAME,
  the same logic applies to BIRDS / HABITATS / IAS.
- Baseline and uncertainty outputs are stored separately and
  do not overwrite each other.
- The implementation supports two complementary workflows:
  (a) Classical spatial attribution (point-in-polygon)
  (b) Positional uncertainty assessment for bias analysis.
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

# Dataset to use: "BIRDS", "HABITATS" or "IAS"
DATASET_NAME = "HABITATS"  # Change as needed

PROCESSED_DIR = DATA_DIR / "processed" / DATASET_NAME
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

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
# Output 1: baseline (same as original 03)
OUTPUT_GPKG_BASELINE = PROCESSED_DIR / f"GBIF_{DATASET_NAME}_with_natura_sites.gpkg"
# Output 2: uncertainty-aware
OUTPUT_GPKG_UNCERT = PROCESSED_DIR / f"GBIF_{DATASET_NAME}_with_natura_sites_uncertainty.gpkg"


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
log(f"Baseline Output GPKG: {rel(OUTPUT_GPKG_BASELINE)}")
log(f"Uncert. Output GPKG:  {rel(OUTPUT_GPKG_UNCERT)}")

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
        df_points = pd.read_csv(
            f,
            dtype={"gbifID": "string", "coordinateUncertaintyInMeters": "float64"},
            low_memory=False
        )
        # Add stable record identifier for downstream uncertainty mapping
        if "gbifID" not in df_points.columns:
            raise KeyError("gbifID column not found in filtered dataset.")

        # ensure gbifID is treated as string (safe for large values)
        df_points["gbifID"] = df_points["gbifID"].astype(str)

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
COUNTRY_COL   = "MS" #COUNTRY"
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


# --- Create slim dataframe for uncertainty logic (RAM-safe) ---
UNC_COL = "coordinateUncertaintyInMeters"
gdf_slim = gdf_joined[["gbifID", "geometry", "index_right", UNC_COL]].copy()





gdf_joined_baseline = gdf_joined.copy()
for col in ["baseline_within", "uncertainty_m", "dist_to_boundary_m", "dist_to_polygon_m", "uncertainty_class"]:
    if col in gdf_joined_baseline.columns:
        gdf_joined_baseline = gdf_joined_baseline.drop(columns=[col])

# Save BASELINE immediately (so we don't lose it if uncertainty step runs out of RAM)
log(f"\nSaving BASELINE enriched occurrences to: {rel(OUTPUT_GPKG_BASELINE)}")
gdf_joined_baseline.to_file(
    OUTPUT_GPKG_BASELINE,
    layer=f"gbif_{DATASET_NAME.lower()}_with_natura",
    driver="GPKG"
)


# free memory from earlier steps
import gc
del df_points, gdf_points, gdf_joined_baseline
gc.collect()
log("[03] Baseline output saved and intermediate memory released.")

# How many occurrences matched a Natura site?
if SITE_CODE_COL in gdf_joined.columns:
    n_matched = gdf_joined[SITE_CODE_COL].notna().sum()
    match_ratio = n_matched / len(gdf_joined) * 100 if len(gdf_joined) > 0 else 0.0
    log(f"Occurrences inside Natura sites: {n_matched} ({match_ratio:.2f}%)")
else:
    log("No site code column found after join; please check Natura column names.")

# --- drop the heavy joined df to free RAM; we keep only slim for uncertainty ---
del gdf_joined
gc.collect()
log("[03] Dropped full gdf_joined from memory (keeping slim only).")

# ----------------------------------------------------------------------
# Uncertainty-aware classification (RAM-safe using slim GeoDataFrame)
# ----------------------------------------------------------------------
import numpy as np
import gc

# Required columns
UNC_COL = "coordinateUncertaintyInMeters"
if UNC_COL not in gdf_slim.columns:
    raise KeyError(f"Expected uncertainty column '{UNC_COL}' not found in slim data.")
if "index_right" not in gdf_slim.columns:
    raise KeyError("Expected 'index_right' column is missing in slim data.")

# 1) Create a slim GeoDataFrame (only what we need for uncertainty computations)
gdf_slim["baseline_within"] = gdf_slim["index_right"].notna()
gdf_slim["uncertainty_m"] = pd.to_numeric(gdf_slim[UNC_COL], errors="coerce")

# Prepare distance columns
gdf_slim["dist_to_boundary_m"] = np.nan
gdf_slim["dist_to_polygon_m"] = np.nan

inside_mask = gdf_slim["baseline_within"]
outside_mask = ~inside_mask

# 2) Project Natura once (geometry only) to metric CRS
METRIC_EPSG = 3035
gdf_natura_m = gdf_natura_join[["geometry"]].to_crs(epsg=METRIC_EPSG)

# Chunk settings (tune if needed)
CHUNK_SIZE_INSIDE = 150_000
CHUNK_SIZE_OUTSIDE = 100_000

# Helper: chunk iterator
def _chunks(idx, size):
    for i in range(0, len(idx), size):
        yield idx[i:i+size]

# 3) INSIDE: distance to boundary of matched polygon (chunked)
inside_idx = gdf_slim.index[inside_mask].to_list()
if len(inside_idx) > 0:
    log(f"[03] Computing INSIDE boundary distances in chunks (n={len(inside_idx)})...")
    for k, idx_chunk in enumerate(_chunks(inside_idx, CHUNK_SIZE_INSIDE), start=1):
        slim_chunk = gdf_slim.loc[idx_chunk, ["geometry", "index_right"]].copy()
        slim_chunk_m = slim_chunk.to_crs(epsg=METRIC_EPSG)

        matched_poly = slim_chunk["index_right"].map(gdf_natura_m.geometry)
        matched_poly = gpd.GeoSeries(matched_poly, index=slim_chunk.index, crs=gdf_natura_m.crs)

        dist_inside = slim_chunk_m.geometry.distance(matched_poly.boundary)
        gdf_slim.loc[idx_chunk, "dist_to_boundary_m"] = dist_inside.values

        if k % 5 == 0:
            log(f"  - INSIDE chunks processed: {k} (last chunk size={len(idx_chunk)})")

# 4) OUTSIDE: distance to nearest Natura polygon (chunked)
outside_idx = gdf_slim.index[outside_mask].to_list()
if len(outside_idx) > 0:
    log(f"[03] Computing OUTSIDE nearest distances in chunks (n={len(outside_idx)})...")
    natura_geom_only = gdf_natura_m[["geometry"]].copy()

    for k, idx_chunk in enumerate(_chunks(outside_idx, CHUNK_SIZE_OUTSIDE), start=1):
        slim_chunk = gdf_slim.loc[idx_chunk, ["geometry"]].copy()
        slim_chunk_m = slim_chunk.to_crs(epsg=METRIC_EPSG)

        nearest = gpd.sjoin_nearest(
        slim_chunk_m,
        natura_geom_only,
        how="left",
        distance_col="dist_to_polygon_m"
        )
        if nearest.index.duplicated().any():
            log("  - NOTE: ties detected in sjoin_nearest (multiple nearest polygons for some points). Using min distance.")

        # If multiple nearest polygons exist (ties), keep the minimum distance per point
        nearest_dist = nearest.groupby(level=0)["dist_to_polygon_m"].min()

        # Assign by index alignment (robust even with ties)
        gdf_slim.loc[nearest_dist.index, "dist_to_polygon_m"] = nearest_dist.values

        if k % 5 == 0:
            log(f"  - OUTSIDE chunks processed: {k} (last chunk size={len(idx_chunk)})")

# 5) Classification into 4 categories
u = gdf_slim["uncertainty_m"]

cond_certain_inside = inside_mask & (gdf_slim["dist_to_boundary_m"] > u)
cond_inside_unc     = inside_mask & (gdf_slim["dist_to_boundary_m"] <= u)

cond_outside_unc    = outside_mask & (gdf_slim["dist_to_polygon_m"] <= u)
cond_certain_out    = outside_mask & (gdf_slim["dist_to_polygon_m"] > u)

gdf_slim["uncertainty_class"] = np.select(
    [cond_certain_inside, cond_inside_unc, cond_outside_unc, cond_certain_out],
    ["Certainly Inside", "Inside – Positional Uncertainty", "Outside – Positional Uncertainty", "Certainly Outside"],
    default="Unclassified"
)

# 6) Copy results back to the full joined dataframe (only new, light columns)
#gdf_joined["baseline_within"] = gdf_slim["baseline_within"].values
#gdf_joined["uncertainty_m"] = gdf_slim["uncertainty_m"].values
#gdf_joined["dist_to_boundary_m"] = gdf_slim["dist_to_boundary_m"].values
#gdf_joined["dist_to_polygon_m"] = gdf_slim["dist_to_polygon_m"].values
#gdf_joined["uncertainty_class"] = gdf_slim["uncertainty_class"].values

# Free memory
for _v in ["gdf_slim_m", "gdf_natura_m", "outside_nearest", "outside_points_m", "natura_geom_only", "matched_poly"]:
    if _v in locals():
        del locals()[_v]
gc.collect()

# 7) Summary CSV in results/ (for presentation)
RESULTS_DIR = PROJECT_ROOT / "results" / "natura2k" / DATASET_NAME
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

SUMMARY_CSV = RESULTS_DIR / f"GBIF_{DATASET_NAME}_uncertainty_summary.csv"

summary = (
    gdf_slim["uncertainty_class"]
    .value_counts(dropna=False)
    .rename_axis("Category")
    .reset_index(name="Count")
)
summary["Percentage"] = (summary["Count"] / len(gdf_slim) * 100).round(2)

baseline_inside = int(gdf_slim["baseline_within"].sum())
baseline_outside = int((~gdf_slim["baseline_within"]).sum())

extra = pd.DataFrame([
    {"Category": "Baseline within (point-in-polygon)", "Count": baseline_inside, "Percentage": round(baseline_inside / len(gdf_slim) * 100, 2)},
    {"Category": "Baseline outside (point-in-polygon)", "Count": baseline_outside, "Percentage": round(baseline_outside / len(gdf_slim) * 100, 2)},
    ])

summary_out = pd.concat([extra, summary], ignore_index=True)
summary_out.to_csv(SUMMARY_CSV, index=False)

log(f"\n[03] Uncertainty summary saved to: {rel(SUMMARY_CSV)}")

MAPPING_CSV = RESULTS_DIR / f"GBIF_{DATASET_NAME}_uncertainty_mapping.csv"
out_map = gdf_slim[[
    "gbifID",
    "uncertainty_m",
    "dist_to_boundary_m",
    "dist_to_polygon_m",
    "uncertainty_class"
]].copy()
out_map.to_csv(MAPPING_CSV, index=False)
log(f"[03] Uncertainty mapping saved to: {rel(MAPPING_CSV)}")

# ----------------------------------------------------------------------
# (0) Build uncertainty-enriched GPKG (RAM-safe)
#     We reload the baseline GPKG and merge uncertainty fields using gbifID
# ----------------------------------------------------------------------

# 0a) Preconditions: gbifID must exist in slim
if "gbifID" not in gdf_slim.columns:
    raise KeyError(
        "gbifID not found in gdf_slim. "
        "Ensure 02_filter_gbif_dataset.py keeps gbifID and 03 uses gbifID (not record_id)."
    )

# 0b) Baseline output must exist (it was saved earlier in this script)
if not OUTPUT_GPKG_BASELINE.exists():
    raise FileNotFoundError(f"Baseline GPKG not found: {OUTPUT_GPKG_BASELINE}")

# 1) Prepare a compact uncertainty mapping table (no geometry needed here)
unc_map = gdf_slim[[
    "gbifID",
    "baseline_within",
    "uncertainty_m",
    "dist_to_boundary_m",
    "dist_to_polygon_m",
    "uncertainty_class"
]].copy()

# Ensure consistent dtype for join key
unc_map["gbifID"] = unc_map["gbifID"].astype(str)

# 2) Reload baseline GPKG (RAM-safe)
baseline_layer = f"gbif_{DATASET_NAME.lower()}_with_natura"
gdf_base = gpd.read_file(OUTPUT_GPKG_BASELINE, layer=baseline_layer)

if "gbifID" not in gdf_base.columns:
    raise KeyError(
        "gbifID not found in baseline GPKG. "
        "Fix 02_filter_gbif_dataset.py to keep gbifID, then rerun 02 -> 03."
    )

gdf_base["gbifID"] = gdf_base["gbifID"].astype(str)

# 3) Merge uncertainty fields onto baseline (left join keeps all baseline records)
gdf_unc = gdf_base.merge(unc_map, on="gbifID", how="left")

# Optional: coverage check (useful for logs)
mapped = gdf_unc["uncertainty_class"].notna().sum()
log(f"[03] Uncertainty fields mapped to baseline: {mapped}/{len(gdf_unc)} ({mapped/len(gdf_unc)*100:.2f}%)")

# 4) Save uncertainty-enriched GPKG
unc_layer = f"gbif_{DATASET_NAME.lower()}_with_natura_uncertainty"
log(f"\nSaving UNCERTAINTY enriched occurrences to: {rel(OUTPUT_GPKG_UNCERT)}")

gdf_unc.to_file(
    OUTPUT_GPKG_UNCERT,
    layer=unc_layer,
    driver="GPKG"
)

log("[03] Uncertainty GPKG saved successfully (baseline + uncertainty columns).")


log("=== Spatial join GBIF ↔ Natura completed (baseline + uncertainty summary/mapping) ===")