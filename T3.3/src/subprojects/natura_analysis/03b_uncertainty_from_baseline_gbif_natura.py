"""
Script: natura_analysis/03b_uncertainty_from_baseline_gbif_natura.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Description:
Computes the uncertainty-aware branch of the GBIF–Natura workflow
starting from an already generated baseline spatial join dataset.

This auxiliary script is intended for cases where the baseline
point-in-polygon output has been produced successfully, but the
uncertainty-aware classification step is too memory-intensive when
executed inside the full 03 workflow.

The script:
- loads the baseline GBIF–Natura GeoPackage
- loads the prepared Natura 2000 polygon layer
- computes distance-to-boundary for inside points
- computes distance-to-nearest-polygon for outside points
- classifies each record into one of four uncertainty classes
- saves:
  1) uncertainty-enriched GeoPackage
  2) uncertainty summary CSV
  3) uncertainty mapping CSV

Input:
- Baseline GBIF–Natura dataset:
  data/processed/<DATASET_NAME>/GBIF_<DATASET_NAME>_natura_spatial_baseline.gpkg

- Prepared Natura 2000 sites:
  data/external/natura2k/Natura2000_sites_prepared.gpkg

Outputs:
- Uncertainty-enriched GeoPackage:
  data/processed/<DATASET_NAME>/GBIF_<DATASET_NAME>_natura_spatial_uncertainty.gpkg

- Uncertainty summary:
  results/natura_analysis/<DATASET_NAME>/GBIF_<DATASET_NAME>_uncertainty_summary.csv

- Uncertainty mapping:
  results/natura_analysis/<DATASET_NAME>/GBIF_<DATASET_NAME>_uncertainty_mapping.csv

Notes:
- This script assumes that the baseline dataset already contains:
  gbifID, geometry, index_right, and coordinateUncertaintyInMeters.
- The inside-distance computation is optimized by processing records
  grouped by matched Natura polygon, reducing repeated boundary-object
  construction and lowering memory pressure.
"""

from pathlib import Path
from datetime import datetime
import logging
import gc

import geopandas as gpd
import pandas as pd
import numpy as np

# ----------------------------------------------------------------------
# Project-level paths
# ----------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]

DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"
EXTERNAL_DIR = DATA_DIR / "external"
NATURA_DIR = EXTERNAL_DIR / "natura2k"

# ----------------------------------------------------------------------
# User configuration
# ----------------------------------------------------------------------
DATASET_NAME = "HABITATS"   # "IAS", "BIRDS", "HABITATS"

BASELINE_GPKG = (
    PROCESSED_DIR
    / DATASET_NAME 
    / "natura_analysis"
    / f"GBIF_{DATASET_NAME}_natura_spatial_baseline.gpkg"
)

UNCERT_GPKG = (
    PROCESSED_DIR
    / DATASET_NAME
    / f"GBIF_{DATASET_NAME}_natura_spatial_uncertainty.gpkg"
)

BASELINE_LAYER = f"gbif_{DATASET_NAME.lower()}_with_natura"
UNCERT_LAYER = f"gbif_{DATASET_NAME.lower()}_with_natura_uncertainty"

NATURA_GPKG = NATURA_DIR / "Natura2000_sites_prepared.gpkg"
NATURA_LAYER = "natura_sites_epsg4326"

RESULTS_DIR = PROJECT_ROOT / "results" / "natura_analysis" / DATASET_NAME
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

SUMMARY_CSV = RESULTS_DIR / f"GBIF_{DATASET_NAME}_uncertainty_summary.csv"
MAPPING_CSV = RESULTS_DIR / f"GBIF_{DATASET_NAME}_uncertainty_mapping.csv"

# Metric CRS for distance calculations
METRIC_EPSG = 3035

# Memory / chunk controls
INSIDE_POINT_CHUNK_SIZE = 10_000
OUTSIDE_CHUNK_SIZE = 50_000

# ----------------------------------------------------------------------
# Logging setup
# ----------------------------------------------------------------------
LOG_DIR = PROJECT_ROOT / "logs" / "natura_analysis" / "03b_uncertainty_from_baseline_gbif_natura"
LOG_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M")
logfile = LOG_DIR / f"uncertainty_from_baseline_{DATASET_NAME}_{timestamp}.log"

logger = logging.getLogger(f"uncertainty_from_baseline_{DATASET_NAME}")
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

file_handler = logging.FileHandler(logfile, mode="a", encoding="utf-8")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)


def log(msg: str) -> None:
    print(msg)
    logger.info(msg)


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except Exception:
        return str(path)


def chunk_list(values, size):
    for i in range(0, len(values), size):
        yield values[i:i + size]


# ----------------------------------------------------------------------
# Sanity checks
# ----------------------------------------------------------------------
log("=== Uncertainty-from-baseline computation started ===")
log(f"Dataset:              {DATASET_NAME}")
log(f"Baseline input GPKG:  {rel(BASELINE_GPKG)}")
log(f"Natura input GPKG:    {rel(NATURA_GPKG)}")
log(f"Uncertainty output:   {rel(UNCERT_GPKG)}")
log(f"Summary CSV:          {rel(SUMMARY_CSV)}")
log(f"Mapping CSV:          {rel(MAPPING_CSV)}")

if not BASELINE_GPKG.exists():
    raise FileNotFoundError(f"Baseline GPKG not found: {BASELINE_GPKG}")

if not NATURA_GPKG.exists():
    raise FileNotFoundError(f"Natura GPKG not found: {NATURA_GPKG}")

# ----------------------------------------------------------------------
# Load baseline data
# ----------------------------------------------------------------------
log("\nLoading baseline GBIF–Natura dataset...")
gdf_base = gpd.read_file(BASELINE_GPKG, layer=BASELINE_LAYER)

log(f"Loaded baseline table with {len(gdf_base)} records.")
log(f"Baseline CRS: {gdf_base.crs}")
log(f"Baseline columns: {', '.join(gdf_base.columns.astype(str))}")

required_cols = ["gbifID", "geometry", "index_right", "coordinateUncertaintyInMeters"]
missing = [c for c in required_cols if c not in gdf_base.columns]
if missing:
    raise KeyError(f"Missing required columns in baseline dataset: {missing}")

if gdf_base.crs is None:
    raise ValueError("Baseline dataset has no CRS defined.")

if gdf_base.crs.to_epsg() != 4326:
    log("Reprojecting baseline dataset to EPSG:4326...")
    gdf_base = gdf_base.to_crs(epsg=4326)

# ----------------------------------------------------------------------
# Load Natura polygons
# ----------------------------------------------------------------------
log("\nLoading prepared Natura 2000 sites...")
gdf_natura = gpd.read_file(NATURA_GPKG, layer=NATURA_LAYER)

log(f"Loaded Natura sites: {len(gdf_natura)} polygons.")
log(f"Natura CRS: {gdf_natura.crs}")

if gdf_natura.crs is None:
    raise ValueError("Natura layer has no CRS defined.")

if gdf_natura.crs.to_epsg() != 4326:
    log("Reprojecting Natura layer to EPSG:4326...")
    gdf_natura = gdf_natura.to_crs(epsg=4326)

# Project Natura once to metric CRS
log("\nProjecting Natura geometries to metric CRS (EPSG:3035)...")
gdf_natura_m = gdf_natura[["geometry"]].to_crs(epsg=METRIC_EPSG)

# ----------------------------------------------------------------------
# Prepare slim working table
# ----------------------------------------------------------------------
log("\nPreparing slim working dataset...")
gdf_slim = gdf_base[["gbifID", "geometry", "index_right", "coordinateUncertaintyInMeters"]].copy()

gdf_slim["baseline_within"] = gdf_slim["index_right"].notna()
gdf_slim["uncertainty_m"] = pd.to_numeric(
    gdf_slim["coordinateUncertaintyInMeters"], errors="coerce"
)

gdf_slim["dist_to_boundary_m"] = np.nan
gdf_slim["dist_to_polygon_m"] = np.nan

inside_mask = gdf_slim["baseline_within"]
outside_mask = ~inside_mask

n_inside = int(inside_mask.sum())
n_outside = int(outside_mask.sum())

log(f"Inside records:  {n_inside}")
log(f"Outside records: {n_outside}")

# ----------------------------------------------------------------------
# 1) INSIDE: distance to matched polygon boundary
#    Optimized: process by matched Natura polygon
# ----------------------------------------------------------------------
log("\nComputing INSIDE boundary distances (grouped by matched Natura polygon)...")

inside_df = gdf_slim.loc[inside_mask, ["geometry", "index_right"]].copy()
inside_df["index_right"] = inside_df["index_right"].astype("Int64")

unique_polygons = inside_df["index_right"].dropna().astype(int).unique().tolist()
log(f"Unique matched Natura polygons among inside records: {len(unique_polygons)}")

for j, poly_idx in enumerate(unique_polygons, start=1):
    idx_for_poly = inside_df.index[inside_df["index_right"] == poly_idx].tolist()

    if poly_idx not in gdf_natura_m.index:
        log(f"WARNING: Polygon index {poly_idx} not found in Natura metric layer. Skipping.")
        continue

    poly_geom = gdf_natura_m.geometry.loc[poly_idx]
    poly_boundary = poly_geom.boundary

    for idx_chunk in chunk_list(idx_for_poly, INSIDE_POINT_CHUNK_SIZE):
        slim_chunk = gdf_slim.loc[idx_chunk, ["geometry"]].copy()
        slim_chunk_m = slim_chunk.to_crs(epsg=METRIC_EPSG)

        dist_inside = slim_chunk_m.geometry.distance(poly_boundary)
        gdf_slim.loc[idx_chunk, "dist_to_boundary_m"] = dist_inside.values

        del slim_chunk, slim_chunk_m, dist_inside
        gc.collect()

    if j % 250 == 0:
        log(f"  - Natura polygons processed: {j} / {len(unique_polygons)}")

del inside_df
gc.collect()
log("INSIDE boundary-distance computation completed.")

# ----------------------------------------------------------------------
# 2) OUTSIDE: distance to nearest Natura polygon
# ----------------------------------------------------------------------
log("\nComputing OUTSIDE nearest distances in chunks...")

outside_idx = gdf_slim.index[outside_mask].tolist()
natura_geom_only = gdf_natura_m[["geometry"]].copy()

for k, idx_chunk in enumerate(chunk_list(outside_idx, OUTSIDE_CHUNK_SIZE), start=1):
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

    nearest_dist = nearest.groupby(level=0)["dist_to_polygon_m"].min()
    gdf_slim.loc[nearest_dist.index, "dist_to_polygon_m"] = nearest_dist.values

    del slim_chunk, slim_chunk_m, nearest, nearest_dist
    gc.collect()

    if k % 5 == 0:
        log(f"  - OUTSIDE chunks processed: {k} (last chunk size={len(idx_chunk)})")

del natura_geom_only, gdf_natura_m
gc.collect()
log("OUTSIDE nearest-distance computation completed.")

# ----------------------------------------------------------------------
# 3) Uncertainty classification
# ----------------------------------------------------------------------
log("\nClassifying records into uncertainty classes...")

u = gdf_slim["uncertainty_m"]

cond_certain_inside = inside_mask & (gdf_slim["dist_to_boundary_m"] > u)
cond_inside_unc = inside_mask & (gdf_slim["dist_to_boundary_m"] <= u)

cond_outside_unc = outside_mask & (gdf_slim["dist_to_polygon_m"] <= u)
cond_certain_out = outside_mask & (gdf_slim["dist_to_polygon_m"] > u)

gdf_slim["uncertainty_class"] = np.select(
    [cond_certain_inside, cond_inside_unc, cond_outside_unc, cond_certain_out],
    [
        "Certainly Inside",
        "Inside – Positional Uncertainty",
        "Outside – Positional Uncertainty",
        "Certainly Outside",
    ],
    default="Unclassified"
)

# ----------------------------------------------------------------------
# 4) Save summary CSV
# ----------------------------------------------------------------------
log("\nSaving uncertainty summary...")
summary = (
    gdf_slim["uncertainty_class"]
    .value_counts(dropna=False)
    .rename_axis("Category")
    .reset_index(name="Count")
)
summary["Percentage"] = (summary["Count"] / len(gdf_slim) * 100).round(2)

extra = pd.DataFrame([
    {
        "Category": "Baseline within (point-in-polygon)",
        "Count": int(gdf_slim["baseline_within"].sum()),
        "Percentage": round(gdf_slim["baseline_within"].sum() / len(gdf_slim) * 100, 2),
    },
    {
        "Category": "Baseline outside (point-in-polygon)",
        "Count": int((~gdf_slim["baseline_within"]).sum()),
        "Percentage": round((~gdf_slim["baseline_within"]).sum() / len(gdf_slim) * 100, 2),
    },
])

summary_out = pd.concat([extra, summary], ignore_index=True)
summary_out.to_csv(SUMMARY_CSV, index=False)
log(f"Uncertainty summary saved to: {rel(SUMMARY_CSV)}")

# ----------------------------------------------------------------------
# 5) Save mapping CSV
# ----------------------------------------------------------------------
log("\nSaving uncertainty mapping...")
mapping = gdf_slim[
    [
        "gbifID",
        "uncertainty_m",
        "dist_to_boundary_m",
        "dist_to_polygon_m",
        "uncertainty_class",
    ]
].copy()

mapping.to_csv(MAPPING_CSV, index=False)
log(f"Uncertainty mapping saved to: {rel(MAPPING_CSV)}")

# ----------------------------------------------------------------------
# 6) Build uncertainty-enriched GPKG
#    IMPORTANT: assign by row index, not gbifID merge
# ----------------------------------------------------------------------
log("\nBuilding uncertainty-enriched GeoPackage...")

gdf_unc = gdf_base.copy()
gdf_unc["baseline_within"] = gdf_slim["baseline_within"].values
gdf_unc["uncertainty_m"] = gdf_slim["uncertainty_m"].values
gdf_unc["dist_to_boundary_m"] = gdf_slim["dist_to_boundary_m"].values
gdf_unc["dist_to_polygon_m"] = gdf_slim["dist_to_polygon_m"].values
gdf_unc["uncertainty_class"] = gdf_slim["uncertainty_class"].values

mapped = gdf_unc["uncertainty_class"].notna().sum()
log(f"Uncertainty fields assigned to baseline rows: {mapped}/{len(gdf_unc)} ({mapped / len(gdf_unc) * 100:.2f}%)")

log(f"\nSaving uncertainty-enriched occurrences to: {rel(UNCERT_GPKG)}")
gdf_unc.to_file(
    UNCERT_GPKG,
    layer=UNCERT_LAYER,
    driver="GPKG"
)

log("Uncertainty GPKG saved successfully.")
log("=== Uncertainty-from-baseline computation completed successfully ===")