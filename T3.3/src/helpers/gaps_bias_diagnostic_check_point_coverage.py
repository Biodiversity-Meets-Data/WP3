"""
Script: diagnostic_check_point_coverage.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Notes:
This diagnostic script checks the consistency between:
- selected occurrence records
- processed study area polygon
- H3 spatial units

Purpose:
To identify whether occurrence records are lost at the study area
selection step or at the H3 grid assignment step.

Input:
- Selected occurrences (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_occurrences_<STUDY_AREA_NAME>.gpkg

- Processed study area polygon (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  <STUDY_AREA_NAME>_study_area.gpkg

- H3 spatial units (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_grid_res<H3_RESOLUTION>.gpkg
"""

from pathlib import Path
import geopandas as gpd

# ----------------------------------------------------------------------
# Project-level paths
# ----------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"

# ----------------------------------------------------------------------
# User configuration
# ----------------------------------------------------------------------
DATASET_NAME = "IAS"
STUDY_AREA_NAME = "greece_natura"
H3_RESOLUTION = 6

INPUT_DIR = PROCESSED_DIR / DATASET_NAME / "gap_bias_surfaces" / STUDY_AREA_NAME

INPUT_OCCURRENCES_GPKG = (
    INPUT_DIR / f"GBIF_{DATASET_NAME}_occurrences_{STUDY_AREA_NAME}.gpkg"
)

INPUT_STUDY_AREA_GPKG = (
    INPUT_DIR / f"{STUDY_AREA_NAME}_study_area.gpkg"
)

INPUT_GRID_GPKG = (
    INPUT_DIR / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_grid_res{H3_RESOLUTION}.gpkg"
)

# ----------------------------------------------------------------------
# Load data
# ----------------------------------------------------------------------
occ_gdf = gpd.read_file(INPUT_OCCURRENCES_GPKG).to_crs(epsg=4326)
study_area_gdf = gpd.read_file(INPUT_STUDY_AREA_GPKG).to_crs(epsg=4326)
grid_gdf = gpd.read_file(INPUT_GRID_GPKG).to_crs(epsg=4326)

print(f"Selected occurrences: {len(occ_gdf)}")
print(f"Study area polygons: {len(study_area_gdf)}")
print(f"H3 cells: {len(grid_gdf)}")

# ----------------------------------------------------------------------
# Check 1: selected points vs study area
# ----------------------------------------------------------------------
join_area = gpd.sjoin(
    occ_gdf,
    study_area_gdf,
    how="inner",
    predicate="intersects"
).copy()

print("\nCheck 1: occurrences vs study area")
print(f"Occurrences intersecting study area: {len(join_area)}")

# ----------------------------------------------------------------------
# Check 2: selected points vs H3 grid
# ----------------------------------------------------------------------
join_grid = gpd.sjoin(
    occ_gdf,
    grid_gdf[["cell_id", "geometry"]],
    how="inner",
    predicate="intersects"
).copy()

print("\nCheck 2: occurrences vs H3 grid")
print(f"Occurrences intersecting H3 grid: {len(join_grid)}")

# ----------------------------------------------------------------------
# Compare missing points
# ----------------------------------------------------------------------
if "gbifID" in occ_gdf.columns:
    area_ids = set(join_area["gbifID"])
    grid_ids = set(join_grid["gbifID"])
    all_ids = set(occ_gdf["gbifID"])

    missing_from_area = all_ids - area_ids
    missing_from_grid = all_ids - grid_ids

    print("\nMissing records summary")
    print(f"Missing from study area join: {len(missing_from_area)}")
    print(f"Missing from H3 grid join: {len(missing_from_grid)}")
else:
    print("\nColumn 'gbifID' not found. Missing-record comparison skipped.")