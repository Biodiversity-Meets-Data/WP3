"""
Script: diagnostic_checks.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Notes:
This script performs validation checks for the gap_bias_surfaces pipeline.

It verifies consistency between:
- selected occurrence records
- processed study area polygon
- H3 spatial units
- aggregated H3 outputs

The checks are intended to confirm that the pipeline outputs are
internally consistent and suitable for downstream analyses.

Purpose:
To provide a reproducible validation layer for the spatial pipeline
and help detect broken assumptions after code changes or reruns.

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

- Aggregated H3 spatial units (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_aggregated_res<H3_RESOLUTION>.gpkg

Output:
- Validation summary text file:
  results/gap_bias_surfaces/<DATASET_NAME>/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_pipeline_validation_res<H3_RESOLUTION>.txt
"""

from pathlib import Path

import geopandas as gpd

# ----------------------------------------------------------------------
# Project-level paths
# ----------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]  # if script is in src/helpers/

DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"
RESULTS_DIR = PROJECT_ROOT / "results"

# ----------------------------------------------------------------------
# User configuration
# ----------------------------------------------------------------------
DATASET_NAME = "IAS"  # BIRDS / HABITATS / IAS
STUDY_AREA_NAME = "greece_natura"
H3_RESOLUTION = 6

# ----------------------------------------------------------------------
# Input paths
# ----------------------------------------------------------------------
INPUT_DIR = PROCESSED_DIR / DATASET_NAME / "gap_bias_surfaces" / STUDY_AREA_NAME

OCC_FILE = INPUT_DIR / f"GBIF_{DATASET_NAME}_occurrences_{STUDY_AREA_NAME}.gpkg"
AREA_FILE = INPUT_DIR / f"{STUDY_AREA_NAME}_study_area.gpkg"
GRID_FILE = INPUT_DIR / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_grid_res{H3_RESOLUTION}.gpkg"
AGG_FILE = INPUT_DIR / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_aggregated_res{H3_RESOLUTION}.gpkg"

# ----------------------------------------------------------------------
# Output path
# ----------------------------------------------------------------------
RESULTS_SUBDIR = RESULTS_DIR / "gap_bias_surfaces" / DATASET_NAME / STUDY_AREA_NAME
RESULTS_SUBDIR.mkdir(parents=True, exist_ok=True)

SUMMARY_FILE = (
    RESULTS_SUBDIR
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_pipeline_validation_res{H3_RESOLUTION}.txt"
)

# ----------------------------------------------------------------------
# Load data
# ----------------------------------------------------------------------
for f in [OCC_FILE, AREA_FILE, GRID_FILE, AGG_FILE]:
    if not f.exists():
        raise FileNotFoundError(f"Required file not found: {f}")

occ_gdf = gpd.read_file(OCC_FILE).to_crs(epsg=4326)
area_gdf = gpd.read_file(AREA_FILE).to_crs(epsg=4326)
grid_gdf = gpd.read_file(GRID_FILE).to_crs(epsg=4326)
agg_gdf = gpd.read_file(AGG_FILE).to_crs(epsg=4326)

# ----------------------------------------------------------------------
# Basic checks
# ----------------------------------------------------------------------
checks = []

def add_check(name: str, passed: bool, details: str):
    checks.append((name, passed, details))
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {name}: {details}")

add_check("Occurrences non-empty", not occ_gdf.empty, f"n={len(occ_gdf)}")
add_check("Study area non-empty", not area_gdf.empty, f"n={len(area_gdf)}")
add_check("Grid non-empty", not grid_gdf.empty, f"n={len(grid_gdf)}")
add_check("Aggregated grid non-empty", not agg_gdf.empty, f"n={len(agg_gdf)}")

# ----------------------------------------------------------------------
# CRS checks
# ----------------------------------------------------------------------
add_check("Occurrences CRS", occ_gdf.crs is not None, str(occ_gdf.crs))
add_check("Study area CRS", area_gdf.crs is not None, str(area_gdf.crs))
add_check("Grid CRS", grid_gdf.crs is not None, str(grid_gdf.crs))
add_check("Aggregated grid CRS", agg_gdf.crs is not None, str(agg_gdf.crs))

# ----------------------------------------------------------------------
# Column checks
# ----------------------------------------------------------------------
add_check("Grid has cell_id", "cell_id" in grid_gdf.columns, "cell_id present")
add_check("Aggregated grid has cell_id", "cell_id" in agg_gdf.columns, "cell_id present")
add_check("Aggregated grid has n_occurrences", "n_occurrences" in agg_gdf.columns, "n_occurrences present")
add_check("Aggregated grid has n_species", "n_species" in agg_gdf.columns, "n_species present")

# ----------------------------------------------------------------------
# Geometry consistency checks
# ----------------------------------------------------------------------
join_area = gpd.sjoin(occ_gdf, area_gdf, how="inner", predicate="intersects")
join_grid = gpd.sjoin(occ_gdf, grid_gdf[["cell_id", "geometry"]], how="inner", predicate="intersects")

n_occ = len(occ_gdf)
n_area = len(join_area)
n_grid = len(join_grid)

add_check(
    "All selected occurrences intersect study area",
    n_area == n_occ,
    f"{n_area}/{n_occ}"
)

add_check(
    "All selected occurrences intersect H3 grid",
    n_grid == n_occ,
    f"{n_grid}/{n_occ}"
)

# ----------------------------------------------------------------------
# Uniqueness and value checks
# ----------------------------------------------------------------------
add_check(
    "Grid cell_id unique",
    grid_gdf["cell_id"].is_unique,
    f"unique={grid_gdf['cell_id'].is_unique}"
)

add_check(
    "Aggregated grid cell_id unique",
    agg_gdf["cell_id"].is_unique,
    f"unique={agg_gdf['cell_id'].is_unique}"
)

non_negative_occ = (agg_gdf["n_occurrences"] >= 0).all()
non_negative_species = (agg_gdf["n_species"] >= 0).all()
species_le_occ = (agg_gdf["n_species"] <= agg_gdf["n_occurrences"]).all()

add_check(
    "n_occurrences non-negative",
    non_negative_occ,
    "all values >= 0"
)

add_check(
    "n_species non-negative",
    non_negative_species,
    "all values >= 0"
)

add_check(
    "n_species <= n_occurrences for all cells",
    species_le_occ,
    "checked all cells"
)

# ----------------------------------------------------------------------
# Aggregation conservation checks
# ----------------------------------------------------------------------
sum_occ = int(agg_gdf["n_occurrences"].sum())
n_active = int((agg_gdf["n_occurrences"] > 0).sum())
n_zero = int((agg_gdf["n_occurrences"] == 0).sum())

add_check(
    "Total aggregated occurrences equals selected occurrences",
    sum_occ == n_occ,
    f"{sum_occ}/{n_occ}"
)

add_check(
    "Active + zero cells equals total cells",
    (n_active + n_zero) == len(agg_gdf),
    f"{n_active} + {n_zero} = {len(agg_gdf)}"
)

# ----------------------------------------------------------------------
# Optional gbifID-level missing check
# ----------------------------------------------------------------------
if "gbifID" in occ_gdf.columns and "gbifID" in join_grid.columns:
    missing_ids = set(occ_gdf["gbifID"]) - set(join_grid["gbifID"])
    add_check(
        "No gbifID lost in point-to-grid assignment",
        len(missing_ids) == 0,
        f"missing={len(missing_ids)}"
    )
else:
    add_check(
        "gbifID loss check skipped",
        True,
        "gbifID not available in both tables"
    )

# ----------------------------------------------------------------------
# Write summary
# ----------------------------------------------------------------------
n_pass = sum(1 for _, passed, _ in checks if passed)
n_fail = sum(1 for _, passed, _ in checks if not passed)

summary_lines = [
    f"Dataset: {DATASET_NAME}",
    f"Study area: {STUDY_AREA_NAME}",
    f"H3 resolution: {H3_RESOLUTION}",
    "",
    f"PASS checks: {n_pass}",
    f"FAIL checks: {n_fail}",
    "",
    "--------------------------------------------------",
    "Validation checks",
    "--------------------------------------------------",
]

for name, passed, details in checks:
    status = "PASS" if passed else "FAIL"
    summary_lines.append(f"[{status}] {name}: {details}")

with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
    f.write("\n".join(summary_lines))

print(f"\nValidation summary saved to: {SUMMARY_FILE}")