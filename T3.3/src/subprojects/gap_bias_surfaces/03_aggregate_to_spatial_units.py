"""
Script: 03_aggregate_to_spatial_units.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Notes:
This script loads the selected occurrence records produced by
01_select_study_area.py and the H3 spatial units produced by
02_prepare_spatial_units.py, and aggregates occurrence information
to the spatial grid.

Aggregation is performed at the H3 cell level and includes:
- number of occurrence records per cell (n_occurrences)
- number of unique species per cell (n_species)

Purpose:
To transform point-based GBIF occurrence data into spatially aggregated
metrics on a hexagonal grid, enabling the analysis of sampling effort,
data availability, and spatial patterns of biodiversity observations.

Input:
- Selected occurrences (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_occurrences_<STUDY_AREA_NAME>.gpkg

- H3 spatial units (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_grid_res<H3_RESOLUTION>.gpkg

Output:
- Aggregated H3 spatial units (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_aggregated_res<H3_RESOLUTION>.gpkg

- Aggregated H3 table (CSV):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_aggregated_res<H3_RESOLUTION>.csv

- Summary text file:
  results/gap_bias_surfaces/<DATASET_NAME>/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_aggregated_res<H3_RESOLUTION>_summary.txt
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd

# ----------------------------------------------------------------------
# Project-level paths
# ----------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]  # .../BMD Implementation

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

INPUT_OCCURRENCES_GPKG = (
    INPUT_DIR / f"GBIF_{DATASET_NAME}_occurrences_{STUDY_AREA_NAME}.gpkg"
)

INPUT_GRID_GPKG = (
    INPUT_DIR / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_grid_res{H3_RESOLUTION}.gpkg"
)

# ----------------------------------------------------------------------
# Output paths
# ----------------------------------------------------------------------
OUT_DIR = INPUT_DIR
OUT_DIR.mkdir(parents=True, exist_ok=True)

RESULTS_SUBDIR = RESULTS_DIR / "gap_bias_surfaces" / DATASET_NAME / STUDY_AREA_NAME
RESULTS_SUBDIR.mkdir(parents=True, exist_ok=True)

OUTPUT_GRID_GPKG = (
    OUT_DIR / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_aggregated_res{H3_RESOLUTION}.gpkg"
)

OUTPUT_GRID_CSV = (
    OUT_DIR / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_aggregated_res{H3_RESOLUTION}.csv"
)

SUMMARY_FILE = (
    RESULTS_SUBDIR
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_aggregated_res{H3_RESOLUTION}_summary.txt"
)

# ----------------------------------------------------------------------
# Load selected occurrences
# ----------------------------------------------------------------------
if not INPUT_OCCURRENCES_GPKG.exists():
    raise FileNotFoundError(f"Selected occurrences not found: {INPUT_OCCURRENCES_GPKG}")

print(f"Loading selected occurrences: {INPUT_OCCURRENCES_GPKG}")

occ_gdf = gpd.read_file(INPUT_OCCURRENCES_GPKG)

if occ_gdf.empty:
    raise ValueError("Selected occurrences GeoPackage is empty.")

print(f"Loaded {len(occ_gdf)} selected occurrence records.")

# ----------------------------------------------------------------------
# Load H3 spatial units
# ----------------------------------------------------------------------
if not INPUT_GRID_GPKG.exists():
    raise FileNotFoundError(f"H3 grid not found: {INPUT_GRID_GPKG}")

print(f"Loading H3 spatial units: {INPUT_GRID_GPKG}")

grid_gdf = gpd.read_file(INPUT_GRID_GPKG)

if grid_gdf.empty:
    raise ValueError("H3 grid GeoPackage is empty.")

print(f"Loaded {len(grid_gdf)} H3 spatial units.")

# ----------------------------------------------------------------------
# Reproject to a common CRS if needed
# ----------------------------------------------------------------------
if occ_gdf.crs is None:
    raise ValueError("Selected occurrences have no CRS defined.")

if grid_gdf.crs is None:
    raise ValueError("H3 grid has no CRS defined.")

occ_gdf = occ_gdf.to_crs(epsg=4326)
grid_gdf = grid_gdf.to_crs(epsg=4326)

# ----------------------------------------------------------------------
# Spatial join: assign occurrences to H3 cells
# ----------------------------------------------------------------------
print("Assigning occurrence records to H3 spatial units...")

joined_gdf = gpd.sjoin(
    occ_gdf,
    grid_gdf[["cell_id", "geometry"]],
    how="inner",
    predicate="intersects"
).copy()

if "index_right" in joined_gdf.columns:
    joined_gdf.drop(columns=["index_right"], inplace=True)

print(f"Assigned {len(joined_gdf)} occurrence records to H3 cells.")

# ----------------------------------------------------------------------
# Aggregate occurrence information per H3 cell
# ----------------------------------------------------------------------
print("Aggregating occurrence information per H3 cell...")

if "scientificName" not in joined_gdf.columns:
    raise ValueError("Column 'scientificName' not found in selected occurrences.")

agg_df = (
    joined_gdf.groupby("cell_id")
    .agg(
        n_occurrences=("cell_id", "size"),
        n_species=("scientificName", "nunique")
    )
    .reset_index()
)

print(f"Computed aggregation metrics for {len(agg_df)} H3 cells.")

# ----------------------------------------------------------------------
# Merge aggregated metrics back to the full H3 grid
# ----------------------------------------------------------------------
grid_agg_gdf = grid_gdf.merge(agg_df, on="cell_id", how="left")

grid_agg_gdf["n_occurrences"] = grid_agg_gdf["n_occurrences"].fillna(0).astype(int)
grid_agg_gdf["n_species"] = grid_agg_gdf["n_species"].fillna(0).astype(int)

total_cells = len(grid_agg_gdf)
cells_with_occurrence = int((grid_agg_gdf["n_occurrences"] > 0).sum())
n_empty_cells = int((grid_agg_gdf["n_occurrences"] == 0).sum())

print(f"H3 cells with zero occurrences: {n_empty_cells}")

# ----------------------------------------------------------------------
# Summary statistics
# ----------------------------------------------------------------------
coverage_pct = (cells_with_occurrence / total_cells) * 100

occ_min = grid_agg_gdf["n_occurrences"].min()
occ_max = grid_agg_gdf["n_occurrences"].max()
occ_mean = grid_agg_gdf["n_occurrences"].mean()
occ_median = grid_agg_gdf["n_occurrences"].median()

species_min = grid_agg_gdf["n_species"].min()
species_max = grid_agg_gdf["n_species"].max()
species_mean = grid_agg_gdf["n_species"].mean()
species_median = grid_agg_gdf["n_species"].median()

total_species = occ_gdf["scientificName"].nunique()

# ----------------------------------------------------------------------
# Export aggregated H3 grid
# ----------------------------------------------------------------------
grid_agg_gdf.to_file(OUTPUT_GRID_GPKG, driver="GPKG")
print(f"Aggregated H3 grid saved to: {OUTPUT_GRID_GPKG}")

grid_agg_gdf.drop(columns="geometry").to_csv(OUTPUT_GRID_CSV, index=False)
print(f"Aggregated H3 table saved to: {OUTPUT_GRID_CSV}")

# ----------------------------------------------------------------------
# Write summary
# ----------------------------------------------------------------------
summary_lines = [
    f"Dataset name: {DATASET_NAME}",
    f"Study area name: {STUDY_AREA_NAME}",
    f"H3 resolution: {H3_RESOLUTION}",
    f"Selected occurrences input: {INPUT_OCCURRENCES_GPKG}",
    f"H3 grid input: {INPUT_GRID_GPKG}",
    f"Selected occurrence records: {len(occ_gdf)}",
    f"H3 spatial units: {len(grid_gdf)}",
    f"Joined occurrence-cell records: {len(joined_gdf)}",
    f"Cells with at least one occurrence: {cells_with_occurrence}",
    f"Cells with zero occurrences: {n_empty_cells}",
    f"Aggregated H3 output (GPKG): {OUTPUT_GRID_GPKG}",
    f"Aggregated H3 output (CSV): {OUTPUT_GRID_CSV}",
]

summary_lines.extend([
    "",
    "--------------------------------------------------",
    "Coverage statistics",
    "--------------------------------------------------",
    f"Coverage (%): {coverage_pct:.2f}",
    "",
    "--------------------------------------------------",
    "Occurrence statistics (n_occurrences)",
    "--------------------------------------------------",
    f"Min: {occ_min}",
    f"Max: {occ_max}",
    f"Mean: {occ_mean:.2f}",
    f"Median: {occ_median:.2f}",
    "",
    "--------------------------------------------------",
    "Species statistics (n_species)",
    "--------------------------------------------------",
    f"Min: {species_min}",
    f"Max: {species_max}",
    f"Mean: {species_mean:.2f}",
    f"Median: {species_median:.2f}",
    "",
    "--------------------------------------------------",
    "Dataset-level biodiversity",
    "--------------------------------------------------",
    f"Total unique species: {total_species}",
    ])

with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
    f.write("\n".join(summary_lines))

print(f"Summary saved to: {SUMMARY_FILE}")
print("Aggregation complete.")