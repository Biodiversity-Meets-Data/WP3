"""
Script: 03_plot_occurrence_density.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Notes:
This script loads the aggregated H3 spatial units produced by
03_aggregate_to_spatial_units.py and visualizes occurrence density
within the selected study area.

Density is represented using the number of occurrence records per H3 cell
(n_occurrences).

Purpose:
To provide a spatial overview of sampling intensity and identify
areas with relatively high or low occurrence density across the
selected study area.

Input:
- Aggregated H3 spatial units (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_aggregated_res<H3_RESOLUTION>.gpkg

- Processed study area polygon (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  <STUDY_AREA_NAME>_study_area.gpkg

Output:
- Figure stored under:
  results/gap_bias_surfaces/<DATASET_NAME>/<STUDY_AREA_NAME>/figures/
"""

from pathlib import Path
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx
import numpy as np

# ----------------------------------------------------------------------
# Project-level paths
# ----------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[4]  # .../BMD Implementation

DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"
RESULTS_DIR = PROJECT_ROOT / "results"

# ----------------------------------------------------------------------
# User configuration
# ----------------------------------------------------------------------
DATASET_NAME = "IAS"  # BIRDS / HABITATS / IAS
STUDY_AREA_NAME = "greece_natura"
H3_RESOLUTION = 6

INPUT_GRID_FILE = (
    PROCESSED_DIR
    / DATASET_NAME
    / "gap_bias_surfaces"
    / STUDY_AREA_NAME
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_aggregated_res{H3_RESOLUTION}.gpkg"
)

INPUT_STUDY_AREA_FILE = (
    PROCESSED_DIR
    / DATASET_NAME
    / "gap_bias_surfaces"
    / STUDY_AREA_NAME
    / f"{STUDY_AREA_NAME}_study_area.gpkg"
)

OUT_DIR = (
    RESULTS_DIR
    / "gap_bias_surfaces"
    / DATASET_NAME
    / STUDY_AREA_NAME
    / "figures"
)
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = (
    OUT_DIR
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_density_res{H3_RESOLUTION}.png"
)

# ----------------------------------------------------------------------
# Load inputs
# ----------------------------------------------------------------------
if not INPUT_GRID_FILE.exists():
    raise FileNotFoundError(f"Aggregated H3 grid not found: {INPUT_GRID_FILE}")

if not INPUT_STUDY_AREA_FILE.exists():
    raise FileNotFoundError(f"Study area not found: {INPUT_STUDY_AREA_FILE}")

print(f"Loading aggregated H3 grid from: {INPUT_GRID_FILE}")
grid_gdf = gpd.read_file(INPUT_GRID_FILE)

print(f"Loading study area from: {INPUT_STUDY_AREA_FILE}")
study_area_gdf = gpd.read_file(INPUT_STUDY_AREA_FILE)

if grid_gdf.empty:
    raise ValueError("Aggregated H3 grid is empty.")

if study_area_gdf.empty:
    raise ValueError("Study area is empty.")

print(f"Loaded {len(grid_gdf)} H3 spatial units.")

if "n_occurrences" not in grid_gdf.columns:
    raise ValueError("Column 'n_occurrences' not found in aggregated H3 grid.")

# ----------------------------------------------------------------------
# Keep only cells with data
# ----------------------------------------------------------------------
density_gdf = grid_gdf[grid_gdf["n_occurrences"] > 0].copy()

density_gdf["n_occ_log"] = np.log1p(density_gdf["n_occurrences"])

if density_gdf.empty:
    raise ValueError("No H3 cells with occurrence records were found.")

print(f"Cells with occurrence data: {len(density_gdf)}")
print(f"Minimum n_occurrences: {density_gdf['n_occurrences'].min()}")
print(f"Maximum n_occurrences: {density_gdf['n_occurrences'].max()}")

# ----------------------------------------------------------------------
# Reproject to Web Mercator
# ----------------------------------------------------------------------
density_gdf = density_gdf.to_crs(epsg=3857)
study_area_gdf = study_area_gdf.to_crs(epsg=3857)

# ----------------------------------------------------------------------
# Create plot
# ----------------------------------------------------------------------
print("Generating occurrence density map...")

fig, ax = plt.subplots(figsize=(10, 10))

study_area_gdf.boundary.plot(
    ax=ax,
    color="red",
    linewidth=1.0
)

density_gdf.plot(
    ax=ax,
    column="n_occ_log",
    cmap="viridis",
    legend=True,
    edgecolor="none",
    alpha=0.8
)

ctx.add_basemap(ax)

ax.set_title(
    f"{DATASET_NAME} occurrence density (log scale, {STUDY_AREA_NAME}, res={H3_RESOLUTION})"
)
ax.set_axis_off()

# ----------------------------------------------------------------------
# Save figure
# ----------------------------------------------------------------------
plt.savefig(OUTPUT_FILE, dpi=300, bbox_inches="tight")
plt.close()

print(f"Occurrence density figure saved to: {OUTPUT_FILE}")
print("Visualization complete.")