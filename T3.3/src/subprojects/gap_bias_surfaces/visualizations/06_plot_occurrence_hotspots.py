"""
Script: 06_plot_occurrence_hotspots.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Notes:
This script loads the aggregated H3 spatial units produced by
03_aggregate_to_spatial_units.py and visualizes occurrence hotspots
within the selected study area.

Hotspots are defined as the H3 cells whose occurrence counts
(n_occurrences) fall within the top percentage of all non-zero cells.

Purpose:
To identify and visualize the spatial concentration of occurrence
records and highlight the cells with the highest sampling intensity.

Input:
- Aggregated H3 spatial units (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_aggregated_res<H3_RESOLUTION>.gpkg

- Processed study area polygon (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  <STUDY_AREA_NAME>_study_area.gpkg

Output:
- Hotspot figure stored under:
  results/gap_bias_surfaces/<DATASET_NAME>/<STUDY_AREA_NAME>/figures/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_occurrence_hotspots_top<TOP_PERCENT>_res<H3_RESOLUTION>.png
"""

from pathlib import Path
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx

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
TOP_PERCENT = 5  # top percentage of non-zero cells

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
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_occurrence_hotspots_top{TOP_PERCENT}_res{H3_RESOLUTION}.png"
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

if "n_occurrences" not in grid_gdf.columns:
    raise ValueError("Column 'n_occurrences' not found in aggregated H3 grid.")

print(f"Loaded {len(grid_gdf)} H3 spatial units.")

# ----------------------------------------------------------------------
# Keep only non-zero cells
# ----------------------------------------------------------------------
nonzero_gdf = grid_gdf[grid_gdf["n_occurrences"] > 0].copy()

if nonzero_gdf.empty:
    raise ValueError("No H3 cells with occurrence records were found.")

print(f"Cells with occurrence data: {len(nonzero_gdf)}")

# ----------------------------------------------------------------------
# Define hotspots using percentile threshold
# ----------------------------------------------------------------------
threshold = nonzero_gdf["n_occurrences"].quantile(1 - TOP_PERCENT / 100)

hotspots_gdf = nonzero_gdf[nonzero_gdf["n_occurrences"] >= threshold].copy()

print(f"Hotspot threshold (top {TOP_PERCENT}%): {threshold}")
print(f"Hotspot cells: {len(hotspots_gdf)}")

# ----------------------------------------------------------------------
# Reproject to Web Mercator
# ----------------------------------------------------------------------
grid_gdf = grid_gdf.to_crs(epsg=3857)
hotspots_gdf = hotspots_gdf.to_crs(epsg=3857)
study_area_gdf = study_area_gdf.to_crs(epsg=3857)

# ----------------------------------------------------------------------
# Create plot
# ----------------------------------------------------------------------
print("Generating hotspot map...")

fig, ax = plt.subplots(figsize=(10, 10))

# Background cells with data
grid_gdf[grid_gdf["n_occurrences"] > 0].plot(
    ax=ax,
    color="lightgrey",
    edgecolor="none",
    alpha=0.5
)

# Hotspots
hotspots_gdf.plot(
    ax=ax,
    color="crimson",
    edgecolor="none",
    alpha=0.9
)

# Study area boundary
study_area_gdf.boundary.plot(
    ax=ax,
    color="black",
    linewidth=0.8
)

ctx.add_basemap(ax)

ax.set_title(
    f"{DATASET_NAME} occurrence hotspots (top {TOP_PERCENT}%, {STUDY_AREA_NAME}, res={H3_RESOLUTION})"
)
ax.set_axis_off()

# ----------------------------------------------------------------------
# Save figure
# ----------------------------------------------------------------------
plt.savefig(OUTPUT_FILE, dpi=300, bbox_inches="tight")
plt.close()

print(f"Hotspot figure saved to: {OUTPUT_FILE}")
print("Visualization complete.")