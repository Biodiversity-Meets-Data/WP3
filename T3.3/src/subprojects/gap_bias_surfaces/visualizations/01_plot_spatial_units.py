"""
Script: 01_plot_spatial_units.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Notes:
This script loads the H3 spatial units produced by
02_prepare_spatial_units.py and generates a simple map for visual
inspection of the spatial grid within the selected study area.

Purpose:
To provide a visual sanity check of the spatial framework created
for the gap and bias surface analyses.

Input:
- H3 spatial units (GeoPackage) from:
  data/processed/<DATASET_NAME>/gap_bias_surfaces/

- Processed study area polygon (GeoPackage) from:
  data/processed/<DATASET_NAME>/gap_bias_surfaces/

Output:
- Figure stored under:
  results/gap_bias_surfaces/<DATASET_NAME>/figures/
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

INPUT_GRID_FILE = (
    PROCESSED_DIR
    / DATASET_NAME
    / "gap_bias_surfaces"
    / STUDY_AREA_NAME
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_grid_res{H3_RESOLUTION}.gpkg"
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
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_grid_res{H3_RESOLUTION}_sample_map.png"
)

# ----------------------------------------------------------------------
# Load inputs
# ----------------------------------------------------------------------
if not INPUT_GRID_FILE.exists():
    raise FileNotFoundError(f"H3 grid not found: {INPUT_GRID_FILE}")

if not INPUT_STUDY_AREA_FILE.exists():
    raise FileNotFoundError(f"Study area not found: {INPUT_STUDY_AREA_FILE}")

print(f"Loading H3 spatial units from: {INPUT_GRID_FILE}")
grid_gdf = gpd.read_file(INPUT_GRID_FILE)

print(f"Loading study area from: {INPUT_STUDY_AREA_FILE}")
study_area_gdf = gpd.read_file(INPUT_STUDY_AREA_FILE)

print(f"Loaded {len(grid_gdf)} H3 spatial units.")

if grid_gdf.empty:
    raise ValueError("Input H3 grid is empty.")

if study_area_gdf.empty:
    raise ValueError("Input study area is empty.")

# ----------------------------------------------------------------------
# Sample spatial units for visualization
# ----------------------------------------------------------------------
print("Sampling H3 spatial units for visualization...")

N_SAMPLE = 5000

if len(grid_gdf) > N_SAMPLE:
    grid_plot_gdf = grid_gdf.sample(n=N_SAMPLE, random_state=42)
else:
    grid_plot_gdf = grid_gdf.copy()

# ----------------------------------------------------------------------
# Reproject to Web Mercator
# ----------------------------------------------------------------------
grid_plot_gdf = grid_plot_gdf.to_crs(epsg=3857)
study_area_gdf = study_area_gdf.to_crs(epsg=3857)

# ----------------------------------------------------------------------
# Create plot
# ----------------------------------------------------------------------
print("Generating spatial unit map...")

fig, ax = plt.subplots(figsize=(10, 10))

study_area_gdf.boundary.plot(
    ax=ax,
    color="red",
    linewidth=1.0
)

grid_plot_gdf.plot(
    ax=ax,
    facecolor="lightblue",
    edgecolor="black",
    linewidth=0.2,
    alpha=0.6
)

ctx.add_basemap(ax)

ax.set_title(
    f"{DATASET_NAME} H3 spatial units ({STUDY_AREA_NAME}, res={H3_RESOLUTION})"
)
ax.set_axis_off()

# ----------------------------------------------------------------------
# Save figure
# ----------------------------------------------------------------------
plt.savefig(OUTPUT_FILE, dpi=300, bbox_inches="tight")
plt.close()

print(f"Spatial unit figure saved to: {OUTPUT_FILE}")
print("Visualization complete.")