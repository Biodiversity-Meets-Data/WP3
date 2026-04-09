"""
Script: 04_spatial_autocorrelation_morans_i.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Notes:
This script computes Global Moran's I statistics to assess spatial autocorrelation
in the aggregated H3 spatial units.

Two complementary analyses are performed:

1) Binary presence/absence Moran's I
   - Variable: has_occurrences (0/1)
   - Tests whether cells with data cluster spatially

2) Log-transformed count Moran's I
   - Variable: log1p(n_occurrences)
   - Tests whether high-intensity sampling areas cluster spatially

Spatial relationships are defined using Queen contiguity between H3 cells.

Purpose:
To move from descriptive spatial analysis to statistical validation by testing
whether observed spatial patterns (coverage and intensity) differ from random.

Input:
- Aggregated H3 spatial units (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_aggregated_res<H3_RESOLUTION>.gpkg

Output:
- Summary text file:
  results/gap_bias_surfaces/<DATASET_NAME>/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_morans_i_res<H3_RESOLUTION>_summary.txt
"""

from pathlib import Path

import geopandas as gpd
import numpy as np
from libpysal.weights import Queen
from esda.moran import Moran

# ----------------------------------------------------------------------
# Project-level paths
# ----------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]

DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"
RESULTS_DIR = PROJECT_ROOT / "results"

# ----------------------------------------------------------------------
# User configuration
# ----------------------------------------------------------------------
DATASET_NAME = "IAS"
STUDY_AREA_NAME = "greece_natura"
H3_RESOLUTION = 6

# ----------------------------------------------------------------------
# Input paths
# ----------------------------------------------------------------------
INPUT_FILE = (
    PROCESSED_DIR
    / DATASET_NAME
    / "gap_bias_surfaces"
    / STUDY_AREA_NAME
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_aggregated_res{H3_RESOLUTION}.gpkg"
)

# ----------------------------------------------------------------------
# Output paths
# ----------------------------------------------------------------------
RESULTS_SUBDIR = RESULTS_DIR / "gap_bias_surfaces" / DATASET_NAME / STUDY_AREA_NAME
RESULTS_SUBDIR.mkdir(parents=True, exist_ok=True)

SUMMARY_FILE = (
    RESULTS_SUBDIR
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_morans_i_res{H3_RESOLUTION}_summary.txt"
)

# ----------------------------------------------------------------------
# Load data
# ----------------------------------------------------------------------
if not INPUT_FILE.exists():
    raise FileNotFoundError(f"Aggregated H3 grid not found: {INPUT_FILE}")

print(f"Loading aggregated H3 grid from: {INPUT_FILE}")

grid_gdf = gpd.read_file(INPUT_FILE)

if grid_gdf.empty:
    raise ValueError("Aggregated H3 grid is empty.")

print(f"Loaded {len(grid_gdf)} H3 spatial units.")

if "n_occurrences" not in grid_gdf.columns:
    raise ValueError("Column 'n_occurrences' not found.")

grid_gdf = grid_gdf.to_crs(epsg=4326)

# ----------------------------------------------------------------------
# Create variables
# ----------------------------------------------------------------------
grid_gdf["has_occurrences"] = (grid_gdf["n_occurrences"] > 0).astype(int)
grid_gdf["log_n_occurrences"] = np.log1p(grid_gdf["n_occurrences"])

n_present = int(grid_gdf["has_occurrences"].sum())
n_absent = int((grid_gdf["has_occurrences"] == 0).sum())

print(f"Cells with data: {n_present}")
print(f"Cells without data: {n_absent}")

# ----------------------------------------------------------------------
# Spatial weights
# ----------------------------------------------------------------------
print("Building Queen contiguity weights...")

weights = Queen.from_dataframe(grid_gdf, use_index=False)
weights.transform = "R"

# ----------------------------------------------------------------------
# Moran - binary
# ----------------------------------------------------------------------
print("Computing Moran's I (binary)...")

y_binary = grid_gdf["has_occurrences"].values
moran_binary = Moran(y_binary, weights)

print(f"Binary Moran's I: {moran_binary.I}")
print(f"Binary p-value: {moran_binary.p_sim}")

# ----------------------------------------------------------------------
# Moran - log counts
# ----------------------------------------------------------------------
print("Computing Moran's I (log counts)...")

y_log = grid_gdf["log_n_occurrences"].values
moran_log = Moran(y_log, weights)

print(f"Log Moran's I: {moran_log.I}")
print(f"Log p-value: {moran_log.p_sim}")

# ----------------------------------------------------------------------
# Interpretation
# ----------------------------------------------------------------------
def interpret(moran_obj, label):
    if moran_obj.p_sim < 0.05:
        if moran_obj.I > 0:
            return f"{label}: Positive spatial autocorrelation (clustering)"
        elif moran_obj.I < 0:
            return f"{label}: Negative spatial autocorrelation (dispersion)"
    return f"{label}: No significant spatial autocorrelation"

interpretation_binary = interpret(moran_binary, "Binary")
interpretation_log = interpret(moran_log, "Log counts")

# ----------------------------------------------------------------------
# Write summary
# ----------------------------------------------------------------------
summary_lines = [
    f"Dataset: {DATASET_NAME}",
    f"Study area: {STUDY_AREA_NAME}",
    f"H3 resolution: {H3_RESOLUTION}",
    "",
    f"Total cells: {len(grid_gdf)}",
    f"Cells with data: {n_present}",
    f"Cells without data: {n_absent}",
    "",
    "--------------------------------------------------",
    "Moran's I - Binary (presence/absence)",
    "--------------------------------------------------",
    f"I: {moran_binary.I}",
    f"Expected I: {moran_binary.EI}",
    f"p-value: {moran_binary.p_sim}",
    f"z-score: {moran_binary.z_sim}",
    interpretation_binary,
    "",
    "--------------------------------------------------",
    "Moran's I - Log counts",
    "--------------------------------------------------",
    "Variable: log1p(n_occurrences)",
    f"I: {moran_log.I}",
    f"Expected I: {moran_log.EI}",
    f"p-value: {moran_log.p_sim}",
    f"z-score: {moran_log.z_sim}",
    interpretation_log,
]

with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
    f.write("\n".join(summary_lines))

print(f"Summary saved to: {SUMMARY_FILE}")
print("Done.")