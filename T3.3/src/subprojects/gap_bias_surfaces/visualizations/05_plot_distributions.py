"""
Script: 05_plot_distributions.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Notes:
This script loads the aggregated H3 spatial units produced by
03_aggregate_to_spatial_units.py and generates distribution plots
(histograms) for key spatial metrics.

The following distributions are computed:
- number of occurrence records per cell (n_occurrences)
- number of unique species per cell (n_species)

For each metric, two plots are generated:
1. Full distribution including zero-valued cells
2. Distribution of non-zero cells only, using a log-scaled y-axis

Purpose:
To examine sparsity, skewness, and clustering patterns in the spatial
distribution of biodiversity observations across H3 spatial units.

Input:
- Aggregated H3 spatial units (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_aggregated_res<H3_RESOLUTION>.gpkg

Output:
- Occurrence distribution histogram (all cells):
  results/gap_bias_surfaces/<DATASET_NAME>/<STUDY_AREA_NAME>/figures/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_occurrence_distribution_res<H3_RESOLUTION>.png

- Occurrence distribution histogram (non-zero cells, log y-axis):
  results/gap_bias_surfaces/<DATASET_NAME>/<STUDY_AREA_NAME>/figures/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_occurrence_distribution_nonzero_log_res<H3_RESOLUTION>.png

- Species distribution histogram (all cells):
  results/gap_bias_surfaces/<DATASET_NAME>/<STUDY_AREA_NAME>/figures/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_species_distribution_res<H3_RESOLUTION>.png

- Species distribution histogram (non-zero cells, log y-axis):
  results/gap_bias_surfaces/<DATASET_NAME>/<STUDY_AREA_NAME>/figures/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_species_distribution_nonzero_log_res<H3_RESOLUTION>.png
"""

from pathlib import Path
import geopandas as gpd
import matplotlib.pyplot as plt

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

INPUT_FILE = (
    PROCESSED_DIR
    / DATASET_NAME
    / "gap_bias_surfaces"
    / STUDY_AREA_NAME
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_aggregated_res{H3_RESOLUTION}.gpkg"
)

OUT_DIR = (
    RESULTS_DIR
    / "gap_bias_surfaces"
    / DATASET_NAME
    / STUDY_AREA_NAME
    / "figures"
)
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_OCC_FILE = (
    OUT_DIR
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_occurrence_distribution_res{H3_RESOLUTION}.png"
)

OUTPUT_OCC_NONZERO_LOG_FILE = (
    OUT_DIR
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_occurrence_distribution_nonzero_log_res{H3_RESOLUTION}.png"
)

OUTPUT_SPECIES_FILE = (
    OUT_DIR
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_species_distribution_res{H3_RESOLUTION}.png"
)

OUTPUT_SPECIES_NONZERO_LOG_FILE = (
    OUT_DIR
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_species_distribution_nonzero_log_res{H3_RESOLUTION}.png"
)

# ----------------------------------------------------------------------
# Load data
# ----------------------------------------------------------------------
if not INPUT_FILE.exists():
    raise FileNotFoundError(f"Aggregated H3 grid not found: {INPUT_FILE}")

print(f"Loading aggregated H3 grid from: {INPUT_FILE}")

gdf = gpd.read_file(INPUT_FILE)

if gdf.empty:
    raise ValueError("Aggregated H3 grid is empty.")

print(f"Loaded {len(gdf)} H3 spatial units.")

if "n_occurrences" not in gdf.columns:
    raise ValueError("Column 'n_occurrences' not found.")

if "n_species" not in gdf.columns:
    raise ValueError("Column 'n_species' not found.")

# ----------------------------------------------------------------------
# Prepare non-zero subsets
# ----------------------------------------------------------------------
occ_nonzero = gdf[gdf["n_occurrences"] > 0].copy()
species_nonzero = gdf[gdf["n_species"] > 0].copy()

print(f"Cells with non-zero occurrences: {len(occ_nonzero)}")
print(f"Cells with non-zero species richness: {len(species_nonzero)}")

# ----------------------------------------------------------------------
# Occurrence distribution (all cells)
# ----------------------------------------------------------------------
print("Generating occurrence distribution plot (all cells)...")

plt.figure(figsize=(8, 5))
plt.hist(gdf["n_occurrences"], bins=50)

plt.title(
    f"{DATASET_NAME} occurrence distribution ({STUDY_AREA_NAME}, res={H3_RESOLUTION})"
)
plt.xlabel("n_occurrences")
plt.ylabel("Number of cells")

plt.savefig(OUTPUT_OCC_FILE, dpi=300, bbox_inches="tight")
plt.close()

print(f"Saved: {OUTPUT_OCC_FILE}")

# ----------------------------------------------------------------------
# Occurrence distribution (non-zero cells, log y-axis)
# ----------------------------------------------------------------------
print("Generating occurrence distribution plot (non-zero cells, log y-axis)...")

if occ_nonzero.empty:
    raise ValueError("No non-zero occurrence values found.")

plt.figure(figsize=(8, 5))
plt.hist(occ_nonzero["n_occurrences"], bins=30)
plt.yscale("log")

plt.title(
    f"{DATASET_NAME} occurrence distribution (>0 cells, log y-axis; {STUDY_AREA_NAME}, res={H3_RESOLUTION})"
)
plt.xlabel("n_occurrences")
plt.ylabel("Number of cells (log scale)")

plt.savefig(OUTPUT_OCC_NONZERO_LOG_FILE, dpi=300, bbox_inches="tight")
plt.close()

print(f"Saved: {OUTPUT_OCC_NONZERO_LOG_FILE}")

# ----------------------------------------------------------------------
# Species distribution (all cells)
# ----------------------------------------------------------------------
print("Generating species distribution plot (all cells)...")

plt.figure(figsize=(8, 5))
plt.hist(gdf["n_species"], bins=20)

plt.title(
    f"{DATASET_NAME} species distribution ({STUDY_AREA_NAME}, res={H3_RESOLUTION})"
)
plt.xlabel("n_species")
plt.ylabel("Number of cells")

plt.savefig(OUTPUT_SPECIES_FILE, dpi=300, bbox_inches="tight")
plt.close()

print(f"Saved: {OUTPUT_SPECIES_FILE}")

# ----------------------------------------------------------------------
# Species distribution (non-zero cells, log y-axis)
# ----------------------------------------------------------------------
print("Generating species distribution plot (non-zero cells, log y-axis)...")

if species_nonzero.empty:
    raise ValueError("No non-zero species values found.")

plt.figure(figsize=(8, 5))
plt.hist(species_nonzero["n_species"], bins=10)
plt.yscale("log")

plt.title(
    f"{DATASET_NAME} species distribution (>0 cells, log y-axis; {STUDY_AREA_NAME}, res={H3_RESOLUTION})"
)
plt.xlabel("n_species")
plt.ylabel("Number of cells (log scale)")

plt.savefig(OUTPUT_SPECIES_NONZERO_LOG_FILE, dpi=300, bbox_inches="tight")
plt.close()

print(f"Saved: {OUTPUT_SPECIES_NONZERO_LOG_FILE}")

print("Distribution plots complete.")