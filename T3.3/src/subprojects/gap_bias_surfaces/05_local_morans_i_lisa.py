"""
Script: 05_local_morans_i_lisa.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Notes:
This script computes Local Moran's I (LISA) statistics to identify local
spatial clusters and outliers in the aggregated H3 spatial units.

Two complementary analyses are performed:

1) Binary LISA
   - Variable: has_occurrences (1 if n_occurrences > 0, else 0)
   - Identifies local clustering of data presence/absence

2) Intensity LISA
   - Variable: log1p(n_occurrences)
   - Identifies local clustering of sampling intensity while reducing
     the influence of extreme outliers

Spatial relationships are defined using Queen contiguity.

The script produces:
- Binary LISA cluster map without basemap
- Binary LISA cluster map with basemap
- Intensity LISA cluster map without basemap
- Intensity LISA cluster map with basemap
- A summary of cluster counts and definitions

Purpose:
To identify where local spatial clustering occurs and classify cells as
hotspots, coldspots, or spatial outliers for both occurrence presence
and occurrence intensity.

Input:
- Aggregated H3 spatial units (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_aggregated_res<H3_RESOLUTION>.gpkg

- Processed study area polygon (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  <STUDY_AREA_NAME>_study_area.gpkg

Output:
- Binary LISA cluster map without basemap (PNG):
  results/gap_bias_surfaces/<DATASET_NAME>/<STUDY_AREA_NAME>/figures/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_lisa_binary_clusters_res<H3_RESOLUTION>.png

- Binary LISA cluster map with basemap (PNG):
  results/gap_bias_surfaces/<DATASET_NAME>/<STUDY_AREA_NAME>/figures/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_lisa_binary_clusters_basemap_res<H3_RESOLUTION>.png

- Intensity LISA cluster map without basemap (PNG):
  results/gap_bias_surfaces/<DATASET_NAME>/<STUDY_AREA_NAME>/figures/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_lisa_intensity_clusters_res<H3_RESOLUTION>.png

- Intensity LISA cluster map with basemap (PNG):
  results/gap_bias_surfaces/<DATASET_NAME>/<STUDY_AREA_NAME>/figures/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_lisa_intensity_clusters_basemap_res<H3_RESOLUTION>.png

- Summary text file:
  results/gap_bias_surfaces/<DATASET_NAME>/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_lisa_res<H3_RESOLUTION>_summary.txt
"""

from pathlib import Path

import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import contextily as ctx
from libpysal.weights import Queen
from esda.moran import Moran_Local

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
DATASET_NAME = "IAS"  # BIRDS / HABITATS / IAS
STUDY_AREA_NAME = "greece_natura"
H3_RESOLUTION = 6

# ----------------------------------------------------------------------
# Input paths
# ----------------------------------------------------------------------
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

# ----------------------------------------------------------------------
# Output paths
# ----------------------------------------------------------------------
RESULTS_SUBDIR = RESULTS_DIR / "gap_bias_surfaces" / DATASET_NAME / STUDY_AREA_NAME
RESULTS_SUBDIR.mkdir(parents=True, exist_ok=True)

FIGURES_DIR = RESULTS_SUBDIR / "figures"
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_BINARY_FIGURE = (
    FIGURES_DIR
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_lisa_binary_clusters_res{H3_RESOLUTION}.png"
)

OUTPUT_BINARY_FIGURE_BASEMAP = (
    FIGURES_DIR
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_lisa_binary_clusters_basemap_res{H3_RESOLUTION}.png"
)

OUTPUT_INTENSITY_FIGURE = (
    FIGURES_DIR
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_lisa_intensity_clusters_res{H3_RESOLUTION}.png"
)

OUTPUT_INTENSITY_FIGURE_BASEMAP = (
    FIGURES_DIR
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_lisa_intensity_clusters_basemap_res{H3_RESOLUTION}.png"
)

SUMMARY_FILE = (
    RESULTS_SUBDIR
    / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_lisa_res{H3_RESOLUTION}_summary.txt"
)

# ----------------------------------------------------------------------
# Load data
# ----------------------------------------------------------------------
if not INPUT_GRID_FILE.exists():
    raise FileNotFoundError(f"Aggregated H3 grid not found: {INPUT_GRID_FILE}")

if not INPUT_STUDY_AREA_FILE.exists():
    raise FileNotFoundError(f"Study area file not found: {INPUT_STUDY_AREA_FILE}")

print(f"Loading aggregated H3 grid from: {INPUT_GRID_FILE}")
grid_gdf = gpd.read_file(INPUT_GRID_FILE)

print(f"Loading study area from: {INPUT_STUDY_AREA_FILE}")
study_area_gdf = gpd.read_file(INPUT_STUDY_AREA_FILE)

if grid_gdf.empty:
    raise ValueError("Aggregated H3 grid is empty.")

if study_area_gdf.empty:
    raise ValueError("Study area is empty.")

grid_gdf = grid_gdf.to_crs(epsg=4326)
study_area_gdf = study_area_gdf.to_crs(epsg=4326)

if "n_occurrences" not in grid_gdf.columns:
    raise ValueError("Column 'n_occurrences' not found in aggregated H3 grid.")

# ----------------------------------------------------------------------
# Create analysis variables
# ----------------------------------------------------------------------
grid_gdf["has_occurrences"] = (grid_gdf["n_occurrences"] > 0).astype(int)
grid_gdf["log_n_occurrences"] = np.log1p(grid_gdf["n_occurrences"])

# ----------------------------------------------------------------------
# Spatial weights
# ----------------------------------------------------------------------
print("Building Queen contiguity weights...")

weights = Queen.from_dataframe(grid_gdf, use_index=False)
weights.transform = "R"

# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------
def classify_lisa_clusters(local_moran_obj, significance_mask):
    """
    Return a list of LISA cluster labels using standard quadrant coding:
    1 = HH, 2 = LH, 3 = LL, 4 = HL
    """
    labels = ["Not significant"] * len(local_moran_obj.q)

    for i, (q, sig) in enumerate(zip(local_moran_obj.q, significance_mask)):
        if not sig:
            continue
        if q == 1:
            labels[i] = "HH"
        elif q == 2:
            labels[i] = "LH"
        elif q == 3:
            labels[i] = "LL"
        elif q == 4:
            labels[i] = "HL"

    return labels


def count_clusters(series):
    counts = series.value_counts()
    return {
        "HH": int(counts.get("HH", 0)),
        "LL": int(counts.get("LL", 0)),
        "HL": int(counts.get("HL", 0)),
        "LH": int(counts.get("LH", 0)),
        "Not significant": int(counts.get("Not significant", 0)),
    }


def make_legend_handles(color_map):
    return [
        mpatches.Patch(color=color_map["HH"], label="HH"),
        mpatches.Patch(color=color_map["LL"], label="LL"),
        mpatches.Patch(color=color_map["HL"], label="HL"),
        mpatches.Patch(color=color_map["LH"], label="LH"),
        mpatches.Patch(color=color_map["Not significant"], label="Not significant"),
    ]


def plot_lisa_map(
    grid_input_gdf,
    study_area_input_gdf,
    cluster_column,
    color_map,
    output_file,
    title,
    add_basemap=False
):
    plot_grid_gdf = grid_input_gdf.copy()
    plot_study_area_gdf = study_area_input_gdf.copy()

    if add_basemap:
        plot_grid_gdf = plot_grid_gdf.to_crs(epsg=3857)
        plot_study_area_gdf = plot_study_area_gdf.to_crs(epsg=3857)

    fig, ax = plt.subplots(figsize=(10, 10))

    # Plot non-significant cells first
    plot_grid_gdf[plot_grid_gdf[cluster_column] == "Not significant"].plot(
        ax=ax,
        color=color_map["Not significant"],
        edgecolor="none",
        alpha=0.55
    )

    # Plot significant classes on top
    for cls in ["LL", "LH", "HL", "HH"]:
        subset = plot_grid_gdf[plot_grid_gdf[cluster_column] == cls]
        if not subset.empty:
            subset.plot(
                ax=ax,
                color=color_map[cls],
                edgecolor="none",
                alpha=0.9
            )

    # Plot outline
    plot_study_area_gdf.boundary.plot(
        ax=ax,
        color="black",
        linewidth=0.8
    )

    if add_basemap:
        ctx.add_basemap(ax)

    ax.set_title(title)
    ax.axis("off")

    ax.legend(
        handles=make_legend_handles(color_map),
        title="LISA clusters",
        loc="lower left",
        frameon=True
    )

    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()


# ----------------------------------------------------------------------
# Color map
# ----------------------------------------------------------------------
color_map = {
    "HH": "red",
    "LL": "blue",
    "HL": "orange",
    "LH": "purple",
    "Not significant": "lightgrey"
}

# ----------------------------------------------------------------------
# Compute binary LISA
# ----------------------------------------------------------------------
print("Computing Local Moran's I (binary presence/absence)...")

y_binary = grid_gdf["has_occurrences"].values
lisa_binary = Moran_Local(y_binary, weights)
sig_binary = lisa_binary.p_sim < 0.05

grid_gdf["lisa_binary_cluster"] = classify_lisa_clusters(lisa_binary, sig_binary)
binary_counts = count_clusters(grid_gdf["lisa_binary_cluster"])

# ----------------------------------------------------------------------
# Compute intensity LISA
# ----------------------------------------------------------------------
print("Computing Local Moran's I (log occurrence intensity)...")

y_intensity = grid_gdf["log_n_occurrences"].values
lisa_intensity = Moran_Local(y_intensity, weights)
sig_intensity = lisa_intensity.p_sim < 0.05

grid_gdf["lisa_intensity_cluster"] = classify_lisa_clusters(lisa_intensity, sig_intensity)
intensity_counts = count_clusters(grid_gdf["lisa_intensity_cluster"])

# ----------------------------------------------------------------------
# Plot binary maps
# ----------------------------------------------------------------------
print("Generating binary LISA map without basemap...")

plot_lisa_map(
    grid_input_gdf=grid_gdf,
    study_area_input_gdf=study_area_gdf,
    cluster_column="lisa_binary_cluster",
    color_map=color_map,
    output_file=OUTPUT_BINARY_FIGURE,
    title=f"Binary LISA clusters ({DATASET_NAME}, {STUDY_AREA_NAME}, res={H3_RESOLUTION})",
    add_basemap=False
)

print(f"Saved: {OUTPUT_BINARY_FIGURE}")

print("Generating binary LISA map with basemap...")

plot_lisa_map(
    grid_input_gdf=grid_gdf,
    study_area_input_gdf=study_area_gdf,
    cluster_column="lisa_binary_cluster",
    color_map=color_map,
    output_file=OUTPUT_BINARY_FIGURE_BASEMAP,
    title=f"Binary LISA clusters with basemap ({DATASET_NAME}, {STUDY_AREA_NAME}, res={H3_RESOLUTION})",
    add_basemap=True
)

print(f"Saved: {OUTPUT_BINARY_FIGURE_BASEMAP}")

# ----------------------------------------------------------------------
# Plot intensity maps
# ----------------------------------------------------------------------
print("Generating intensity LISA map without basemap...")

plot_lisa_map(
    grid_input_gdf=grid_gdf,
    study_area_input_gdf=study_area_gdf,
    cluster_column="lisa_intensity_cluster",
    color_map=color_map,
    output_file=OUTPUT_INTENSITY_FIGURE,
    title=f"Intensity LISA clusters ({DATASET_NAME}, {STUDY_AREA_NAME}, res={H3_RESOLUTION})",
    add_basemap=False
)

print(f"Saved: {OUTPUT_INTENSITY_FIGURE}")

print("Generating intensity LISA map with basemap...")

plot_lisa_map(
    grid_input_gdf=grid_gdf,
    study_area_input_gdf=study_area_gdf,
    cluster_column="lisa_intensity_cluster",
    color_map=color_map,
    output_file=OUTPUT_INTENSITY_FIGURE_BASEMAP,
    title=f"Intensity LISA clusters with basemap ({DATASET_NAME}, {STUDY_AREA_NAME}, res={H3_RESOLUTION})",
    add_basemap=True
)

print(f"Saved: {OUTPUT_INTENSITY_FIGURE_BASEMAP}")

# ----------------------------------------------------------------------
# Write summary
# ----------------------------------------------------------------------
summary_lines = [
    f"Dataset: {DATASET_NAME}",
    f"Study area: {STUDY_AREA_NAME}",
    f"H3 resolution: {H3_RESOLUTION}",
    "",
    "--------------------------------------------------",
    "Binary LISA cluster counts",
    "--------------------------------------------------",
    "Variable: has_occurrences (1 if n_occurrences > 0, else 0)",
    f"High-High (HH): {binary_counts['HH']}",
    f"Low-Low (LL): {binary_counts['LL']}",
    f"High-Low (HL): {binary_counts['HL']}",
    f"Low-High (LH): {binary_counts['LH']}",
    f"Not significant: {binary_counts['Not significant']}",
    "",
    "--------------------------------------------------",
    "Intensity LISA cluster counts",
    "--------------------------------------------------",
    "Variable: log1p(n_occurrences)",
    f"High-High (HH): {intensity_counts['HH']}",
    f"Low-Low (LL): {intensity_counts['LL']}",
    f"High-Low (HL): {intensity_counts['HL']}",
    f"Low-High (LH): {intensity_counts['LH']}",
    f"Not significant: {intensity_counts['Not significant']}",
    "",
    "--------------------------------------------------",
    "LISA cluster definitions",
    "--------------------------------------------------",
    "High–High (HH): cells with high values surrounded by high values; hotspots",
    "Low–Low (LL): cells with low values surrounded by low values; coldspots or gaps",
    "High–Low (HL): high-value cells surrounded by low-value neighbors; spatial outliers",
    "Low–High (LH): low-value cells surrounded by high-value neighbors; spatial outliers",
    "Not significant: no statistically significant local spatial pattern detected",
    "",
    "Significance threshold: p < 0.05 (permutation-based)",
]

with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
    f.write("\n".join(summary_lines))

print(f"Summary saved to: {SUMMARY_FILE}")
print("LISA analysis complete.")