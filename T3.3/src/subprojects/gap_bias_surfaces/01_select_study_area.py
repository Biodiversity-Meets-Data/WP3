"""
Script: 01_select_study_area.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Notes:
This script selects occurrence records that fall within a predefined
study area polygon and stores both the selected occurrences and a
processed copy of the study area used in the analysis.

The study area input is a GeoPackage (.gpkg) only. The input may contain
one or multiple polygons. If multiple polygons are present, they are
merged into a single study area geometry.

Natura 2000 polygons can also be used as study areas. In that case,
selection is performed using Natura attributes such as:
- SITECODE
- list of SITECODE values
- MS

Purpose:
To define a reproducible study area and extract the subset of filtered
GBIF occurrences that fall within it.

Input:
- Filtered GBIF dataset (ZIP containing CSV) from:
  data/filtered/<DATASET_NAME>/

- Study area polygon (.gpkg) OR Natura polygon dataset (.gpkg)

Output:
- Selected occurrences stored under:
  data/processed/<DATASET_NAME>/gap_bias_surfaces/

- Processed copy of the study area stored under:
  data/processed/<DATASET_NAME>/gap_bias_surfaces/

- Summary text file stored under:
  results/gap_bias_surfaces/<DATASET_NAME>/
"""

from pathlib import Path
import zipfile

import pandas as pd
import geopandas as gpd
from shapely.ops import unary_union

# ----------------------------------------------------------------------
# Project-level paths
# ----------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]  # .../BMD Implementation

DATA_DIR = PROJECT_ROOT / "data"
FILTERED_DIR = DATA_DIR / "filtered"
PROCESSED_DIR = DATA_DIR / "processed"
RESULTS_DIR = PROJECT_ROOT / "results"

# ----------------------------------------------------------------------
# User configuration
# ----------------------------------------------------------------------
DATASET_NAME = "IAS"  # BIRDS / HABITATS / IAS

# Study area naming
STUDY_AREA_NAME = "greece_natura"

# Study area source:
# - "gpkg"   : use a user-provided GeoPackage study area
# - "natura" : use Natura polygons and select by attributes
STUDY_AREA_SOURCE = "natura"  # gpkg / natura

# ----------------------------------------------------------------------
# Generic study area GeoPackage
# ----------------------------------------------------------------------
STUDY_AREA_FILE = DATA_DIR / "external" / "study_areas" / "XXXX.gpkg" #Change as needed

# ----------------------------------------------------------------------
# Natura study area GeoPackage
# ----------------------------------------------------------------------
NATURA_FILE = DATA_DIR / "external" / "natura2k" / "Natura2000_sites_prepared.gpkg"

# Natura selection modes:
# - "site_code"
# - "site_code_list"
# - "member_state"
NATURA_SELECTION_MODE = "member_state"

NATURA_SITE_CODE = "AT3111000"
NATURA_SITE_CODE_LIST = []
NATURA_MEMBER_STATE = "GR"

# ----------------------------------------------------------------------
# Input GBIF filtered dataset
# ----------------------------------------------------------------------
FILTERED_SUBDIR = FILTERED_DIR / DATASET_NAME
FILTERED_ZIP = FILTERED_SUBDIR / f"GBIF_{DATASET_NAME}_filtered_occurrences.zip"
INNER_CSV = f"GBIF_{DATASET_NAME}_filtered_occurrences.csv"

# ----------------------------------------------------------------------
# Output paths
# ----------------------------------------------------------------------
OUT_DIR = PROCESSED_DIR / DATASET_NAME / "gap_bias_surfaces" / STUDY_AREA_NAME
OUT_DIR.mkdir(parents=True, exist_ok=True)

RESULTS_SUBDIR = RESULTS_DIR / "gap_bias_surfaces" / DATASET_NAME / STUDY_AREA_NAME
RESULTS_SUBDIR.mkdir(parents=True, exist_ok=True)

OUTPUT_OCCURRENCES_CSV = OUT_DIR / f"GBIF_{DATASET_NAME}_occurrences_{STUDY_AREA_NAME}.csv"
OUTPUT_OCCURRENCES_GPKG = OUT_DIR / f"GBIF_{DATASET_NAME}_occurrences_{STUDY_AREA_NAME}.gpkg"
OUTPUT_STUDY_AREA_GPKG = OUT_DIR / f"{STUDY_AREA_NAME}_study_area.gpkg"

SUMMARY_FILE = RESULTS_SUBDIR / f"GBIF_{DATASET_NAME}_study_area_{STUDY_AREA_NAME}_summary.txt"

# ----------------------------------------------------------------------
# Load filtered dataset
# ----------------------------------------------------------------------
if not FILTERED_ZIP.exists():
    raise FileNotFoundError(f"Filtered ZIP not found: {FILTERED_ZIP}")

print(f"Loading filtered dataset from ZIP: {FILTERED_ZIP}")

with zipfile.ZipFile(FILTERED_ZIP, "r") as z:
    if INNER_CSV not in z.namelist():
        raise FileNotFoundError(f"{INNER_CSV} not found inside ZIP.")
    with z.open(INNER_CSV) as f:
        df = pd.read_csv(f, low_memory=False)

print(f"Loaded {len(df)} records with {len(df.columns)} columns.")

# ----------------------------------------------------------------------
# Keep valid coordinates and convert to GeoDataFrame
# ----------------------------------------------------------------------
df = df.dropna(subset=["decimalLatitude", "decimalLongitude"]).copy()

print(f"Retained {len(df)} records with valid coordinates.")

if df.empty:
    raise ValueError("No valid coordinates found in filtered dataset.")

occ_gdf = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df["decimalLongitude"], df["decimalLatitude"]),
    crs="EPSG:4326"
)

# ----------------------------------------------------------------------
# Load and prepare study area
# ----------------------------------------------------------------------
print("Loading study area...")

if STUDY_AREA_SOURCE == "gpkg":
    if not STUDY_AREA_FILE.exists():
        raise FileNotFoundError(f"Study area GeoPackage not found: {STUDY_AREA_FILE}")

    area_gdf = gpd.read_file(STUDY_AREA_FILE)

elif STUDY_AREA_SOURCE == "natura":
    if not NATURA_FILE.exists():
        raise FileNotFoundError(f"Natura GeoPackage not found: {NATURA_FILE}")

    area_gdf = gpd.read_file(NATURA_FILE)

    if NATURA_SELECTION_MODE == "site_code":
        area_gdf = area_gdf[area_gdf["SITECODE"] == NATURA_SITE_CODE].copy()

    elif NATURA_SELECTION_MODE == "site_code_list":
        if not NATURA_SITE_CODE_LIST:
            raise ValueError("NATURA_SITE_CODE_LIST is empty.")
        area_gdf = area_gdf[area_gdf["SITECODE"].isin(NATURA_SITE_CODE_LIST)].copy()

    elif NATURA_SELECTION_MODE == "member_state":
        area_gdf = area_gdf[area_gdf["MS"] == NATURA_MEMBER_STATE].copy()

    else:
        raise ValueError(
            f"Unsupported NATURA_SELECTION_MODE: {NATURA_SELECTION_MODE}"
        )

else:
    raise ValueError(
        f"Unsupported STUDY_AREA_SOURCE: {STUDY_AREA_SOURCE}. "
        "Expected 'gpkg' or 'natura'."
    )

if area_gdf.empty:
    raise ValueError("No study area polygons were selected.")

# ----------------------------------------------------------------------
# Reproject study area to EPSG:4326
# ----------------------------------------------------------------------
if area_gdf.crs is None:
    raise ValueError("Study area GeoPackage has no CRS defined.")

area_gdf = area_gdf.to_crs(epsg=4326)

# ----------------------------------------------------------------------
# Merge multiple polygons into one study area geometry
# ----------------------------------------------------------------------
selected_feature_count = len(area_gdf)

merged_geometry = unary_union(area_gdf.geometry)

study_area_gdf = gpd.GeoDataFrame(
    {"study_area_name": [STUDY_AREA_NAME]},
    geometry=[merged_geometry],
    crs="EPSG:4326"
)

print(f"Prepared study area from {selected_feature_count} input polygon(s).")

# ----------------------------------------------------------------------
# Select occurrences intersecting the study area
# ----------------------------------------------------------------------
print("Selecting occurrences intersecting the study area...")

selected_occ_gdf = gpd.sjoin(
    occ_gdf,
    study_area_gdf,
    how="inner",
    predicate="intersects"
).copy()

# Remove join helper columns if present
for col in ["index_right"]:
    if col in selected_occ_gdf.columns:
        selected_occ_gdf.drop(columns=[col], inplace=True)

print(f"Selected {len(selected_occ_gdf)} occurrence records.")

# ----------------------------------------------------------------------
# Export processed study area
# ----------------------------------------------------------------------
study_area_gdf.to_file(OUTPUT_STUDY_AREA_GPKG, driver="GPKG")
print(f"Processed study area saved to: {OUTPUT_STUDY_AREA_GPKG}")

# ----------------------------------------------------------------------
# Export selected occurrences
# ----------------------------------------------------------------------
selected_occ_gdf.drop(columns="geometry").to_csv(OUTPUT_OCCURRENCES_CSV, index=False)
print(f"Selected occurrences CSV saved to: {OUTPUT_OCCURRENCES_CSV}")

selected_occ_gdf.to_file(OUTPUT_OCCURRENCES_GPKG, driver="GPKG")
print(f"Selected occurrences GPKG saved to: {OUTPUT_OCCURRENCES_GPKG}")

# ----------------------------------------------------------------------
# Write summary
# ----------------------------------------------------------------------
summary_lines = [
    f"Dataset name: {DATASET_NAME}",
    f"Study area name: {STUDY_AREA_NAME}",
    f"Study area source: {STUDY_AREA_SOURCE}",
    f"Filtered input ZIP: {FILTERED_ZIP}",
    f"Input records with valid coordinates: {len(occ_gdf)}",
    f"Selected study area polygons before union: {selected_feature_count}",
    f"Selected occurrence records: {len(selected_occ_gdf)}",
    f"Processed study area output: {OUTPUT_STUDY_AREA_GPKG}",
    f"Selected occurrences CSV: {OUTPUT_OCCURRENCES_CSV}",
    f"Selected occurrences GPKG: {OUTPUT_OCCURRENCES_GPKG}",
]

if STUDY_AREA_SOURCE == "gpkg":
    summary_lines.append(f"Study area file: {STUDY_AREA_FILE}")

if STUDY_AREA_SOURCE == "natura":
    summary_lines.append(f"Natura file: {NATURA_FILE}")
    summary_lines.append(f"Natura selection mode: {NATURA_SELECTION_MODE}")
    if NATURA_SELECTION_MODE == "site_code":
        summary_lines.append(f"Natura site code: {NATURA_SITE_CODE}")
    elif NATURA_SELECTION_MODE == "site_code_list":
        summary_lines.append(f"Natura site code list: {NATURA_SITE_CODE_LIST}")
    elif NATURA_SELECTION_MODE == "member_state":
        summary_lines.append(f"Natura member state: {NATURA_MEMBER_STATE}")

with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
    f.write("\n".join(summary_lines))

print(f"Summary saved to: {SUMMARY_FILE}")
print("Study area selection complete.")