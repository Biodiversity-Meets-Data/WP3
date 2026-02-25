"""
Script: 03_filtered_data_summaries.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Notes:
This script loads the filtered GBIF dataset produced by 02_filter_gbif_dataset.py
and generates separate summary tables by key attributes:
- occurrences per species
- occurrences per country
- occurrences per year
- occurrences per basis of record

Purpose:
To aggregate the filtered dataset into structured summary tables
for quick analysis, visualization, and reporting as part of the core pipeline.

Input:
- Filtered GBIF dataset (CSV) from:
  data/filtered/GBIF_<DATASET_NAME>_filtered_occurrences.csv

Output:
- Summary tables stored under:
  results/filtering/<DATASET_NAME>/
"""

from pathlib import Path
import pandas as pd

# ----------------------------------------------------------------------
# Project-level paths
# ----------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]  # .../BMD Implementation

DATA_DIR = PROJECT_ROOT / "data"
FILTERED_DIR = DATA_DIR / "filtered"
RESULTS_DIR = PROJECT_ROOT / "results" / "filtering"

# ----------------------------------------------------------------------
# User configuration
# ----------------------------------------------------------------------
DATASET_NAME = "HABITATS"  # BIRDS / HABITATS / IAS

FILTERED_SUBDIR = FILTERED_DIR / DATASET_NAME
FILTERED_ZIP = FILTERED_SUBDIR / f"GBIF_{DATASET_NAME}_filtered_occurrences.zip"
INNER_CSV = f"GBIF_{DATASET_NAME}_filtered_occurrences.csv"

OUT_DIR = RESULTS_DIR / DATASET_NAME
OUT_DIR.mkdir(parents=True, exist_ok=True)

SPECIES_FILE = OUT_DIR / f"GBIF_{DATASET_NAME}_species_summary.csv"
COUNTRY_FILE = OUT_DIR / f"GBIF_{DATASET_NAME}_country_summary.csv"
YEAR_FILE    = OUT_DIR / f"GBIF_{DATASET_NAME}_year_summary.csv"
BASIS_FILE   = OUT_DIR / f"GBIF_{DATASET_NAME}_basis_summary.csv"

# ----------------------------------------------------------------------
# Load filtered dataset
# ----------------------------------------------------------------------
import zipfile

if not FILTERED_ZIP.exists():
    raise FileNotFoundError(f"Filtered ZIP not found: {FILTERED_ZIP}")

print(f"Loading filtered dataset from ZIP: {FILTERED_ZIP}")

with zipfile.ZipFile(FILTERED_ZIP, "r") as z:
    if INNER_CSV not in z.namelist():
        raise FileNotFoundError(f"{INNER_CSV} not found inside ZIP.")
    with z.open(INNER_CSV) as f:
        df = pd.read_csv(f, low_memory=False, dtype={"year": "Int64"})

print(f"Loaded {len(df)} records with {len(df.columns)} columns.")

# ----------------------------------------------------------------------
# Species summary
# ----------------------------------------------------------------------
if "scientificName" in df.columns:
    species_summary = (
        df.groupby("scientificName")
        .size()
        .reset_index(name="occurrences")
        .sort_values("occurrences", ascending=False)
    )
    species_summary.to_csv(SPECIES_FILE, index=False)
    print(f"Species summary saved to: {SPECIES_FILE}")

# ----------------------------------------------------------------------
# Country summary
# ----------------------------------------------------------------------
if "countryCode" in df.columns:
    country_summary = (
        df.groupby("countryCode")
        .size()
        .reset_index(name="occurrences")
        .sort_values("occurrences", ascending=False)
    )
    country_summary.to_csv(COUNTRY_FILE, index=False)
    print(f"Country summary saved to: {COUNTRY_FILE}")

# ----------------------------------------------------------------------
# Year summary
# ----------------------------------------------------------------------
if "year" in df.columns:
    year_summary = (
        df.groupby("year")
        .size()
        .reset_index(name="occurrences")
        .sort_values("year")
    )
    year_summary.to_csv(YEAR_FILE, index=False)
    print(f"Year summary saved to: {YEAR_FILE}")

# ----------------------------------------------------------------------
# Basis of record summary
# ----------------------------------------------------------------------
if "basisOfRecord" in df.columns:
    basis_summary = (
        df.groupby("basisOfRecord")
        .size()
        .reset_index(name="occurrences")
        .sort_values("occurrences", ascending=False)
    )
    basis_summary.to_csv(BASIS_FILE, index=False)
    print(f"Basis of record summary saved to: {BASIS_FILE}")

print("Aggregation complete.")