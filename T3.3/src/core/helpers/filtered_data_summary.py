'''
Script: GBIF_filtered_data_summary.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces 

Notes:
This script loads the filtered GBIF dataset (CSV or ZIP) 
and generates separate summary tables by key attributes:
- occurrences per species
- occurrences per country
- occurrences per year
- occurrences per basis of record

Purpose:
To split and aggregate the filtered dataset into structured summary values
for quick analysis and visualization.
'''

import pandas as pd
import zipfile
import os

# ============================================================
# USER SETTINGS
# ============================================================

##Datasets: _BIRDS_ // _HABITATS_ // _IAS_

ZIP_FILE = "Filtered_datasets/GBIF_IAS_filtered_occurrences.zip"   # the ZIP file containing the filtered CSV
CSV_INSIDE_ZIP = "GBIF_IAS_filtered_occurrences.csv"  # CSV filename inside the zip
SPECIES_FILE = "GBIF_IAS_species_summary.csv"
COUNTRY_FILE = "GBIF_IAS_country_summary.csv"
YEAR_FILE = "GBIF_IAS_year_summary.csv"
BASIS_FILE = "GBIF_IAS_basis_summary.csv"

# ============================================================
# LOAD DATA FROM ZIP
# ============================================================

if not os.path.exists(ZIP_FILE):
    raise FileNotFoundError(f"{ZIP_FILE} not found. Please check the path.")

print("Loading filtered dataset from ZIP...")

with zipfile.ZipFile(ZIP_FILE, "r") as z:
    if CSV_INSIDE_ZIP not in z.namelist():
        raise FileNotFoundError(f"{CSV_INSIDE_ZIP} not found inside {ZIP_FILE}.")
    with z.open(CSV_INSIDE_ZIP) as f:
        df = pd.read_csv(f, low_memory=False,  dtype={"year": "Int64"})

print(f"Loaded {len(df)} records with {len(df.columns)} columns\n")

# Show a quick preview of the dataset
pd.set_option("display.max_columns", None)
print("Preview of filtered dataset (first 5 rows):")
print(df.head(5).to_string(index=False))
print("\n")

# ============================================================
# SPECIES SUMMARY
# ============================================================

if "scientificName" in df.columns:
    species_summary = (
        df.groupby("scientificName")
        .size()
        .reset_index(name="occurrences")
        .sort_values("occurrences", ascending=False)
    )
    species_summary.to_csv(SPECIES_FILE, index=False)
    print(f"Species summary saved to: {os.path.abspath(SPECIES_FILE)}")

# ============================================================
# COUNTRY SUMMARY
# ============================================================

if "countryCode" in df.columns:
    country_summary = (
        df.groupby("countryCode")
        .size()
        .reset_index(name="occurrences")
        .sort_values("occurrences", ascending=False)
    )
    country_summary.to_csv(COUNTRY_FILE, index=False)
    print(f"Country summary saved to: {os.path.abspath(COUNTRY_FILE)}")

# ============================================================
# YEAR SUMMARY
# ============================================================

if "year" in df.columns:
    year_summary = (
        df.groupby("year")
        .size()
        .reset_index(name="occurrences")
        .sort_values("year")
    )
    year_summary.to_csv(YEAR_FILE, index=False)
    print(f"Year summary saved to: {os.path.abspath(YEAR_FILE)}")

# ============================================================
# BASIS OF RECORD SUMMARY (OPTIONAL)
# ============================================================

if "basisOfRecord" in df.columns:
    basis_summary = (
        df.groupby("basisOfRecord")
        .size()
        .reset_index(name="occurrences")
        .sort_values("occurrences", ascending=False)
    )
    basis_summary.to_csv(BASIS_FILE, index=False)
    print(f"Basis of Record summary saved to: {os.path.abspath(BASIS_FILE)}")

print("\nAggregation complete.")
