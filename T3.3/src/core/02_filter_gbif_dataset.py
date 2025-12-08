"""
Script: 02_filter_gbif_dataset.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Current version: v2_251203
Old versions:
- v1_251106

Description:
This script processes raw GBIF occurrence data (provided as a zipped Darwin Core archive).
It reads occurrence records in memory-efficient chunks, applies data quality filters,
and produces a filtered dataset along with a statistical summary report.

Input:
- Occurrence from Raw downloaded GBIF data zip file

Output:
- GBIF Filtered dataset

Purpose:
To generate a clean, analysis-ready GBIF dataset by:
- Removing incomplete or low-quality records (missing coordinates, invalid taxon names).
- Filtering by basis of record and spatial precision thresholds (<1000 m uncertainty).
- Computing key statistics: species richness, country coverage, temporal distribution,
  basis of record proportions, and spatial extent.
The resulting files support downstream analyses such as bias detection,
data gap assessment, or species distribution modelling.

UPDATES:
v2
--Added optional spatial (lat/lon) and temporal (year) filters, with strict removal of invalid years.
--Rebuilt mask logic to avoid index misalignment and updated report to reflect only fully filtered data.
--Added full parameter/value reporting in the summary report.

"""


# ----------------------------------------------------------------------
# Imports
# ----------------------------------------------------------------------
# Standard library and third-party modules used for:
# - reading and processing tabular occurrence data (pandas),
# - accessing the Darwin Core Archive inside the GBIF ZIP (zipfile),
# - counting occurrences for summary statistics (collections.Counter),
# - managing filesystem paths in a portable way (pathlib).

import pandas as pd
import zipfile
import os
from collections import Counter
from datetime import datetime
from pathlib import Path


# ----------------------------------------------------------------------
# Project-level paths (BMD structure)
# ----------------------------------------------------------------------
# Resolve the root of the BMD Implementation project based on the
# location of this script. Input and output paths are then derived
# relative to this root directory.

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]  # .../BMD Implementation
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
FILTERED_DIR = DATA_DIR / "filtered"
FILTERED_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR = PROJECT_ROOT / "results" / "filtering"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------
# User configuration
# ----------------------------------------------------------------------
# Define which GBIF download to process (IAS, BIRDS, HABITATS) and
# construct the corresponding input and output filenames. The ZIP file
# is expected to be the raw Darwin Core Archive produced by the download
# script (01_download_gbif.py) and stored under data/raw.

DATASET_NAME = "IAS"   # BIRDS or HABITATS or IAS

ZIP_FILE = RAW_DIR / f"GBIF_{DATASET_NAME}_251106.zip"
REPORT_FILE = RESULTS_DIR / f"GBIF_{DATASET_NAME}_summary_report.txt"
FILTERED_FILE = FILTERED_DIR / f"GBIF_{DATASET_NAME}_filtered_occurrences.csv"

CHUNK_SIZE = 100_000  # rows per chunk

# ----------------------------------------------------------------------
# Columns and data-quality filters
# ----------------------------------------------------------------------
# COLUMNS_KEEP defines the subset of GBIF Darwin Core fields that are
# required for downstream analyses. ALLOWED_BASIS restricts records to
# specific basisOfRecord values (e.g. human observations, specimens),
# excluding records that are not appropriate for spatial/temporal bias
# assessment or species distribution modelling.

COLUMNS_KEEP = [
    "taxonKey",
    "scientificName",
    "decimalLatitude",
    "decimalLongitude",
    "countryCode",
    "basisOfRecord",
    "coordinateUncertaintyInMeters",
    "year",
    "month",
    "eventDate"
]

ALLOWED_BASIS = ["HUMAN_OBSERVATION", "MACHINE_OBSERVATION", "PRESERVED_SPECIMEN"]

# ----------------------------------------------------------------------
# Counters and aggregation structures
# ----------------------------------------------------------------------
# Initialize counters for:
#   - species occurrences (scientificName),
#   - taxonKey counts,
#   - country-level coverage,
#   - basisOfRecord distribution,
#   - temporal coverage by year.
# These are used to build a summary report for the filtered dataset.

# ============================================================
# INITIALIZE COUNTERS AND STORAGE
# ============================================================

species_counter = Counter()
taxon_counter = Counter()
country_counter = Counter()
basis_counter = Counter()
year_counter = Counter()

total_records = 0
filtered_records = 0
filtered_data = []

# ----------------------------------------------------------------------
# Optional spatial and temporal filters
# ----------------------------------------------------------------------
# Spatial filters (LAT_MIN, LAT_MAX, LON_MIN, LON_MAX) allow restricting
# the dataset to a specific bounding box (e.g. a study region). Temporal
# filters (YEAR_MIN, YEAR_MAX) constrain the dataset to a given period.
# When all are set to None, the script performs only the core data-quality
# filtering without additional spatial/temporal restriction.

# Optional spatial filters (if set to None, no spatial filtering is applied)
LAT_MIN = None   # e.g. 34.0
LAT_MAX = None   # e.g. 42.0
LON_MIN = None   # e.g. 19.0
LON_MAX = None   # e.g. 29.0

# Optional temporal filters (if set to None, no year filtering is applied)
YEAR_MIN = None  # π.χ. 1900
YEAR_MAX = None  # π.χ. 2020

# ----------------------------------------------------------------------
# Read GBIF occurrence data in chunks (memory-efficient)
# ----------------------------------------------------------------------
# The occurrence table (occurrence.txt) inside the DwC-A ZIP is read in
# chunks to avoid loading the full dataset into memory. For each chunk
# we:
#   1. Drop rows missing critical fields (species name, key, coordinates),
#   2. Convert numeric fields to appropriate dtypes,
#   3. Apply mandatory data-quality filters (basisOfRecord, coordinate
#      uncertainty),
#   4. Optionally apply spatial and temporal filters,
#   5. Update summary counters and collect the filtered subset.

with zipfile.ZipFile(str(ZIP_FILE), "r") as z:
    if "occurrence.txt" not in z.namelist():
        raise FileNotFoundError("occurrence.txt not found in the ZIP file.")
    with z.open("occurrence.txt") as f:
        for chunk in pd.read_csv(f, sep="\t", usecols=lambda c: c in COLUMNS_KEEP,
                                 chunksize=CHUNK_SIZE, low_memory=False):

            total_records += len(chunk)

            # 1. Drop rows missing critical fields (these must always exist)
            chunk = chunk.dropna(
                subset=["scientificName", "taxonKey", "decimalLatitude", "decimalLongitude"]
            )
            
            # 2. Convert fields to numeric where needed
            chunk["decimalLatitude"] = pd.to_numeric(chunk["decimalLatitude"], errors="coerce")
            chunk["decimalLongitude"] = pd.to_numeric(chunk["decimalLongitude"], errors="coerce")
            chunk["coordinateUncertaintyInMeters"] = pd.to_numeric(
                chunk["coordinateUncertaintyInMeters"], errors="coerce"
            )
            chunk["year"] = pd.to_numeric(chunk["year"], errors="coerce")
            
            # 3. If year filtering is active, drop rows without a valid numerical year
            if YEAR_MIN is not None or YEAR_MAX is not None:
                chunk = chunk.dropna(subset=["year"])
            
            # 4. Build boolean mask and apply mandatory + optional filters
            mask = pd.Series(True, index=chunk.index)
            
            # ------------------------------------------------
            # Mandatory filters
            # ------------------------------------------------
            
            # basisOfRecord filter
            mask &= chunk["basisOfRecord"].isin(ALLOWED_BASIS)
            
            # coordinateUncertainty filter
            mask &= chunk["coordinateUncertaintyInMeters"].fillna(0) < 1000
            
            # ------------------------------------------------
            # Optional spatial filtering
            # ------------------------------------------------
            if any(v is not None for v in [LAT_MIN, LAT_MAX, LON_MIN, LON_MAX]):
                if LAT_MIN is not None:
                    mask &= chunk["decimalLatitude"] >= LAT_MIN
                if LAT_MAX is not None:
                    mask &= chunk["decimalLatitude"] <= LAT_MAX
                if LON_MIN is not None:
                    mask &= chunk["decimalLongitude"] >= LON_MIN
                if LON_MAX is not None:
                    mask &= chunk["decimalLongitude"] <= LON_MAX
            
            # ------------------------------------------------
            # Optional temporal filtering (year)
            # ------------------------------------------------
            if YEAR_MIN is not None:
                mask &= chunk["year"] >= YEAR_MIN
            
            if YEAR_MAX is not None:
                mask &= chunk["year"] <= YEAR_MAX
            
            # 5. Apply mask to obtain filtered subset
            filtered_chunk = chunk[mask]
            
            # 6. Update counters AFTER filtering
            if not filtered_chunk.empty:
                species_counter.update(filtered_chunk["scientificName"])
                taxon_counter.update(filtered_chunk["taxonKey"])
                country_counter.update(filtered_chunk["countryCode"].dropna())
                basis_counter.update(filtered_chunk["basisOfRecord"].dropna())
                year_counter.update(filtered_chunk["year"].dropna())
            
            # 7. Collect filtered chunk for final concatenation
            filtered_records += len(filtered_chunk)
            filtered_data.append(filtered_chunk)


# ----------------------------------------------------------------------
# Combine filtered chunks and persist dataset
# ----------------------------------------------------------------------
# Concatenate all filtered chunks into a single DataFrame and write the
# result to CSV under data/filtered. If no records pass the filters, only
# a console message is printed and no output file is created.

if filtered_data:
    filtered_df = pd.concat(filtered_data, ignore_index=True)
    filtered_df.to_csv(FILTERED_FILE, index=False)
else:
    print("No data after filtering.")
    



# ----------------------------------------------------------------------
# Generate textual summary report
# ----------------------------------------------------------------------
# Build a human-readable summary of:
#   - filter configuration and parameter values,
#   - record retention ratio,
#   - species and taxonKey richness,
#   - temporal coverage (year range and distribution),
#   - geographic coverage (countries),
#   - basisOfRecord distribution,
#   - basic spatial extent (lat/lon ranges).
# The report is written to a plain text file alongside the filtered dataset.

with open(REPORT_FILE, "w", encoding="utf-8") as report:
    report.write("GBIF DATA SUMMARY REPORT\n\n")
    report.write(f"Dataset: {ZIP_FILE}\n")
    #report.write("Generated on: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n")
    report.write("=" * 60 + "\n\n")
    
    
    # ==============================
    # Filter configuration summary
    # ==============================
    report.write("Filter configuration\n")
    report.write("--------------------\n")

    # Required non-null fields
    report.write(
         "Required non-null fields: "
         "scientificName, taxonKey, decimalLatitude, decimalLongitude\n"
    )

    # basisOfRecord filter
    report.write(f"basisOfRecord filter: {ALLOWED_BASIS}\n")
    
    # coordinateUncertainty filter
    report.write(
         "coordinateUncertaintyInMeters filter: "
         "< 1000 m (NaN treated as 0)\n"
    )
    
     # Spatial filters
    if any(v is not None for v in [LAT_MIN, LAT_MAX, LON_MIN, LON_MAX]):
        report.write("Spatial filter (bounding box):\n")
        report.write(f"  LAT_MIN = {LAT_MIN}\n")
        report.write(f"  LAT_MAX = {LAT_MAX}\n")
        report.write(f"  LON_MIN = {LON_MIN}\n")
        report.write(f"  LON_MAX = {LON_MAX}\n")
    else:
        report.write("Spatial filter (bounding box): none (all lat/lon kept)\n")
    
    # Temporal filters
    if YEAR_MIN is not None or YEAR_MAX is not None:
        report.write("Temporal filter (year):\n")
        report.write(f"  YEAR_MIN = {YEAR_MIN}\n")
        report.write(f"  YEAR_MAX = {YEAR_MAX}\n")
    else:
        report.write("Temporal filter (year): none (all years kept)\n")
    
    report.write("\n") 
    
    # ==============================
    # Dataset summary
    # ==============================
    
    report.write(f"Total records (raw): {total_records}\n")
    report.write(f"Records after filtering: {filtered_records}\n")
    report.write(f"Unique species: {len(species_counter)}\n")
    report.write(f"Unique taxonKeys: {len(taxon_counter)}\n\n")

    ratio = (filtered_records / total_records * 100) if total_records > 0 else 0
    report.write(f"Retention ratio after filtering: {ratio:.2f}%\n\n")
    
    # Year range
    if "year" in filtered_df.columns:
        years = filtered_df["year"].dropna()
        if len(years) > 0:
            report.write(f"Year range: {int(years.min())} to {int(years.max())}\n")
        else:
            report.write("Year range: no valid years\n")
            
    # Geographic coverage
    n_countries = len(country_counter)
    report.write(f"Geographic coverage: {n_countries} unique countries\n")

    # Latitude / Longitude range
    if "decimalLatitude" in filtered_df.columns:
        lat = filtered_df["decimalLatitude"]
        lon = filtered_df["decimalLongitude"]

        report.write(
            f"Latitude range: {lat.min():.4f} to {lat.max():.4f}\n"
        )
        report.write(
            f"Longitude range: {lon.min():.4f} to {lon.max():.4f}\n"
        )

    report.write("\n")
    
    
    
    # Basis of Record
    total_basis = sum(basis_counter.values())
    report.write("Basis of Record distribution:\n")
    for b, count in basis_counter.most_common():
        percent = (count / total_basis) * 100 if total_basis > 0 else 0
        report.write(f"  {b}: {count} ({percent:.2f}%)\n")
    report.write("\n")

    # Top species
    report.write("Top 10 species by number of occurrences:\n")
    for name, count in species_counter.most_common(10):
        report.write(f"  {name}: {count}\n")
    report.write("\n")

    # Country summary (sorted by occurrences)
    report.write("Occurrences per country (sorted by number of occurrences):\n")
    for c, count in sorted(country_counter.items(), key=lambda x: x[1], reverse=True):
        report.write(f"  {c}: {count}\n")
    report.write("\n")

    

    # Year distribution (ALL years)
    report.write("Occurrences by year (all years):\n")
    year_counter = {int(float(k)): v for k, v in year_counter.items() if str(k).replace('.', '', 1).isdigit()}
    sorted_years = sorted(year_counter.items(), key=lambda x: x[0])
    for y, count in sorted_years:
        report.write(f"  {int(y)}: {count}\n")
    report.write("\n")

    # Compute and print year range
    if sorted_years:
        min_year = int(min(year_counter.keys()))
        max_year = int(max(year_counter.keys()))
        report.write(f"Data coverage (year range): {min_year}–{max_year}\n\n")

    

    # All species sorted
    report.write("All species sorted by number of occurrences:\n")
    for name, count in species_counter.most_common():
        report.write(f"  {name}: {count}\n")

print(f"Report saved to: {REPORT_FILE.resolve()}")
print(f"Filtered dataset saved to: {FILTERED_FILE.resolve()}")
