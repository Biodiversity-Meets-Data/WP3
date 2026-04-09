"""
Script: 02_filter_gbif_dataset.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Current version: v3_260327
Old versions:
- v2_251203
- v1_251106

Description:
This script processes raw GBIF occurrence data (provided as a zipped Darwin Core archive).
It reads occurrence records in memory-efficient chunks, applies data quality filters,
and produces a filtered dataset along with a statistical summary report.

The gbifID field is preserved throughout the filtering process and serves as the
stable unique occurrence identifier (logical primary key) for all downstream spatial
and analytical modules.

Input:
- Occurrence table from raw downloaded GBIF data ZIP file

Output:
- GBIF filtered dataset (CSV inside ZIP archive)
- Plain-text summary report

Purpose:
To generate a clean, analysis-ready GBIF dataset by:
- removing incomplete or low-quality records
- filtering by basis of record and spatial precision thresholds (<1000 m uncertainty)
- optionally restricting the dataset spatially and/or temporally
- standardising column types before export
- computing key statistics on the final filtered dataset

Updates in v3:
- Added explicit final type standardisation for gbifID, taxonKey, year, and month
- Enforced canonical output schema before export
- Added basic dataset integrity checks before writing output
- Clarified reporting wording ("Unique scientific names" vs "Unique taxonKeys")
- Kept eventDate as raw GBIF temporal field (string/object), without forced datetime parsing
"""

# ----------------------------------------------------------------------
# Imports
# ----------------------------------------------------------------------

import pandas as pd
import zipfile
from collections import Counter
from pathlib import Path


# ----------------------------------------------------------------------
# Project-level paths (BMD structure)
# ----------------------------------------------------------------------

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

DATASET_NAME = "IAS"   # BIRDS or HABITATS or IAS

ZIP_FILE = RAW_DIR / f"GBIF_{DATASET_NAME}_251106.zip"

RESULTS_SUBDIR = RESULTS_DIR / DATASET_NAME
RESULTS_SUBDIR.mkdir(parents=True, exist_ok=True)

REPORT_FILE = RESULTS_SUBDIR / f"GBIF_{DATASET_NAME}_summary_report.txt"

FILTERED_SUBDIR = FILTERED_DIR / DATASET_NAME
FILTERED_SUBDIR.mkdir(parents=True, exist_ok=True)

FILTERED_ZIP = FILTERED_SUBDIR / f"GBIF_{DATASET_NAME}_filtered_occurrences.zip"
INNER_CSV_NAME = f"GBIF_{DATASET_NAME}_filtered_occurrences.csv"

CHUNK_SIZE = 100_000


# ----------------------------------------------------------------------
# Columns and data-quality filters
# ----------------------------------------------------------------------

COLUMNS_KEEP = [
    "gbifID",
    "taxonKey",
    "scientificName",
    "decimalLatitude",
    "decimalLongitude",
    "countryCode",
    "basisOfRecord",
    "coordinateUncertaintyInMeters",
    "year",
    "month",
    "eventDate",
]

# Canonical output order
OUTPUT_COLUMNS = [
    "gbifID",
    "taxonKey",
    "scientificName",
    "decimalLatitude",
    "decimalLongitude",
    "countryCode",
    "basisOfRecord",
    "coordinateUncertaintyInMeters",
    "year",
    "month",
    "eventDate",
]

ALLOWED_BASIS = ["HUMAN_OBSERVATION", "MACHINE_OBSERVATION", "PRESERVED_SPECIMEN"]


# ----------------------------------------------------------------------
# Counters and aggregation structures
# ----------------------------------------------------------------------

species_counter = Counter()   # based on scientificName
taxon_counter = Counter()     # based on taxonKey
country_counter = Counter()
basis_counter = Counter()
year_counter = Counter()

total_records = 0
filtered_records = 0
filtered_data = []


# ----------------------------------------------------------------------
# Optional spatial and temporal filters
# ----------------------------------------------------------------------

LAT_MIN = None
LAT_MAX = None
LON_MIN = None
LON_MAX = None

YEAR_MIN = None
YEAR_MAX = None


# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------

def standardise_chunk_types(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Standardise data types within a chunk before filtering.
    Numeric conversion is applied only where necessary.
    eventDate is intentionally kept as raw string/object field.
    """
    chunk["gbifID"] = pd.to_numeric(chunk["gbifID"], errors="coerce")
    chunk["taxonKey"] = pd.to_numeric(chunk["taxonKey"], errors="coerce")
    chunk["decimalLatitude"] = pd.to_numeric(chunk["decimalLatitude"], errors="coerce")
    chunk["decimalLongitude"] = pd.to_numeric(chunk["decimalLongitude"], errors="coerce")
    chunk["coordinateUncertaintyInMeters"] = pd.to_numeric(
        chunk["coordinateUncertaintyInMeters"], errors="coerce"
    )
    chunk["year"] = pd.to_numeric(chunk["year"], errors="coerce")
    chunk["month"] = pd.to_numeric(chunk["month"], errors="coerce")
    return chunk


def apply_final_output_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply final semantic typing to the filtered dataset before export.
    Uses pandas nullable integers where missing values may exist.
    """
    # IDs and keys
    df["gbifID"] = pd.to_numeric(df["gbifID"], errors="coerce").astype("Int64")
    df["taxonKey"] = pd.to_numeric(df["taxonKey"], errors="coerce").astype("Int64")

    # Coordinates and uncertainty remain float
    df["decimalLatitude"] = pd.to_numeric(df["decimalLatitude"], errors="coerce")
    df["decimalLongitude"] = pd.to_numeric(df["decimalLongitude"], errors="coerce")
    df["coordinateUncertaintyInMeters"] = pd.to_numeric(
        df["coordinateUncertaintyInMeters"], errors="coerce"
    )

    # Temporal integer fields (nullable)
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["month"] = pd.to_numeric(df["month"], errors="coerce").astype("Int64")

    # Keep eventDate as raw GBIF field
    if "eventDate" in df.columns:
        df["eventDate"] = df["eventDate"].astype("string")

    # String-like columns
    for col in ["scientificName", "countryCode", "basisOfRecord"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    return df


def run_final_integrity_checks(df: pd.DataFrame) -> None:
    """
    Perform minimal integrity checks on the final exported dataset.
    These are safeguards because this dataset is the canonical filtered output.
    """
    if df["gbifID"].isna().any():
        raise ValueError("Final dataset contains missing gbifID values.")

    if df["gbifID"].duplicated().any():
        raise ValueError("Final dataset contains duplicate gbifID values.")

    if df["decimalLatitude"].isna().any() or df["decimalLongitude"].isna().any():
        raise ValueError("Final dataset contains missing coordinates.")

    if not df["decimalLatitude"].between(-90, 90).all():
        raise ValueError("Final dataset contains invalid latitude values.")

    if not df["decimalLongitude"].between(-180, 180).all():
        raise ValueError("Final dataset contains invalid longitude values.")


# ----------------------------------------------------------------------
# Read GBIF occurrence data in chunks (memory-efficient)
# ----------------------------------------------------------------------

with zipfile.ZipFile(str(ZIP_FILE), "r") as z:
    if "occurrence.txt" not in z.namelist():
        raise FileNotFoundError("occurrence.txt not found in the ZIP file.")

    with z.open("occurrence.txt") as f:
        for chunk in pd.read_csv(
            f,
            sep="\t",
            usecols=lambda c: c in COLUMNS_KEEP,
            chunksize=CHUNK_SIZE,
            low_memory=False,
        ):
            total_records += len(chunk)

            # 1. Drop rows missing critical fields
            chunk = chunk.dropna(
                subset=["gbifID", "scientificName", "taxonKey", "decimalLatitude", "decimalLongitude"]
            )

            # 2. Standardise numeric fields
            chunk = standardise_chunk_types(chunk)

            # 3. Drop rows that became invalid after numeric conversion
            chunk = chunk.dropna(
                subset=["gbifID", "taxonKey", "decimalLatitude", "decimalLongitude"]
            )

            # 4. If year filtering is active, require valid numerical year
            if YEAR_MIN is not None or YEAR_MAX is not None:
                chunk = chunk.dropna(subset=["year"])

            # 5. Build boolean mask
            mask = pd.Series(True, index=chunk.index)

            # Mandatory filters
            mask &= chunk["basisOfRecord"].isin(ALLOWED_BASIS)
            mask &= chunk["coordinateUncertaintyInMeters"].fillna(0) < 1000

            # Optional spatial filtering
            if any(v is not None for v in [LAT_MIN, LAT_MAX, LON_MIN, LON_MAX]):
                if LAT_MIN is not None:
                    mask &= chunk["decimalLatitude"] >= LAT_MIN
                if LAT_MAX is not None:
                    mask &= chunk["decimalLatitude"] <= LAT_MAX
                if LON_MIN is not None:
                    mask &= chunk["decimalLongitude"] >= LON_MIN
                if LON_MAX is not None:
                    mask &= chunk["decimalLongitude"] <= LON_MAX

            # Optional temporal filtering
            if YEAR_MIN is not None:
                mask &= chunk["year"] >= YEAR_MIN
            if YEAR_MAX is not None:
                mask &= chunk["year"] <= YEAR_MAX

            # 6. Apply mask
            filtered_chunk = chunk[mask].copy()

            # 7. Update counters AFTER filtering
            if not filtered_chunk.empty:
                species_counter.update(filtered_chunk["scientificName"].dropna())
                taxon_counter.update(filtered_chunk["taxonKey"].dropna())
                country_counter.update(filtered_chunk["countryCode"].dropna())
                basis_counter.update(filtered_chunk["basisOfRecord"].dropna())
                year_counter.update(filtered_chunk["year"].dropna())

            filtered_records += len(filtered_chunk)
            filtered_data.append(filtered_chunk)


# ----------------------------------------------------------------------
# Combine filtered chunks and persist dataset
# ----------------------------------------------------------------------

if filtered_data:
    filtered_df = pd.concat(filtered_data, ignore_index=True)

    # Canonical schema order
    filtered_df = filtered_df[OUTPUT_COLUMNS]

    # Final semantic typing
    filtered_df = apply_final_output_types(filtered_df)

    # Final safeguards
    run_final_integrity_checks(filtered_df)

    # Write CSV into ZIP archive
    with zipfile.ZipFile(FILTERED_ZIP, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(INNER_CSV_NAME, filtered_df.to_csv(index=False))

else:
    print("No data after filtering.")
    filtered_df = pd.DataFrame(columns=OUTPUT_COLUMNS)


# ----------------------------------------------------------------------
# Generate textual summary report
# ----------------------------------------------------------------------

with open(REPORT_FILE, "w", encoding="utf-8") as report:
    report.write("GBIF DATA SUMMARY REPORT\n\n")
    report.write(f"Dataset: {ZIP_FILE}\n")
    report.write("=" * 60 + "\n\n")

    # ==============================
    # Filter configuration summary
    # ==============================
    report.write("Filter configuration\n")
    report.write("--------------------\n")

    report.write(
        "Required non-null fields: "
        "gbifID, scientificName, taxonKey, decimalLatitude, decimalLongitude\n"
    )

    report.write(f"basisOfRecord filter: {ALLOWED_BASIS}\n")
    report.write("coordinateUncertaintyInMeters filter: < 1000 m (NaN treated as 0)\n")

    if any(v is not None for v in [LAT_MIN, LAT_MAX, LON_MIN, LON_MAX]):
        report.write("Spatial filter (bounding box):\n")
        report.write(f"  LAT_MIN = {LAT_MIN}\n")
        report.write(f"  LAT_MAX = {LAT_MAX}\n")
        report.write(f"  LON_MIN = {LON_MIN}\n")
        report.write(f"  LON_MAX = {LON_MAX}\n")
    else:
        report.write("Spatial filter (bounding box): none (all lat/lon kept)\n")

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
    report.write(f"Unique scientific names: {len(species_counter)}\n")
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
    report.write(f"Geographic coverage: {len(country_counter)} unique countries\n")

    # Latitude / Longitude range
    if "decimalLatitude" in filtered_df.columns and "decimalLongitude" in filtered_df.columns:
        lat = filtered_df["decimalLatitude"].dropna()
        lon = filtered_df["decimalLongitude"].dropna()
        if len(lat) > 0 and len(lon) > 0:
            report.write(f"Latitude range: {lat.min():.4f} to {lat.max():.4f}\n")
            report.write(f"Longitude range: {lon.min():.4f} to {lon.max():.4f}\n")

    report.write("\n")

    # Basis of Record
    total_basis = sum(basis_counter.values())
    report.write("Basis of Record distribution:\n")
    for b, count in basis_counter.most_common():
        percent = (count / total_basis) * 100 if total_basis > 0 else 0
        report.write(f"  {b}: {count} ({percent:.2f}%)\n")
    report.write("\n")

    # Top scientific names
    report.write("Top 10 scientific names by number of occurrences:\n")
    for name, count in species_counter.most_common(10):
        report.write(f"  {name}: {count}\n")
    report.write("\n")

    # Country summary
    report.write("Occurrences per country (sorted by number of occurrences):\n")
    for c, count in sorted(country_counter.items(), key=lambda x: x[1], reverse=True):
        report.write(f"  {c}: {count}\n")
    report.write("\n")

    # Year distribution
    report.write("Occurrences by year (all years):\n")
    clean_year_counter = {
        int(k): v for k, v in year_counter.items() if pd.notna(k)
    }
    sorted_years = sorted(clean_year_counter.items(), key=lambda x: x[0])

    for y, count in sorted_years:
        report.write(f"  {y}: {count}\n")
    report.write("\n")

    if sorted_years:
        min_year = min(clean_year_counter.keys())
        max_year = max(clean_year_counter.keys())
        report.write(f"Data coverage (year range): {min_year}–{max_year}\n\n")

    # All scientific names
    report.write("All scientific names sorted by number of occurrences:\n")
    for name, count in species_counter.most_common():
        report.write(f"  {name}: {count}\n")

print(f"Report saved to: {REPORT_FILE.resolve()}")
print(f"Filtered dataset saved to: {FILTERED_ZIP.resolve()}")