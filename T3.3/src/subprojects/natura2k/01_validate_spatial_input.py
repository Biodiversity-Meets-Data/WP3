'''
Script: 01_validate_spatial_input.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Description:
Validates a filtered GBIF occurrence dataset before spatial processing.
Checks required fields, missing coordinates, and coordinate ranges.
Generates a validation log file for reproducibility.

Input:
- Filtered GBIF dataset (CSV or CSV inside ZIP)

Output:
- Log file with validation messages
- Console summary of validation results

Notes:
This step does not modify the dataset. It only validates structure and
coordinate integrity. All dataset types (Birds, Habitats, IAS) are supported.
# This validation is specific to spatial analyses (Natura pipeline).
# It does not cover temporal or other analytical dimensions.
'''


import pandas as pd
import zipfile
import logging
import os
from pathlib import Path
from datetime import datetime

# ----------------------------------------------------------------------
# Project-level paths
# ----------------------------------------------------------------------
# Resolve the root directory of the BMD Implementation project based on
# the location of this file. All data and log paths are derived from this
# root to ensure portability across machines and environments.

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]  # .../BMD Implementation
DATA_DIR = PROJECT_ROOT / "data"
FILTERED_DIR = DATA_DIR / "filtered"
LOG_DIR = PROJECT_ROOT / "logs" / "natura2k" / "01_validate_spatial_input"
LOG_DIR.mkdir(parents=True, exist_ok=True)


# ----------------------------------------------------------------------
# Dataset configuration
# ----------------------------------------------------------------------
# DATASET_NAME identifies which filtered GBIF dataset is being validated.
# The ZIP file is expected to have been produced by the core filtering
# pipeline (02_filter_gbif_dataset.py) and stored under data/filtered.
# Only the file naming pattern should change when switching between
# IAS / BIRDS / HABITATS.

DATASET_NAME = "HABITATS"  # "IAS", "BIRDS", "HABITATS"
ZIP_FILENAME = f"GBIF_{DATASET_NAME}_filtered_occurrences.zip"  # όνομα αρχείου μέσα στο data/filtered
ZIP_PATH = FILTERED_DIR / DATASET_NAME /  ZIP_FILENAME

# Zip contains csv with the same name .csv:
INNER_CSV = ZIP_FILENAME.replace(".zip", ".csv")

# Column names in this dataset (as they exist in the CSV)
COL_SCI_NAME   = "scientificName"      # change if different
COL_SPECIES_KEY = "taxonKey"        # or "taxonKey" etc.
COL_LONGITUDE  = "decimalLongitude"   # or "longitude"
COL_LATITUDE   = "decimalLatitude"    # or "latitude"

# ----------------------------------------------------------------------
# Logging configuration
# ----------------------------------------------------------------------
# Configure a dedicated logger for dataset validation.
# All messages are written both to stdout and to a rotating log file
# under logs/natura2k. Existing handlers are cleared to avoid duplicated
# log entries if the module is imported multiple times.

timestamp = datetime.now().strftime("%Y%m%d_%H%M")
logfile = LOG_DIR / f"{DATASET_NAME}_spatial_validation_{timestamp}.log"
logger = logging.getLogger("bmd.natura2k.spatial_validation")
logger.setLevel(logging.INFO)
if logger.hasHandlers():
    logger.handlers.clear()

file_handler = logging.FileHandler(logfile, mode="a", encoding="utf-8")
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(message)s')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

def log(msg: str):
    """
    Helper function to send a validation message both to the console
    and to the configured logger. This ensures that all checks are
    reproducible and traceable from the dataset-specific log file
    created under logs/natura2k/01_validate_spatial_input.
    """
    print(msg)
    logger.info(msg)






# ----------------------------------------------------------------------
# Load filtered occurrence dataset
# ----------------------------------------------------------------------
# Open the filtered GBIF dataset from the ZIP archive and load the
# occurrence table into memory. At this stage we assume that the file
# has already passed the core filtering step (basisOfRecord, coordinate
# uncertainty, etc.) and we focus exclusively on spatial consistency.

with zipfile.ZipFile(str(ZIP_PATH)) as z:
    with z.open(INNER_CSV) as f:
        df = pd.read_csv(f)

print(f"Dataset: {DATASET_NAME}")
print("Shape:", df.shape)

log("=== Dataset validation started ===")
log(f"Dataset shape: {df.shape}")
log(f"Scientific name column: {COL_SCI_NAME}")
log(f"Species key column:     {COL_SPECIES_KEY}")
log(f"Longitude column:       {COL_LONGITUDE}")
log(f"Latitude column:        {COL_LATITUDE}")


# ----------------------------------------------------------------------
# Missing coordinate values
# ----------------------------------------------------------------------
# Check for records without longitude or latitude. These records cannot
# be used in any spatial analysis and will be removed in the downstream
# cleaning step.

missing_lon = df[COL_LONGITUDE].isna().sum()
missing_lat = df[COL_LATITUDE].isna().sum()

log("\n[Missing values]")
log(f"Missing {COL_LONGITUDE}: {missing_lon}")
log(f"Missing {COL_LATITUDE}:  {missing_lat}")


# ----------------------------------------------------------------------
# Coordinate range validation
# ----------------------------------------------------------------------
# Validate that all coordinate values fall within the valid geographic
# ranges:
#   - latitude in [-90, 90]
#   - longitude in [-180, 180]
# Any out-of-range values indicate corrupted or misprojected records
# and must be handled before spatial joins with Natura 2000 polygons.
lat_range_ok = df[COL_LATITUDE].between(-90, 90).all()
lon_range_ok = df[COL_LONGITUDE].between(-180, 180).all()

log("\n[Coordinate range validation]")
log(f"Latitude in [-90, 90]:    {lat_range_ok}")
log(f"Longitude in [-180, 180]: {lon_range_ok}")



# ----------------------------------------------------------------------
# Summary and readiness for spatial processing
# ----------------------------------------------------------------------
# Aggregate the results of the checks into a short textual summary.
# This summary documents whether the dataset is ready to be converted
# into a GeoDataFrame and used in spatial joins.

log("\n[Summary]")

if missing_lon == 0 and missing_lat == 0:
    log("No missing coordinate values.")
else:
    log("Missing coordinate values detected (these rows will be removed).")

if lat_range_ok and lon_range_ok:
    log("Coordinate ranges are valid.")
else:
    log("Invalid coordinate values detected (these rows will be cleaned).")

log("Dataset is structurally ready: GeoDataFrame conversion and spatial join.")
log("=== Dataset validation completed ===")






