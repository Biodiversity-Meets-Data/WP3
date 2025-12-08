'''
Script: 01_download_gbif.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Notes:
This script automates the download of species occurrence data from the GBIF API.
It reads a species list (CSV file) containing usageKey or acceptedUsageKey values,
constructs a JSON predicate based on taxonomic, spatial, and data-quality filters,
and submits a download request to the GBIF platform.

Purpose:
To generate GBIF Darwin Core (DwC-A) datasets for specific European species lists.
The script:
- Combines acceptedUsageKey and usageKey to create a valid taxon key list.
- Applies filters for European countries, coordinate availability, and record quality.
- Monitors the GBIF download job until completion.
- Automatically retrieves and stores the final ZIP archive for further analysis
  (e.g., bias surface creation, data harmonisation, or temporal-spatial studies).
'''


# ----------------------------------------------------------------------
# Imports
# ----------------------------------------------------------------------
# Standard library and third-party modules required for:
# - interacting with the GBIF HTTP API (requests),
# - working with dates and timestamps,
# - reading the species list (pandas),
# - handling filesystem paths in a portable way (pathlib).

import requests
import time
import pandas as pd
import os
import sys
from datetime import datetime
from pathlib import Path


# ----------------------------------------------------------------------
# Project-level paths (BMD structure)
# ----------------------------------------------------------------------
# Derive the root directory of the BMD project from the location of this
# script. All data and configuration paths are built relative to this root
# to ensure that the script is portable across machines and environments.

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]  # .../BMD

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
CONFIG_DIR = PROJECT_ROOT / "src" / "core" / "config"

# Ensure directories exist
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------
# GBIF authentication (environment-based)
# ----------------------------------------------------------------------
# GBIF credentials are read from environment variables:
#   - GBIF_USER
#   - GBIF_PASSWORD
#   - GBIF_EMAIL
# This avoids hard-coding sensitive information in the source code and
# makes the script safe to share and version-control (e.g. on GitHub).

USERNAME = os.getenv("GBIF_USER")
PASSWORD = os.getenv("GBIF_PASSWORD")
EMAIL = os.getenv("GBIF_EMAIL")

if not USERNAME or not PASSWORD or not EMAIL:
    sys.exit("GBIF credentials not set. Please define GBIF_USER, GBIF_PASSWORD, GBIF_EMAIL as environment variables.")


# ----------------------------------------------------------------------
# Input species list
# ----------------------------------------------------------------------
# Species list provided as a CSV file with at least the columns:
#   - usageKey
#   - acceptedUsageKey
# It is stored under src/core/config and can be swapped to target
# different directive lists (Birds, Habitats, IAS, test subsets, etc.).
# IAS: src/core/config/ias_gbif_species_list.csv
# Birds Directive Annex I: src/core/config/birds_directive_annexi+gbif.csv
# Habitats Directive Annex II: src/core/config/habitats_directive_annexii+gbif.csv

SPECIES_FILE = CONFIG_DIR / "birds_directive_annexi+gbif.csv"


# ----------------------------------------------------------------------
# Download output configuration
# ----------------------------------------------------------------------
# The output ZIP filename is timestamped (YYYYMMDD) to ensure that each
# GBIF download job is stored as a distinct archive under data/raw, making
# it easy to track and reproduce specific runs of the pipeline.

# Output filename (timestamped)
DATE = datetime.now().strftime("%Y%m%d")
OUTPUT_FILE = RAW_DIR / f"GBIF_EU_Download_{DATE}.zip"


# ----------------------------------------------------------------------
# Load species list and build TAXON_KEY set
# ----------------------------------------------------------------------
# Read the species list and derive the taxonKey values used in the GBIF
# predicate. When an acceptedUsageKey is available it is preferred over
# usageKey to ensure taxonomic consistency. Non-numeric or missing keys
# are removed before constructing the final list of TAXON_KEY filters.

species_df = pd.read_csv(SPECIES_FILE)

# Combine acceptedUsageKey (preferred) and usageKey (fallback)
species_df["key_to_use"] = species_df["acceptedUsageKey"].fillna(species_df["usageKey"])

# Clean keys (remove NaN, cast to int, unique)
species_df = species_df[pd.to_numeric(species_df["key_to_use"], errors="coerce").notnull()]
usage_keys = species_df["key_to_use"].astype(int).unique().tolist()

print("Number of taxon keys:", len(usage_keys))
print("Example keys:", usage_keys[:10])

if not usage_keys:
    sys.exit("No valid taxon keys found. Check your CSV file columns.")

# === EUROPEAN COUNTRIES (ISO2 codes) ===
EU_COUNTRIES = [
    "AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","GR","HU",
    "IE","IT","LV","LT","LU","MT","NL","PL","PT","RO","SK","SI","ES","SE",
    "IS","NO","CH","LI","GB"
]

# === BASIS OF RECORD ===
BOR = ["HUMAN_OBSERVATION", "MACHINE_OBSERVATION", "PRESERVED_SPECIMEN"]


# ----------------------------------------------------------------------
# GBIF download predicate
# ----------------------------------------------------------------------
# Construct the JSON predicate used by the GBIF occurrence download API.
# The predicate combines:
#   - an OR block over all TAXON_KEY values from the species list,
#   - a filter on European countries (EU + associated states),
#   - coordinate availability and geospatial quality filters,
#   - a maximum coordinate uncertainty threshold,
#   - a restriction to selected basisOfRecord values.
# The resulting download is a Darwin Core Archive (DwC-A) in ZIP format.

predicate = {
    "creator": USERNAME,
    "notificationAddresses": [EMAIL],
    "sendNotification": True,
    "format": "DWCA",
    "predicate": {
        "type": "and",
        "predicates": [
            {
                # TAXON_KEY filter (multiple equals inside OR)
                "type": "or",
                "predicates": [
                    {"type": "equals", "key": "TAXON_KEY", "value": int(k)}
                    for k in usage_keys
                ]
            },
            {
                "type": "in",
                "key": "COUNTRY",
                "values": EU_COUNTRIES
            },
            {
                "type": "equals",
                "key": "HAS_COORDINATE",
                "value": "TRUE"
            },
            {
                "type": "equals",
                "key": "HAS_GEOSPATIAL_ISSUE",
                "value": "FALSE"
            },
            {
                "type": "lessThan",
                "key": "COORDINATE_UNCERTAINTY_IN_METERS",
                "value": "1000"
            },
            {
                "type": "in",
                "key": "BASIS_OF_RECORD",
                "values": BOR
            }
        ]
    }
}


# ----------------------------------------------------------------------
# Submit download request and retrieve archive
# ----------------------------------------------------------------------
# 1. Submit the occurrence download request to the GBIF API.
# 2. Poll the download status until it reaches the SUCCEEDED state
#    (or abort if it fails).
# 3. Once ready, stream the resulting ZIP archive to data/raw for
#    downstream processing in the BMD pipeline.

print("\nSubmitting GBIF download request...")

# Submit response
response = requests.post(
    "https://api.gbif.org/v1/occurrence/download/request",
    json=predicate, auth=(USERNAME, PASSWORD)
)

if response.status_code == 420:
    sys.exit("Too many active downloads. Wait for some to finish in your GBIF profile.")
elif response.status_code != 201:
    print("Error submitting download:", response.status_code, response.text)
    sys.exit()

key = response.text.strip()
print("Download request submitted. Key:", key)

status_url = f"https://api.gbif.org/v1/occurrence/download/{key}"
print("Waiting for GBIF to process download...")

# Poll job status
while True:
    status = requests.get(status_url).json()
    state = status.get("status")
    print("Status:", state)
    if state == "SUCCEEDED":
        print("Download ready.")
        break
    elif state == "FAILED":
        sys.exit("GBIF download failed.")
    time.sleep(20)

# Download the resulting ZIP archive
file_url = f"https://api.gbif.org/v1/occurrence/download/request/{key}.zip"
print("Downloading data from:", file_url)

r = requests.get(file_url, stream=True)
with open(OUTPUT_FILE, "wb") as f:
    for chunk in r.iter_content(chunk_size=8192):
        if chunk:
            f.write(chunk)

print("\nDownload complete.")
print("Saved GBIF DwC-A archive to:", OUTPUT_FILE.resolve())
