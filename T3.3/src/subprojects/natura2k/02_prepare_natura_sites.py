"""
Script: 02_prepare_natura_sites.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Description:
Loads the official Natura 2000 GeoPackage, inspects its structure,
reprojects the geometries to WGS84 (EPSG:4326) and saves a cleaned,
analysis-ready layer with the key site attributes.

Input:
- Natura 2000 GeoPackage (e.g. Natura2000_end2022.gpkg) located in:
  data/external/natura2000/

Output:
- Cleaned Natura 2000 sites layer in WGS84:
  data/external/natura2000/Natura2000_sites_prepared.gpkg

Notes:
This script prepares site geometries for spatial joins with GBIF
occurrence data, which are stored in WGS84 (EPSG:4326).
"""

from pathlib import Path
import geopandas as gpd
import logging
from datetime import datetime

# ----------------------------------------------------------------------
# Project-level paths
# ----------------------------------------------------------------------
# Current file: .../BMD Implementation/src/subprojects/natura2k/02_prepare_natura_sites.py
# Project root: .../BMD Implementation   → parents[4]

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]

DATA_DIR = PROJECT_ROOT / "data"
EXTERNAL_DIR = DATA_DIR / "external"
NATURA_DIR = EXTERNAL_DIR / "natura2k"

# Input: raw Natura 2000 GeoPackage (as downloaded from EEA)
NATURA_GPKG = NATURA_DIR / "Natura2000_end2022.gpkg"

# Output: cleaned & reprojected Natura sites
OUTPUT_GPKG = NATURA_DIR / "Natura2000_sites_prepared.gpkg"

def rel(path: Path) -> str:
    """
    Return path as a string, relative to the project root.
    Used only for logging, so that absolute disk paths
    (e.g. /Users/...) do not appear in log files.
    """
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        # αν για κάποιο λόγο δεν είναι κάτω από το PROJECT_ROOT,
        # γύρνα το όπως είναι (ασφαλές fallback)
        return str(path)

# ----------------------------------------------------------------------
# Logging setup
# ----------------------------------------------------------------------
LOG_DIR = PROJECT_ROOT / "logs" / "natura2k" / "02_prepare_natura_sites"
LOG_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M")
logfile = LOG_DIR / f"prepare_natura_{timestamp}.log"

logger = logging.getLogger("prepare_natura")
logger.setLevel(logging.INFO)

# clear old handlers (avoid duplicates in interactive use)
if logger.hasHandlers():
    logger.handlers.clear()

file_handler = logging.FileHandler(logfile, mode="a", encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

def log(msg):
    print(msg)
    logger.info(msg)

log(f"Project root:        .")
log(f"Natura input GPKG:   {rel(NATURA_GPKG)}")
log(f"Natura output GPKG:  {rel(OUTPUT_GPKG)}")

# ----------------------------------------------------------------------
# Load Natura 2000 GeoPackage
# ----------------------------------------------------------------------
if not NATURA_GPKG.exists():
    raise FileNotFoundError(f"Natura GeoPackage not found: {NATURA_GPKG}")

log("\nLoading Natura 2000 GeoPackage...")
gdf = gpd.read_file(NATURA_GPKG)

log("Loaded Natura layer.")
log(f"Number of sites: {len(gdf)}")
log(f"CRS: {gdf.crs}")
log("\nColumns:")
log(list(gdf.columns))

print("\nFirst 5 rows (for inspection):")
print(gdf.head())

# ----------------------------------------------------------------------
# Reproject to WGS84 (EPSG:4326) for compatibility with GBIF points
# ----------------------------------------------------------------------
if gdf.crs is None:
    raise ValueError("Natura layer has no CRS defined. Please check the source file.")

if gdf.crs.to_epsg() != 4326:
    log("\nReprojecting Natura sites to WGS84 (EPSG:4326)...")
    gdf = gdf.to_crs(epsg=4326)
    log("Reprojection completed.")
else:
    log("\nNatura layer is already in EPSG:4326.")

# ----------------------------------------------------------------------
# Select key columns
# ----------------------------------------------------------------------
# IMPORTANT:
# After the first run, check the printed column names and update
# these constants to match the actual schema.

SITE_CODE_COL = "SITECODE"   # Adjust after first run if needed
NAME_COL      = "SITENAME"
COUNTRY_COL   = "COUNTRY"
TYPE_COL      = "SITETYPE"

missing_cols = [
    col for col in [SITE_CODE_COL, NAME_COL, COUNTRY_COL, TYPE_COL]
    if col not in gdf.columns
]

if missing_cols:
    log("\nWARNING: Some expected columns are missing:")
    for c in missing_cols:
        log(f"  - {c}")
    log("Keeping all columns for now. Update the constants above after inspection.")
    gdf_clean = gdf.copy()
else:
    keep_cols = [SITE_CODE_COL, NAME_COL, COUNTRY_COL, TYPE_COL, "geometry"]
    gdf_clean = gdf[keep_cols].copy()

log(f"\nCleaned GeoDataFrame shape: {gdf_clean.shape}")

# ----------------------------------------------------------------------
# Save cleaned Natura sites layer
# ----------------------------------------------------------------------
NATURA_DIR.mkdir(parents=True, exist_ok=True)

log(f"\nSaving cleaned Natura sites to: {OUTPUT_GPKG}")
gdf_clean.to_file(
    OUTPUT_GPKG,
    layer="natura_sites_epsg4326",
    driver="GPKG"
)

log("\nDone. Cleaned Natura 2000 sites layer is ready for spatial joins.")