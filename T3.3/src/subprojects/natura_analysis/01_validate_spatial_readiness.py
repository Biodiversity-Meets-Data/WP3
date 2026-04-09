"""
Script: natura_analysis/01_validate_spatial_readiness.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Description:
Performs input-level spatial readiness validation for a filtered GBIF
occurrence dataset before downstream Natura 2000 spatial processing.

This script does not generate a new dataset and does not perform any
spatial join. Instead, it checks whether the filtered GBIF table is
structurally and spatially suitable for downstream point creation and
spatial analysis in the Natura pipeline.

Validation covers:
- Required columns
- gbifID presence, completeness, uniqueness
- Coordinate presence and numeric validity
- Coordinate range validity
- Zero / suspicious coordinates
- Duplicate coordinate pairs
- Spatial extent summary
- Coarse study-area plausibility (Europe-oriented bounding box)
- coordinateUncertaintyInMeters presence and validity

Input:
- Filtered GBIF dataset (ZIP containing CSV) from:
  data/filtered/<DATASET_NAME>/GBIF_<DATASET_NAME>_filtered_occurrences.zip

Outputs:
1) Validation log file:
   logs/natura_analysis/01_validate_spatial_input/

2) Validation report table:
   results/natura_analysis/<DATASET_NAME>/
       GBIF_<DATASET_NAME>_spatial_validation_report.csv

3) Validation summary text file:
   results/natura_analysis/<DATASET_NAME>/
       GBIF_<DATASET_NAME>_spatial_validation_summary.txt

4) Out-of-bounds coordinate diagnostics:
   results/natura_analysis/<DATASET_NAME>/
       GBIF_<DATASET_NAME>_records_outside_europe_bbox.csv

Notes:
- This step is intended for reporting and readiness assessment only.
- No rows are removed or modified.
- Geometry creation and spatial joins are performed downstream
  in 03_spatial_join_gbif_natura.py.
- Supported datasets: BIRDS / HABITATS / IAS
"""

from pathlib import Path
from datetime import datetime
import logging
import zipfile

import pandas as pd

# ----------------------------------------------------------------------
# Project-level paths
# ----------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]  # .../BMD Implementation

DATA_DIR = PROJECT_ROOT / "data"
FILTERED_DIR = DATA_DIR / "filtered"

LOG_DIR = PROJECT_ROOT / "logs" / "natura_analysis" / "01_validate_spatial_readiness"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------
# Dataset configuration
# ----------------------------------------------------------------------
DATASET_NAME = "IAS"  # "IAS", "BIRDS", "HABITATS"

RESULTS_DIR = PROJECT_ROOT / "results" / "natura_analysis" / DATASET_NAME
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

ZIP_FILENAME = f"GBIF_{DATASET_NAME}_filtered_occurrences.zip"
ZIP_PATH = FILTERED_DIR / DATASET_NAME / ZIP_FILENAME
INNER_CSV = ZIP_FILENAME.replace(".zip", ".csv")

# Column names
COL_GBIF_ID = "gbifID"
COL_SCI_NAME = "scientificName"
COL_SPECIES_KEY = "taxonKey"
COL_LONGITUDE = "decimalLongitude"
COL_LATITUDE = "decimalLatitude"
COL_UNCERTAINTY = "coordinateUncertaintyInMeters"

REQUIRED_COLUMNS = [
    COL_GBIF_ID,
    COL_LONGITUDE,
    COL_LATITUDE,
    COL_UNCERTAINTY,
]

OPTIONAL_COLUMNS = [
    COL_SCI_NAME,
    COL_SPECIES_KEY,
]

# Europe-oriented coarse bounding box (for Natura plausibility checks)
EUROPE_MIN_LON = -35.0
EUROPE_MAX_LON = 45.0
EUROPE_MIN_LAT = 24.0
EUROPE_MAX_LAT = 75.0

# ----------------------------------------------------------------------
# Logging configuration
# ----------------------------------------------------------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
logfile = LOG_DIR / f"{DATASET_NAME}_spatial_validation_{timestamp}.log"

logger = logging.getLogger("bmd.natura_analysis.spatial_validation")
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

file_handler = logging.FileHandler(logfile, mode="a", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
logger.addHandler(file_handler)


def log(msg: str) -> None:
    print(msg)
    logger.info(msg)


# ----------------------------------------------------------------------
# Report helpers
# ----------------------------------------------------------------------
report_rows = []


def add_report_row(section: str, metric: str, value, status: str = "INFO", notes: str = "") -> None:
    report_rows.append(
        {
            "dataset_name": DATASET_NAME,
            "section": section,
            "metric": metric,
            "value": value,
            "status": status,
            "notes": notes,
        }
    )


def safe_int(value) -> int:
    return int(value) if pd.notna(value) else 0


def safe_str(value) -> str:
    return "NA" if pd.isna(value) else str(value)


# ----------------------------------------------------------------------
# Output file paths
# ----------------------------------------------------------------------
REPORT_CSV = RESULTS_DIR / f"GBIF_{DATASET_NAME}_spatial_validation_report.csv"
SUMMARY_TXT = RESULTS_DIR / f"GBIF_{DATASET_NAME}_spatial_validation_summary.txt"

# ----------------------------------------------------------------------
# Load dataset
# ----------------------------------------------------------------------
if not ZIP_PATH.exists():
    raise FileNotFoundError(f"Filtered ZIP not found: {ZIP_PATH}")

with zipfile.ZipFile(str(ZIP_PATH)) as z:
    if INNER_CSV not in z.namelist():
        raise FileNotFoundError(f"Inner CSV '{INNER_CSV}' not found inside ZIP: {ZIP_PATH}")
    with z.open(INNER_CSV) as f:
        df = pd.read_csv(f, low_memory=False)

log("=== Spatial input validation started ===")
log(f"Dataset: {DATASET_NAME}")
log(f"ZIP path: {ZIP_PATH}")
log(f"Shape: {df.shape}")

add_report_row("dataset", "rows", len(df))
add_report_row("dataset", "columns", len(df.columns))
add_report_row("dataset", "input_zip", str(ZIP_PATH), "INFO")
add_report_row("dataset", "inner_csv", INNER_CSV, "INFO")

# ----------------------------------------------------------------------
# Required columns
# ----------------------------------------------------------------------
log("\n[Required columns]")
missing_required = [c for c in REQUIRED_COLUMNS if c not in df.columns]

if missing_required:
    log(f"ERROR: Missing required columns: {missing_required}")
    add_report_row(
        "required_columns",
        "missing_required_columns",
        ", ".join(missing_required),
        status="FAIL",
        notes="Downstream spatial processing cannot proceed safely.",
    )
else:
    log("All required columns detected.")
    add_report_row(
        "required_columns",
        "all_required_columns_present",
        True,
        status="PASS",
    )

for optional_col in OPTIONAL_COLUMNS:
    add_report_row(
        "optional_columns",
        f"{optional_col}_present",
        optional_col in df.columns,
        status="INFO",
    )

critical_fail = len(missing_required) > 0

# Default placeholders for summary
missing_gbif = None
duplicate_gbif = None
invalid_lon_numeric = None
invalid_lat_numeric = None
invalid_unc_numeric = None
missing_lon = None
missing_lat = None
missing_unc = None
invalid_lon_range = None
invalid_lat_range = None
zero_zero = None
zero_lon = None
zero_lat = None
duplicate_coord_pairs = None
min_lon = None
max_lon = None
min_lat = None
max_lat = None
records_inside_bbox = None
records_outside_bbox = None
nonnull_uncertainty = None
negative_uncertainty = None
zero_uncertainty = None
unc_min = None
unc_median = None
unc_max = None

# ----------------------------------------------------------------------
# gbifID validation
# ----------------------------------------------------------------------
if COL_GBIF_ID in df.columns:
    log("\n[gbifID validation]")
    missing_gbif = safe_int(df[COL_GBIF_ID].isna().sum())
    duplicate_gbif = safe_int(df[COL_GBIF_ID].duplicated().sum())

    log(f"Missing gbifID values: {missing_gbif}")
    log(f"Duplicate gbifID values: {duplicate_gbif}")

    add_report_row("gbifID", "missing_gbifID", missing_gbif, "PASS" if missing_gbif == 0 else "FAIL")
    add_report_row("gbifID", "duplicate_gbifID", duplicate_gbif, "PASS" if duplicate_gbif == 0 else "FAIL")

# ----------------------------------------------------------------------
# Coordinate and uncertainty checks
# ----------------------------------------------------------------------
if not critical_fail:
    log("\n[Numeric coercion checks]")

    lon_num = pd.to_numeric(df[COL_LONGITUDE], errors="coerce")
    lat_num = pd.to_numeric(df[COL_LATITUDE], errors="coerce")
    unc_num = pd.to_numeric(df[COL_UNCERTAINTY], errors="coerce")

    invalid_lon_numeric = safe_int(df[COL_LONGITUDE].notna().sum() - lon_num.notna().sum())
    invalid_lat_numeric = safe_int(df[COL_LATITUDE].notna().sum() - lat_num.notna().sum())
    invalid_unc_numeric = safe_int(df[COL_UNCERTAINTY].notna().sum() - unc_num.notna().sum())

    log(f"Non-numeric longitude values: {invalid_lon_numeric}")
    log(f"Non-numeric latitude values: {invalid_lat_numeric}")
    log(f"Non-numeric uncertainty values: {invalid_unc_numeric}")

    add_report_row(
        "numeric_validation",
        "invalid_longitude_numeric",
        invalid_lon_numeric,
        "PASS" if invalid_lon_numeric == 0 else "FAIL",
    )
    add_report_row(
        "numeric_validation",
        "invalid_latitude_numeric",
        invalid_lat_numeric,
        "PASS" if invalid_lat_numeric == 0 else "FAIL",
    )
    add_report_row(
        "numeric_validation",
        "invalid_uncertainty_numeric",
        invalid_unc_numeric,
        "PASS" if invalid_unc_numeric == 0 else "WARNING",
        notes="Uncertainty-aware branch may be partially affected.",
    )

    # ------------------------------------------------------------------
    # Missing values
    # ------------------------------------------------------------------
    log("\n[Missing values]")
    missing_lon = safe_int(df[COL_LONGITUDE].isna().sum())
    missing_lat = safe_int(df[COL_LATITUDE].isna().sum())
    missing_unc = safe_int(df[COL_UNCERTAINTY].isna().sum())

    log(f"Missing longitude values: {missing_lon}")
    log(f"Missing latitude values: {missing_lat}")
    log(f"Missing uncertainty values: {missing_unc}")

    add_report_row("missing_values", "missing_longitude", missing_lon, "PASS" if missing_lon == 0 else "FAIL")
    add_report_row("missing_values", "missing_latitude", missing_lat, "PASS" if missing_lat == 0 else "FAIL")
    add_report_row(
        "missing_values",
        "missing_uncertainty",
        missing_unc,
        "PASS" if missing_unc == 0 else "WARNING",
        notes="Baseline spatial join may still proceed, but uncertainty-aware assessment may be incomplete.",
    )

    # ------------------------------------------------------------------
    # Coordinate ranges
    # ------------------------------------------------------------------
    log("\n[Coordinate range validation]")
    invalid_lon_range = safe_int((lon_num.notna() & ~lon_num.between(-180, 180)).sum())
    invalid_lat_range = safe_int((lat_num.notna() & ~lat_num.between(-90, 90)).sum())

    log(f"Longitude outside [-180, 180]: {invalid_lon_range}")
    log(f"Latitude outside [-90, 90]: {invalid_lat_range}")

    add_report_row(
        "coordinate_ranges",
        "invalid_longitude_range",
        invalid_lon_range,
        "PASS" if invalid_lon_range == 0 else "FAIL",
    )
    add_report_row(
        "coordinate_ranges",
        "invalid_latitude_range",
        invalid_lat_range,
        "PASS" if invalid_lat_range == 0 else "FAIL",
    )

    # ------------------------------------------------------------------
    # Zero / suspicious coordinates
    # ------------------------------------------------------------------
    log("\n[Zero / suspicious coordinate checks]")
    zero_zero = safe_int(((lon_num == 0) & (lat_num == 0)).sum())
    zero_lon = safe_int((lon_num == 0).sum())
    zero_lat = safe_int((lat_num == 0).sum())

    log(f"Records at (0,0): {zero_zero}")
    log(f"Records with longitude = 0: {zero_lon}")
    log(f"Records with latitude = 0: {zero_lat}")

    add_report_row(
        "suspicious_coordinates",
        "records_at_0_0",
        zero_zero,
        "PASS" if zero_zero == 0 else "WARNING",
    )
    add_report_row("suspicious_coordinates", "records_with_longitude_0", zero_lon, "INFO")
    add_report_row("suspicious_coordinates", "records_with_latitude_0", zero_lat, "INFO")

    # ------------------------------------------------------------------
    # Longitude = 0 diagnostic
    # ------------------------------------------------------------------
    log("\n[Longitude = 0 diagnostic]")

    lon_zero_mask = lon_num == 0
    lon_zero_count = int(lon_zero_mask.sum())
    unique_lat_count = None
    top_lat_counts = None

    unique_lon_count = None
    top_lon_counts = None

    if lon_zero_count > 0:
        lat_values_lon0 = lat_num[lon_zero_mask]

        unique_lat_count = lat_values_lon0.nunique()
        top_lat_counts = lat_values_lon0.value_counts().head(5)

        log(f"Records with longitude = 0: {lon_zero_count}")
        log(f"Unique latitude values among longitude = 0: {unique_lat_count}")
        log("Top latitude values (lon = 0):")
        
        for lat_val, count in top_lat_counts.items():
            log(f"  Latitude {lat_val}: {count} records")

        add_report_row(
            "suspicious_coordinates",
            "lon0_unique_latitudes",
            unique_lat_count,
            "INFO"
        )

        add_report_row(
            "suspicious_coordinates",
            "lon0_top_latitudes",
            "; ".join([f"{round(lat,4)}:{cnt}" for lat, cnt in top_lat_counts.items()]),
            "INFO",
            notes="Top repeated latitude values among records with longitude = 0"
        )

    else:
        log("No records with longitude = 0 found.")

    # ------------------------------------------------------------------
    # Latitude = 0 diagnostic 
    # ------------------------------------------------------------------
    log("\n[Latitude = 0 diagnostic]")

    lat_zero_mask = lat_num == 0
    lat_zero_count = int(lat_zero_mask.sum())

    if lat_zero_count > 0:
        lon_values_lat0 = lon_num[lat_zero_mask]

        unique_lon_count = lon_values_lat0.nunique()
        top_lon_counts = lon_values_lat0.value_counts().head(5)

        log(f"Records with latitude = 0: {lat_zero_count}")
        log(f"Unique longitude values among latitude = 0: {unique_lon_count}")
        log("Top longitude values (lat = 0):")

        for lon_val, count in top_lon_counts.items():
            log(f"  Longitude {lon_val}: {count} records")

        add_report_row(
            "suspicious_coordinates",
            "lat0_unique_longitudes",
            unique_lon_count,
            "INFO"
        )

        add_report_row(
            "suspicious_coordinates",
            "lat0_top_longitudes",
            "; ".join([f"{round(lon,4)}:{cnt}" for lon, cnt in top_lon_counts.items()]),
            "INFO",
            notes="Top repeated longitude values among records with latitude = 0"
        )

    else:
        log("No records with latitude = 0 found.")

    # ------------------------------------------------------------------
    # Duplicate coordinate pairs
    # ------------------------------------------------------------------
    log("\n[Duplicate coordinate pairs]")
    coords_df = pd.DataFrame({"lon": lon_num, "lat": lat_num})
    valid_coord_pairs = coords_df["lon"].notna() & coords_df["lat"].notna()
    duplicate_coord_pairs = safe_int(coords_df.loc[valid_coord_pairs].duplicated().sum())

    log(f"Duplicate coordinate pairs: {duplicate_coord_pairs}")

    add_report_row(
        "duplicate_coordinates",
        "duplicate_coordinate_pairs",
        duplicate_coord_pairs,
        "PASS" if duplicate_coord_pairs == 0 else "WARNING",
        notes="Repeated coordinates may reflect repeated sampling locations, centroid assignment, or institutional georeferencing patterns.",
    )

    # ------------------------------------------------------------------
    # Spatial extent summary
    # ------------------------------------------------------------------
    log("\n[Spatial extent summary]")
    min_lon = lon_num.min()
    max_lon = lon_num.max()
    min_lat = lat_num.min()
    max_lat = lat_num.max()

    log(f"Longitude extent: {min_lon} to {max_lon}")
    log(f"Latitude extent:  {min_lat} to {max_lat}")

    add_report_row("spatial_extent", "min_longitude", min_lon, "INFO")
    add_report_row("spatial_extent", "max_longitude", max_lon, "INFO")
    add_report_row("spatial_extent", "min_latitude", min_lat, "INFO")
    add_report_row("spatial_extent", "max_latitude", max_lat, "INFO")

    # ------------------------------------------------------------------
    # Coarse Europe plausibility check
    # ------------------------------------------------------------------
    log("\n[Coarse study-area plausibility check]")
    valid_numeric_coords = lon_num.notna() & lat_num.notna()
    inside_europe_bbox = (
        valid_numeric_coords
        & lon_num.between(EUROPE_MIN_LON, EUROPE_MAX_LON)
        & lat_num.between(EUROPE_MIN_LAT, EUROPE_MAX_LAT)
    )

    records_inside_bbox = safe_int(inside_europe_bbox.sum())
    records_outside_bbox = safe_int(valid_numeric_coords.sum() - inside_europe_bbox.sum())

    log(f"Records inside Europe-oriented bounding box: {records_inside_bbox}")
    log(f"Records outside Europe-oriented bounding box: {records_outside_bbox}")

    add_report_row(
        "study_area_plausibility",
        "records_inside_europe_bbox",
        records_inside_bbox,
        "INFO",
        notes=f"Bounding box: lon [{EUROPE_MIN_LON}, {EUROPE_MAX_LON}], lat [{EUROPE_MIN_LAT}, {EUROPE_MAX_LAT}]",
    )
    add_report_row(
        "study_area_plausibility",
        "records_outside_europe_bbox",
        records_outside_bbox,
        "PASS" if records_outside_bbox == 0 else "WARNING",
        notes="These records may be unexpected for the Natura-oriented European study extent and should be reviewed.",
    )
    # ------------------------------------------------------------------
    # Export records outside Europe bounding box (diagnostic)
    # ------------------------------------------------------------------
    if records_outside_bbox > 0:
        log("\n[Exporting records outside Europe bounding box]")

        outside_df = df.loc[~inside_europe_bbox & valid_numeric_coords].copy()

        # Keep only useful columns
        cols_to_keep = [
            COL_GBIF_ID,
            COL_SCI_NAME if COL_SCI_NAME in df.columns else None,
            COL_SPECIES_KEY if COL_SPECIES_KEY in df.columns else None,
            COL_LONGITUDE,
            COL_LATITUDE,
            COL_UNCERTAINTY,
        ]
        cols_to_keep = [c for c in cols_to_keep if c is not None]

        outside_df = outside_df[cols_to_keep]

        OUTSIDE_CSV = RESULTS_DIR / f"GBIF_{DATASET_NAME}_records_outside_europe_bbox.csv"
        outside_df.to_csv(OUTSIDE_CSV, index=False)

        log(f"Saved outside-Europe records to: {OUTSIDE_CSV}")

        add_report_row(
            "study_area_plausibility",
            "outside_records_export",
            str(OUTSIDE_CSV.name),
            "INFO",
            notes="Subset of records falling outside the Europe-oriented bounding box."
        )

    # ------------------------------------------------------------------
    # Uncertainty field checks
    # ------------------------------------------------------------------
    log("\n[Coordinate uncertainty checks]")
    nonnull_uncertainty = safe_int(unc_num.notna().sum())
    negative_uncertainty = safe_int((unc_num < 0).sum())
    zero_uncertainty = safe_int((unc_num == 0).sum())

    unc_min = unc_num.min()
    unc_median = unc_num.median()
    unc_max = unc_num.max()

    log(f"Non-null uncertainty values: {nonnull_uncertainty}")
    log(f"Negative uncertainty values: {negative_uncertainty}")
    log(f"Zero uncertainty values: {zero_uncertainty}")
    log(f"Uncertainty min / median / max: {unc_min} / {unc_median} / {unc_max}")

    add_report_row("uncertainty_field", "non_null_uncertainty_values", nonnull_uncertainty, "INFO")
    add_report_row(
        "uncertainty_field",
        "negative_uncertainty_values",
        negative_uncertainty,
        "PASS" if negative_uncertainty == 0 else "WARNING",
    )
    add_report_row("uncertainty_field", "zero_uncertainty_values", zero_uncertainty, "INFO")
    add_report_row("uncertainty_field", "uncertainty_min", unc_min, "INFO")
    add_report_row("uncertainty_field", "uncertainty_median", unc_median, "INFO")
    add_report_row("uncertainty_field", "uncertainty_max", unc_max, "INFO")

# ----------------------------------------------------------------------
# Final verdict
# ----------------------------------------------------------------------
log("\n[Final readiness assessment]")

if critical_fail:
    final_status = "FAIL"
    final_notes = "Critical required columns are missing. Downstream spatial processing should not proceed."
else:
    fail_count = sum(1 for row in report_rows if row["status"] == "FAIL")
    warning_count = sum(1 for row in report_rows if row["status"] == "WARNING")

    if fail_count > 0:
        final_status = "FAIL"
        final_notes = "One or more critical spatial readiness checks failed."
    elif warning_count > 0:
        final_status = "PASS WITH WARNINGS"
        final_notes = "Dataset passed core spatial readiness checks, but one or more warnings should be reviewed."
    else:
        final_status = "PASS"
        final_notes = "Dataset passed spatial readiness validation."

log(f"Final status: {final_status}")
log(final_notes)

add_report_row("final_assessment", "final_status", final_status, final_status, final_notes)
add_report_row("final_assessment", "validation_timestamp", timestamp, "INFO")

# ----------------------------------------------------------------------
# Save CSV report
# ----------------------------------------------------------------------
report_df = pd.DataFrame(report_rows)
report_df.to_csv(REPORT_CSV, index=False)

# ----------------------------------------------------------------------
# Save TXT summary
# ----------------------------------------------------------------------

def format_value(val):
    if pd.isna(val):
        return "NA"
    return str(val)

lines = [
    "BMD Natura 2000 - Spatial Input Validation Summary",
    "=" * 60,
    f"Dataset: {DATASET_NAME}",
    f"Validation timestamp: {timestamp}",
    "",
]

# Group by section (preserves logical structure)
for section in report_df["section"].unique():
    
    lines.append(section.replace("_", " ").title())
    lines.append("-" * 60)

    subset = report_df[report_df["section"] == section]

    for _, row in subset.iterrows():
        metric = row["metric"].replace("_", " ")
        value = format_value(row["value"])
        status = row["status"]
        notes = row["notes"]

        line = f"{metric}: {value}"

        if status in ["WARNING", "FAIL"]:
            line += f"  [{status}]"

        lines.append(line)

        if notes:
            lines.append(f"  -> {notes}")

    lines.append("")

# Output paths (cleaner – relative instead of full system path)
lines.extend([
    "Output files",
    "-" * 60,
    f"CSV report: {REPORT_CSV.name}",
    f"TXT summary: {SUMMARY_TXT.name}",
    f"Log file: {logfile.name}",
    "",
    "Interpretation guide",
    "-" * 60,
    "PASS: Dataset passed the corresponding validation check.",
    "WARNING: Dataset may still be usable, but the flagged issue should be reviewed.",
    "FAIL: Critical issue detected; downstream spatial processing may be unsafe or invalid.",
])

with open(SUMMARY_TXT, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))

log(f"\nValidation report saved to: {REPORT_CSV}")
log(f"Validation summary saved to: {SUMMARY_TXT}")
log(f"Validation log saved to: {logfile}")
log("=== Spatial input validation completed ===")