"""
Script: 04_dataset_schema_summary.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Description:
Generates a transparent schema-level summary of a filtered GBIF dataset.
The script documents the structural properties of the harmonised dataset,
including column names, data types, completeness, uniqueness, sample values,
logical field roles, and selected descriptive summaries.

Purpose:
To provide a reproducible dataset inspection step at the end of the core
pipeline, supporting transparency, debugging, and downstream analytical
readiness across all dataset types (Birds, Habitats, IAS).

Input:
- Filtered GBIF dataset (CSV inside ZIP) produced by 02_filter_gbif_dataset.py

Output:
- Plain-text schema summary report
- CSV table with per-column schema information

Notes:
This script does not modify the dataset.
It is dataset-agnostic and intended as the final transparency/documentation
step of the core GBIF processing pipeline.
"""

from pathlib import Path
import zipfile
import pandas as pd
from datetime import datetime


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
DATASET_NAME = "IAS"  # BIRDS / HABITATS / IAS

FILTERED_SUBDIR = FILTERED_DIR / DATASET_NAME
FILTERED_ZIP = FILTERED_SUBDIR / f"GBIF_{DATASET_NAME}_filtered_occurrences.zip"
INNER_CSV = f"GBIF_{DATASET_NAME}_filtered_occurrences.csv"

OUT_DIR = RESULTS_DIR / DATASET_NAME
OUT_DIR.mkdir(parents=True, exist_ok=True)

TXT_REPORT = OUT_DIR / f"GBIF_{DATASET_NAME}_dataset_schema_summary.txt"
CSV_REPORT = OUT_DIR / f"GBIF_{DATASET_NAME}_dataset_schema_summary.csv"

N_SAMPLES = 3


# ----------------------------------------------------------------------
# Column metadata
# ----------------------------------------------------------------------
COLUMN_ROLES = {
    "gbifID": "primary key",
    "taxonKey": "taxonomic identifier",
    "scientificName": "taxonomic label",
    "basisOfRecord": "record provenance field",
    "eventDate": "raw temporal field",
    "year": "derived temporal field",
    "month": "derived temporal field",
    "countryCode": "geographic code",
    "decimalLatitude": "spatial coordinate",
    "decimalLongitude": "spatial coordinate",
    "coordinateUncertaintyInMeters": "spatial precision field",
}

ID_FIELDS = {"gbifID", "taxonKey"}
DATE_LIKE_FIELDS = {"eventDate"}
MEASUREMENT_FIELDS = {
    "decimalLatitude",
    "decimalLongitude",
    "coordinateUncertaintyInMeters",
}
TEMPORAL_INTEGER_FIELDS = {"year", "month"}
CATEGORICAL_FIELDS = {"basisOfRecord", "countryCode"}
TEXT_FIELDS = {"scientificName"}


# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------
def safe_sample_values(series: pd.Series, n: int = 3) -> list[str]:
    """Return up to n unique non-null sample values as strings."""
    try:
        vals = series.dropna().astype(str).unique()[:n]
        return list(vals)
    except Exception:
        return []


def is_numeric_series(series: pd.Series) -> bool:
    """True only for genuinely numeric dtypes."""
    return pd.api.types.is_numeric_dtype(series)


def safe_numeric_stats(series: pd.Series) -> dict:
    """
    Return numeric summaries only for genuinely numeric series.
    For non-numeric series, return None values.
    """
    if not is_numeric_series(series):
        return {
            "numeric_min": None,
            "numeric_max": None,
            "numeric_mean": None,
            "numeric_median": None,
        }

    non_null = series.dropna()
    if len(non_null) == 0:
        return {
            "numeric_min": None,
            "numeric_max": None,
            "numeric_mean": None,
            "numeric_median": None,
        }

    return {
        "numeric_min": non_null.min(),
        "numeric_max": non_null.max(),
        "numeric_mean": round(float(non_null.mean()), 4),
        "numeric_median": round(float(non_null.median()), 4),
    }


def safe_measurement_stats(series: pd.Series, column_name: str) -> dict:
    """
    Return numeric summaries only where they are semantically meaningful.
    IDs do not receive min/max/mean/median, even if numeric.
    """
    if column_name in ID_FIELDS:
        return {
            "numeric_min": None,
            "numeric_max": None,
            "numeric_mean": None,
            "numeric_median": None,
        }

    if (
        column_name in MEASUREMENT_FIELDS
        or column_name in TEMPORAL_INTEGER_FIELDS
    ):
        return safe_numeric_stats(series)

    return {
        "numeric_min": None,
        "numeric_max": None,
        "numeric_mean": None,
        "numeric_median": None,
    }


def detect_logical_type(series: pd.Series, column_name: str) -> str:
    """Assign a human-readable logical type to each column."""
    if column_name in ID_FIELDS:
        return "integer identifier"
    if column_name in DATE_LIKE_FIELDS:
        return "date-like string"
    if column_name in TEMPORAL_INTEGER_FIELDS:
        return "temporal integer"
    if column_name in MEASUREMENT_FIELDS:
        return "numeric measurement"
    if column_name in CATEGORICAL_FIELDS:
        return "categorical-like string"
    if column_name in TEXT_FIELDS:
        return "free-text taxonomic field"

    dtype = str(series.dtype)
    if pd.api.types.is_integer_dtype(series):
        return "integer"
    if pd.api.types.is_float_dtype(series):
        return "float"
    if pd.api.types.is_string_dtype(series) or dtype in {"object", "string"}:
        return "string"
    return "other"


def detect_date_like_quality(series: pd.Series) -> tuple[int, float]:
    """
    Assess how many non-null values are parseable as dates.
    Useful for eventDate or other date-like string fields.
    """
    non_null = series.dropna()
    if len(non_null) == 0:
        return 0, 0.0

    parsed = pd.to_datetime(non_null, errors="coerce")
    valid = int(parsed.notna().sum())
    pct = round((valid / len(non_null)) * 100, 4)
    return valid, pct


def compute_warning_flags(
    series: pd.Series, missing_pct: float, unique_vals: int
) -> str:
    """
    Create simple warning / diagnostic flags.
    """
    flags = []

    if missing_pct >= 20:
        flags.append("high_missingness")
    elif missing_pct > 0:
        flags.append("some_missingness")

    if unique_vals == 1 and series.notna().sum() > 0:
        flags.append("constant_column")

    if unique_vals <= 12:
        flags.append("low_cardinality")

    return " | ".join(flags) if flags else "none"


# ----------------------------------------------------------------------
# Load filtered dataset
# ----------------------------------------------------------------------
if not FILTERED_ZIP.exists():
    raise FileNotFoundError(f"Filtered ZIP not found: {FILTERED_ZIP}")

read_dtypes = {
    "gbifID": "Int64",
    "taxonKey": "Int64",
    "year": "Int64",
    "month": "Int64",
    "scientificName": "string",
    "basisOfRecord": "string",
    "countryCode": "string",
    "eventDate": "string",
}

with zipfile.ZipFile(FILTERED_ZIP, "r") as z:
    if INNER_CSV not in z.namelist():
        raise FileNotFoundError(f"{INNER_CSV} not found inside ZIP archive.")
    with z.open(INNER_CSV) as f:
        df = pd.read_csv(
            f,
            low_memory=False,
            dtype=read_dtypes,
        )

print(f"Loaded dataset: {DATASET_NAME}")
print(f"Shape: {df.shape}")


# ----------------------------------------------------------------------
# Build schema summary table
# ----------------------------------------------------------------------
rows = []

for col in df.columns:
    non_null = int(df[col].notna().sum())
    missing = int(df[col].isna().sum())
    missing_pct = round((missing / len(df)) * 100, 4) if len(df) > 0 else 0.0
    unique_vals = int(df[col].nunique(dropna=True))
    samples = safe_sample_values(df[col], n=N_SAMPLES)
    role = COLUMN_ROLES.get(col, "unspecified")
    logical_type = detect_logical_type(df[col], col)
    warning_flags = compute_warning_flags(df[col], missing_pct, unique_vals)

    numeric_stats = safe_measurement_stats(df[col], col)

    date_like_valid = None
    date_like_valid_pct = None
    if col in DATE_LIKE_FIELDS:
        date_like_valid, date_like_valid_pct = detect_date_like_quality(df[col])

    row = {
        "column": col,
        "role": role,
        "dtype": str(df[col].dtype),
        "logical_type": logical_type,
        "non_null": non_null,
        "missing": missing,
        "missing_pct": missing_pct,
        "unique_values": unique_vals,
        "sample_values": " | ".join(samples),
        "numeric_min": numeric_stats["numeric_min"],
        "numeric_max": numeric_stats["numeric_max"],
        "numeric_mean": numeric_stats["numeric_mean"],
        "numeric_median": numeric_stats["numeric_median"],
        "date_like_valid_values": date_like_valid,
        "date_like_valid_pct": date_like_valid_pct,
        "warning_flags": warning_flags,
    }
    rows.append(row)

schema_df = pd.DataFrame(rows)
schema_df.to_csv(CSV_REPORT, index=False)


# ----------------------------------------------------------------------
# Write text report
# ----------------------------------------------------------------------
with open(TXT_REPORT, "w", encoding="utf-8") as report:
    report.write("GBIF DATASET SCHEMA SUMMARY REPORT\n\n")
    report.write(f"Dataset name: {DATASET_NAME}\n")
    report.write(f"Source ZIP: {FILTERED_ZIP}\n")
    report.write(f"Inner CSV: {INNER_CSV}\n")
    report.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    report.write("=" * 70 + "\n\n")

    report.write("[Dataset overview]\n")
    report.write(f"Rows: {df.shape[0]}\n")
    report.write(f"Columns: {df.shape[1]}\n")
    report.write(f"Column names: {list(df.columns)}\n\n")

    report.write("[Column groups]\n")
    report.write(f"Identifiers: {[c for c in df.columns if c in ID_FIELDS]}\n")
    report.write(f"Taxonomic fields: {[c for c in df.columns if c in {'taxonKey', 'scientificName'}]}\n")
    report.write(f"Spatial fields: {[c for c in df.columns if c in {'decimalLatitude', 'decimalLongitude', 'coordinateUncertaintyInMeters', 'countryCode'}]}\n")
    report.write(f"Temporal fields: {[c for c in df.columns if c in {'eventDate', 'year', 'month'}]}\n")
    report.write(f"Provenance fields: {[c for c in df.columns if c in {'basisOfRecord'}]}\n\n")

    report.write("[Per-column schema summary]\n")
    for _, row in schema_df.iterrows():
        report.write(
            f"Column: {row['column']}\n"
            f"  role: {row['role']}\n"
            f"  dtype: {row['dtype']}\n"
            f"  logical type: {row['logical_type']}\n"
            f"  non-null: {row['non_null']}\n"
            f"  missing: {row['missing']} ({row['missing_pct']}%)\n"
            f"  unique values: {row['unique_values']}\n"
            f"  sample values: {row['sample_values']}\n"
        )

        if pd.notna(row["date_like_valid_values"]):
            report.write(
                f"  date-like valid values: {int(row['date_like_valid_values'])} "
                f"({row['date_like_valid_pct']}% of non-null values)\n"
            )

        if pd.notna(row["numeric_min"]) or pd.notna(row["numeric_max"]):
            report.write(f"  numeric min: {row['numeric_min']}\n")
            report.write(f"  numeric max: {row['numeric_max']}\n")

        if pd.notna(row["numeric_mean"]) or pd.notna(row["numeric_median"]):
            report.write(f"  numeric mean: {row['numeric_mean']}\n")
            report.write(f"  numeric median: {row['numeric_median']}\n")

        report.write(f"  warning flags: {row['warning_flags']}\n\n")

print(f"Schema text report saved to: {TXT_REPORT}")
print(f"Schema CSV report saved to: {CSV_REPORT}")