"""
Script: core/helpers-archived/explore_joined_natura_outputs.py

Purpose:
Lightweight exploratory helper script for inspecting the joined GBIF-Natura
outputs produced by 03_spatial_join_gbif_natura.py.

It is intended for personal exploration only and does not modify data.

It explores:
- Baseline joined GeoPackage
- Uncertainty-enriched GeoPackage
"""

from pathlib import Path
import geopandas as gpd
import pandas as pd


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
DATASET_NAME = "IAS"   # "IAS", "BIRDS", "HABITATS"

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

PROCESSED_DIR = PROJECT_ROOT / "data" / "processed" / DATASET_NAME

BASELINE_GPKG = PROCESSED_DIR / f"GBIF_{DATASET_NAME}_with_natura_sites.gpkg"
UNCERT_GPKG = PROCESSED_DIR / f"GBIF_{DATASET_NAME}_with_natura_sites_uncertainty.gpkg"

BASELINE_LAYER = f"gbif_{DATASET_NAME.lower()}_with_natura"
UNCERT_LAYER = f"gbif_{DATASET_NAME.lower()}_with_natura_uncertainty"

SITE_CODE_COL = "SITECODE"
SITE_NAME_COL = "SITENAME"
COUNTRY_COL = "MS"
TYPE_COL = "SITETYPE"
UNC_CLASS_COL = "uncertainty_class"


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def print_header(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def top_counts(df: pd.DataFrame, col: str, n: int = 10) -> None:
    print_header(f"Top {n} values for: {col}")
    if col not in df.columns:
        print(f"Column '{col}' not found.")
        return
    print(df[col].value_counts(dropna=False).head(n).to_string())


def show_basic_info(gdf: gpd.GeoDataFrame, label: str) -> None:
    print_header(f"{label} - Basic overview")
    print(f"Rows:   {len(gdf)}")
    print(f"Cols:   {len(gdf.columns)}")
    print(f"CRS:    {gdf.crs}")
    print("\nColumns:")
    print(list(gdf.columns))


def show_geometry_checks(gdf: gpd.GeoDataFrame, label: str) -> None:
    print_header(f"{label} - Geometry checks")
    print(f"Missing geometries: {int(gdf.geometry.isna().sum())}")
    print(f"Empty geometries:   {int(gdf.geometry.is_empty.sum())}")
    print(f"Invalid geometries: {int((~gdf.geometry.is_valid).sum())}")


def show_bounds(gdf: gpd.GeoDataFrame, label: str) -> None:
    print_header(f"{label} - Spatial extent")
    minx, miny, maxx, maxy = gdf.total_bounds
    print(f"min longitude: {minx}")
    print(f"min latitude:  {miny}")
    print(f"max longitude: {maxx}")
    print(f"max latitude:  {maxy}")


# ----------------------------------------------------------------------
# Load baseline
# ----------------------------------------------------------------------
if not BASELINE_GPKG.exists():
    raise FileNotFoundError(f"Baseline GPKG not found: {BASELINE_GPKG}")

print_header("Loading BASELINE joined output")
gdf_base = gpd.read_file(BASELINE_GPKG, layer=BASELINE_LAYER)

show_basic_info(gdf_base, "BASELINE")

print_header("BASELINE - First 5 rows")
print(gdf_base.head())

show_geometry_checks(gdf_base, "BASELINE")
show_bounds(gdf_base, "BASELINE")

# ----------------------------------------------------------------------
# Baseline-specific exploration
# ----------------------------------------------------------------------
print_header("BASELINE - Natura match summary")
if SITE_CODE_COL in gdf_base.columns:
    inside_count = int(gdf_base[SITE_CODE_COL].notna().sum())
    outside_count = int(gdf_base[SITE_CODE_COL].isna().sum())
    total = len(gdf_base)

    print(f"Inside Natura:  {inside_count} ({inside_count / total * 100:.2f}%)")
    print(f"Outside Natura: {outside_count} ({outside_count / total * 100:.2f}%)")
else:
    print(f"Column '{SITE_CODE_COL}' not found.")

top_counts(gdf_base, COUNTRY_COL, n=15)
top_counts(gdf_base, TYPE_COL, n=10)

if SITE_NAME_COL in gdf_base.columns:
    print_header("BASELINE - Example Natura site names")
    print(gdf_base[SITE_NAME_COL].dropna().head(10).to_string(index=False))

if "gbifID" in gdf_base.columns:
    print_header("BASELINE - gbifID checks")
    print(f"Missing gbifID:   {int(gdf_base['gbifID'].isna().sum())}")
    print(f"Duplicate gbifID: {int(gdf_base['gbifID'].duplicated().sum())}")

# ----------------------------------------------------------------------
# Load uncertainty
# ----------------------------------------------------------------------
if not UNCERT_GPKG.exists():
    raise FileNotFoundError(f"Uncertainty GPKG not found: {UNCERT_GPKG}")

print_header("Loading UNCERTAINTY joined output")
gdf_unc = gpd.read_file(UNCERT_GPKG, layer=UNCERT_LAYER)

show_basic_info(gdf_unc, "UNCERTAINTY")

print_header("UNCERTAINTY - First 5 rows")
print(gdf_unc.head())

show_geometry_checks(gdf_unc, "UNCERTAINTY")
show_bounds(gdf_unc, "UNCERTAINTY")

# ----------------------------------------------------------------------
# Uncertainty-specific exploration
# ----------------------------------------------------------------------
top_counts(gdf_unc, UNC_CLASS_COL, n=10)

print_header("UNCERTAINTY - Distance field summary")
for col in ["uncertainty_m", "dist_to_boundary_m", "dist_to_polygon_m"]:
    if col in gdf_unc.columns:
        print(f"\n{col}")
        print(gdf_unc[col].describe().to_string())
    else:
        print(f"\nColumn '{col}' not found.")

print_header("UNCERTAINTY - Baseline within check")
if "baseline_within" in gdf_unc.columns:
    print(gdf_unc["baseline_within"].value_counts(dropna=False).to_string())
else:
    print("Column 'baseline_within' not found.")

# ----------------------------------------------------------------------
# Compare baseline vs uncertainty file sizes
# ----------------------------------------------------------------------
print_header("Comparison - baseline vs uncertainty")
print(f"Baseline rows:    {len(gdf_base)}")
print(f"Uncertainty rows: {len(gdf_unc)}")

if len(gdf_base) == len(gdf_unc):
    print("Row counts match.")
else:
    print("WARNING: Row counts do not match.")

print_header("Done")
print("Lightweight exploration completed.")