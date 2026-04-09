"""
Script: core/helpers-archived/explore_natura_sites.py

Purpose:
A lightweight exploratory helper script for inspecting the prepared
Natura 2000 sites layer. This script is intended for personal use
and quick familiarisation with the dataset structure, attributes,
basic spatial properties, and simple summaries.

It does not modify any data and does not generate production outputs.
"""

from pathlib import Path
import geopandas as gpd
import pandas as pd


# ----------------------------------------------------------------------
# Project paths
# ----------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]  # .../BMD Implementation

NATURA_GPKG = (
    PROJECT_ROOT
    / "data"
    / "external"
    / "natura2k"
    / "Natura2000_sites_prepared.gpkg"
)

NATURA_LAYER = "natura_sites_epsg4326"


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def print_header(title: str) -> None:
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def show_top_counts(df: pd.DataFrame, column: str, n: int = 10) -> None:
    print_header(f"Top {n} values for: {column}")
    if column not in df.columns:
        print(f"Column '{column}' not found.")
        return

    counts = df[column].value_counts(dropna=False).head(n)
    print(counts.to_string())


# ----------------------------------------------------------------------
# Load dataset
# ----------------------------------------------------------------------
if not NATURA_GPKG.exists():
    raise FileNotFoundError(f"Natura prepared GeoPackage not found: {NATURA_GPKG}")

print_header("Loading Natura 2000 prepared layer")
gdf = gpd.read_file(NATURA_GPKG, layer=NATURA_LAYER)

# ----------------------------------------------------------------------
# 1. Basic overview
# ----------------------------------------------------------------------
print_header("1. Basic overview")
print(f"File:   {NATURA_GPKG}")
print(f"Layer:  {NATURA_LAYER}")
print(f"Rows:   {len(gdf)}")
print(f"Cols:   {len(gdf.columns)}")
print(f"CRS:    {gdf.crs}")

print("\nColumns:")
print(list(gdf.columns))

# ----------------------------------------------------------------------
# 2. First rows
# ----------------------------------------------------------------------
print_header("2. First 5 rows")
print(gdf.head())

# ----------------------------------------------------------------------
# 3. Attribute exploration
# ----------------------------------------------------------------------
show_top_counts(gdf, "MS", n=15)
show_top_counts(gdf, "SITETYPE", n=10)

if "SITENAME" in gdf.columns:
    print_header("Example site names")
    print(gdf["SITENAME"].dropna().head(10).to_string(index=False))

# ----------------------------------------------------------------------
# 4. Geometry health checks
# ----------------------------------------------------------------------
print_header("4. Geometry checks")
empty_geoms = int(gdf.geometry.is_empty.sum())
missing_geoms = int(gdf.geometry.isna().sum())
invalid_geoms = int((~gdf.geometry.is_valid).sum())

print(f"Missing geometries: {missing_geoms}")
print(f"Empty geometries:   {empty_geoms}")
print(f"Invalid geometries: {invalid_geoms}")

# ----------------------------------------------------------------------
# 5. Spatial extent
# ----------------------------------------------------------------------
print_header("5. Spatial extent")
minx, miny, maxx, maxy = gdf.total_bounds
print(f"Total bounds:")
print(f"  min longitude: {minx}")
print(f"  min latitude:  {miny}")
print(f"  max longitude: {maxx}")
print(f"  max latitude:  {maxy}")

# ----------------------------------------------------------------------
# 6. Geometry types
# ----------------------------------------------------------------------
print_header("6. Geometry types")
geom_types = gdf.geometry.geom_type.value_counts(dropna=False)
print(geom_types.to_string())

# ----------------------------------------------------------------------
# 7. Quick area summary (approximate, in EPSG:3035)
# ----------------------------------------------------------------------
print_header("7. Approximate area exploration")
try:
    gdf_3035 = gdf.to_crs(epsg=3035)
    gdf_3035["area_km2"] = gdf_3035.geometry.area / 1_000_000

    print("Area statistics (km²):")
    print(gdf_3035["area_km2"].describe().to_string())

    print_header("Top 10 largest Natura sites (approximate area)")
    area_cols = [c for c in ["SITECODE", "SITENAME", "MS", "SITETYPE"] if c in gdf_3035.columns]
    largest = gdf_3035[area_cols + ["area_km2"]].sort_values("area_km2", ascending=False).head(10)
    print(largest.to_string(index=False))

except Exception as e:
    print(f"Area calculation skipped due to error: {e}")

# ----------------------------------------------------------------------
# 8. Quick duplicate checks
# ----------------------------------------------------------------------
print_header("8. Simple duplicate checks")
for col in ["SITECODE", "INSPIRE_ID"]:
    if col in gdf.columns:
        dup_count = int(gdf[col].duplicated().sum())
        missing_count = int(gdf[col].isna().sum())
        print(f"{col}: missing={missing_count}, duplicates={dup_count}")

print_header("Done")
print("This was only a lightweight exploratory inspection.")