"""
Script: 02_prepare_spatial_units.py
Author: Ioannis Kavakiotis
Institution: International Hellenic University
Project: Biodiversity Meets Data (BMD)
Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
Task: T3.3 - Data gap and bias surfaces

Notes:
This script loads the processed study area produced by
01_select_study_area.py and creates the spatial units that will be used
in the downstream gap and bias analyses.

In the current implementation, the spatial units are H3 hexagonal cells
generated over the bounding box of the processed study area and then
filtered to retain only those cells that intersect the study area
polygon. This approach is used to avoid missing boundary cells.

Purpose:
To generate a reproducible geospatial framework for subsequent spatial
analyses, including occurrence density, species richness, spatial
coverage, and spatial autocorrelation.

Input:
- Processed study area polygon (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  <STUDY_AREA_NAME>_study_area.gpkg

Output:
- H3 spatial units (GeoPackage):
  data/processed/<DATASET_NAME>/gap_bias_surfaces/<STUDY_AREA_NAME>/
  GBIF_<DATASET_NAME>_<STUDY_AREA_NAME>_h3_grid_res<H3_RESOLUTION>.gpkg
"""

from pathlib import Path

import geopandas as gpd
from shapely.geometry import Polygon, box
import h3

# ----------------------------------------------------------------------
# Project-level paths
# ----------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]  # .../BMD Implementation

DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"

# ----------------------------------------------------------------------
# User configuration
# ----------------------------------------------------------------------
DATASET_NAME = "IAS"  # BIRDS / HABITATS / IAS
STUDY_AREA_NAME = "greece_natura"
H3_RESOLUTION = 6

OUT_DIR = PROCESSED_DIR / DATASET_NAME / "gap_bias_surfaces" / STUDY_AREA_NAME
OUT_DIR.mkdir(parents=True, exist_ok=True)

STUDY_AREA_FILE = OUT_DIR / f"{STUDY_AREA_NAME}_study_area.gpkg"
GRID_FILE = OUT_DIR / f"GBIF_{DATASET_NAME}_{STUDY_AREA_NAME}_h3_grid_res{H3_RESOLUTION}.gpkg"

# ----------------------------------------------------------------------
# Load processed study area
# ----------------------------------------------------------------------
if not STUDY_AREA_FILE.exists():
    raise FileNotFoundError(f"Processed study area not found: {STUDY_AREA_FILE}")

print(f"Loading processed study area: {STUDY_AREA_FILE}")

study_area_gdf = gpd.read_file(STUDY_AREA_FILE)

if study_area_gdf.empty:
    raise ValueError("Processed study area is empty.")

if study_area_gdf.crs is None:
    raise ValueError("Processed study area has no CRS defined.")

study_area_gdf = study_area_gdf.to_crs(epsg=4326)

print(f"Loaded {len(study_area_gdf)} study area polygon(s).")

# ----------------------------------------------------------------------
# Merge geometry
# ----------------------------------------------------------------------
study_area_geom = study_area_gdf.geometry.union_all()

if study_area_geom.is_empty:
    raise ValueError("Study area geometry is empty after union.")

# ----------------------------------------------------------------------
# Build study area bounding box
# ----------------------------------------------------------------------
minx, miny, maxx, maxy = study_area_geom.bounds

study_area_bbox = box(minx, miny, maxx, maxy)

print("Study area bounding box:")
print(f"  Latitude : {miny} -> {maxy}")
print(f"  Longitude: {minx} -> {maxx}")

# ----------------------------------------------------------------------
# Generate H3 cells over bounding box
# ----------------------------------------------------------------------
print(f"Generating H3 candidate cells at resolution {H3_RESOLUTION}...")

bbox_h3shape = h3.LatLngPoly([
    (miny, minx),
    (maxy, minx),
    (maxy, maxx),
    (miny, maxx),
    (miny, minx)
])

h3_cells = h3.polygon_to_cells(bbox_h3shape, H3_RESOLUTION)

print(f"Generated {len(h3_cells)} H3 candidate cells.")

# ----------------------------------------------------------------------
# Convert H3 cell ids to polygon geometries
# ----------------------------------------------------------------------
cell_ids = []
cell_polygons = []

for cell_id in h3_cells:
    boundary = h3.cell_to_boundary(cell_id)
    polygon = Polygon([(lng, lat) for lat, lng in boundary])

    cell_ids.append(cell_id)
    cell_polygons.append(polygon)

grid_gdf = gpd.GeoDataFrame(
    {"cell_id": cell_ids},
    geometry=cell_polygons,
    crs="EPSG:4326"
)

# ----------------------------------------------------------------------
# Keep only cells intersecting the study area
# ----------------------------------------------------------------------
grid_gdf = grid_gdf[grid_gdf.intersects(study_area_geom)].copy()

print(f"Retained {len(grid_gdf)} H3 cells intersecting the study area.")

# ----------------------------------------------------------------------
# Export H3 spatial units
# ----------------------------------------------------------------------
grid_gdf.to_file(GRID_FILE, driver="GPKG")

print(f"H3 spatial units saved to: {GRID_FILE}")
print("Spatial unit preparation complete.")