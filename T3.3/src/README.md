# 📁 Source Code (`src/`)

This directory contains all scripts developed for the *Biodiversity Meets Data (BMD)* project.

The codebase is structured to support a fully reproducible pipeline for GBIF data acquisition, processing, and spatial analysis.

---

## Structure

src/  
├── core/  
├── helpers/  
└── subprojects/  

- **core/** → GBIF data acquisition and filtering  
- **subprojects/** → analytical pipelines (e.g. spatial analysis, Natura 2000)  
- **helpers/** → diagnostics, validation, exploratory utilities  

All scripts are:
- dataset-agnostic (BIRDS / HABITATS / IAS)  
- modular and independently executable  
- reproducible with explicit inputs/outputs  

---

# 1. core/ — GBIF Data Preparation

Pipeline for creating clean, analysis-ready GBIF datasets.

## Workflow
1. Download data from GBIF  
2. Apply filtering and validation  
3. Generate summaries and schema checks  

---

## Scripts

### `01_download_gbif.py`
Downloads occurrence data using the GBIF API.

- Builds taxonomic queries (`usageKey`, `acceptedUsageKey`)
- Applies initial filters at query level
- Retrieves Darwin Core Archive (DwC-A)

**Output:** `data/raw/`

---

### `02_filter_gbif_dataset.py`
Applies data-quality filtering.

- Removes invalid records  
- Filters by coordinate quality and uncertainty  
- Standardises schema  

**Output:**
- `data/filtered/`
- `results/filtering/`

---

### `03_filtered_data_summaries.py`
Generates descriptive summaries.

- Species distributions  
- Country distributions  
- Temporal patterns  

**Output:** `results/filtering/<DATASET_NAME>/`

---

### `04_dataset_schema_summary.py`
Performs dataset structure validation.

- Missing values  
- Data types  
- Value ranges  

**Output:** schema summary reports  

---

# 2. subprojects/ — Analytical Pipelines

Contains specialised workflows built on top of the filtered datasets.

---

## gap_bias_surfaces/

Spatial analysis pipeline for assessing data coverage and sampling bias.

### Workflow
1. Define study area  
2. Generate spatial grid (H3)  
3. Aggregate occurrence data  
4. Analyse spatial patterns  

---

### `01_select_study_area.py`
Defines the study area and selects relevant occurrences.

- Supports:
  - user-defined polygons  
  - multiple geometries  
  - attribute-based selections (e.g. Natura 2000)

**Output:**
- Selected occurrences  
- Processed study area  
- Summary report  

---

### `02_prepare_spatial_units.py`
Generates H3 hexagonal grid.

- Grid created over bounding box  
- Cells filtered by intersection  
- Resolution configurable (e.g. H3 res 6, 8)

**Output:** H3 grid  

---

### `03_aggregate_to_spatial_units.py`
Aggregates data at cell level.

- Assigns occurrences to cells  
- Computes:
  - `n_occurrences`
  - `n_species`
- Preserves empty cells  

**Output:**
- Aggregated grid (GPKG, CSV)  
- Summary statistics  
- Spatial visualisations  

---

### `04_spatial_autocorrelation_morans_i.py`
Computes Global Moran’s I.

- Binary analysis → presence/absence clustering  
- Intensity analysis → sampling intensity clustering  

**Output:** statistical summary  

---

### `05_local_morans_i_lisa.py`
Computes Local Moran’s I (LISA).

- Identifies:
  - hotspots (HH)  
  - coldspots (LL)  
  - spatial outliers (HL / LH)  

**Output:**
- Cluster maps (with/without basemap)  
- Cluster summary  

---

## natura_analysis/

Pipeline for integrating GBIF data with Natura 2000 sites.

### `01_validate_spatial_readiness.py`
Validates spatial input datasets.

### `02_prepare_natura_sites.py`
Standardises Natura 2000 polygons.

### `03_spatial_join_gbif_natura.py`
Assigns GBIF records to Natura sites.

### `03b_uncertainty_from_baseline_gbif_natura.py`
Adds uncertainty-aware classification.

### `04_baseline_basic_metrics_gbif_natura.py`
Computes basic metrics.

### `05_baseline_advanced_metrics_gbif_natura.py`
Computes advanced ecological metrics.

### `06_uncertainty_metrics_gbif_natura.py`
Computes uncertainty-aware statistics.

---

# 3. helpers/

Utility scripts for diagnostics and validation.

### Includes:
- pipeline validation checks  
- spatial consistency checks  
- exploratory analysis tools  

---

# Notes

- All pipelines use `gbifID` as a unique identifier  
- CRS standard: **EPSG:4326**  
- Spatial grid: **H3 hexagonal system**  
- Pipeline is fully modular and can be run step-by-step  

---

# Summary

The `src/` directory implements a complete workflow from raw GBIF data to advanced spatial analysis, enabling:

- data harmonisation  
- spatial aggregation  
- bias detection  
- statistical validation  