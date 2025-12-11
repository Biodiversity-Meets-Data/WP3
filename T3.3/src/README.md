# Source Code Directory (`src/`)

This directory contains all scripts used in the *Biodiversity Meets Data (BMD)* project.
The codebase is organised into two main components:

- **core/** — generic GBIF data acquisition and filtering
- **subprojects/** — specialised analytical pipelines (e.g., Natura2k)

Each script is modular, reproducible, and designed to run independently.

---

## 1. `core/` — GBIF Download, Filtering, Configuration

The `core/` folder contains scripts and configuration files for creating
clean, analysis-ready GBIF datasets.

### Scripts

#### **01_download_gbif.py**
Automates the download of GBIF occurrence data using the GBIF API.
- Reads species lists from `config/`
- Submits a GBIF download request
- Monitors job status and retrieves the final DwC-A ZIP file  
Output stored in `data/raw/`.

#### **02_filter_gbif_dataset.py**
Processes raw GBIF downloads and applies robust data-quality filters.
- Removes incomplete or low-quality records
- Applies coordinate and basis-of-record rules
- Generates a filtered CSV + summary report  
Outputs stored in `data/filtered/` and `results/filtering/`.

### Configuration

#### **config/**
Contains all species lists used for GBIF download requests.  
Each CSV includes usageKey or acceptedUsageKey fields.

---

## 2. `subprojects/` — Thematic Analysis Pipelines

The `subprojects/` directory hosts specialised workflows.  
Currently implemented:

---

## Natura2k Pipeline (`subprojects/natura2k/`)

This pipeline integrates GBIF occurrence data with Natura 2000 polygon datasets,
performs spatial joins, and produces ecological metrics.

### Scripts

#### **01_validate_spatial_input.py**
Validates filtered GBIF datasets before spatial analysis.
- Checks required columns
- Detects missing or invalid coordinates
- Produces a log file with all validation results

#### **02_prepare_natura_sites.py**
Loads and standardises Natura 2000 polygon datasets.
- Converts geometries to WGS84 (EPSG:4326)
- Keeps essential fields (SITECODE, SITENAME, MS, SITETYPE)
- Produces a single clean `.gpkg` for downstream use  
Output stored in `data/processed/natura2k/`.

#### **03_spatial_join_gbif_natura.py**
Performs a spatial join between GBIF points and Natura 2000 sites.
- Uses prepared Natura polygons
- Attaches SITECODE, SITENAME, MS, SITETYPE to each GBIF occurrence  
Result stored in `data/processed/<DATASET_NAME>/`.

#### **04_basic_metrics_gbif_natura.py**
Computes site-level and Member-State-level summary statistics.
- Occurrence counts
- Species richness
- Temporal ranges  
Outputs stored in `results/natura2k/<DATASET_NAME>/`.

#### **05_advanced_metrics_gbif_natura.py**
Generates higher-level ecological indicators.
- Species-level metrics
- Site-type level metrics
- Temporal gap detection per Natura site  
Outputs stored in `results/natura2k/<DATASET_NAME>/`.

---

## Notes

- All scripts are dataset-agnostic (IAS / BIRDS / HABITATS) via `DATASET_NAME`.
- Logging for each step is stored under `logs/natura2k/`.
- Raw data are never stored in this directory; only Python scripts and config files.