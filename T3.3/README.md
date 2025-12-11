# Biodiversity Meets Data (BMD)
## Work Package: WP3 - Data harmonisation in Space-Time-Taxonomy, Data gaps and biases
## Task: T3.3 - Data gap and bias surfaces

This repository contains the core scripts and subprojects used to download,
filter, validate, and analyse GBIF occurrence data for the
**Biodiversity Meets Data (BMD) - Τ3.3** project.

The current implementation focuses on:

- Downloading GBIF occurrences for directive-based species lists (Birds, Habitats, IAS).
- Applying robust data-quality filters to generate analysis-ready datasets.
- Validating spatial inputs as a prerequisite for the Natura 2000 analysis pipeline.
- Downloading GBIF occurrence data for Birds, Habitats and IAS species lists.
- Applying reproducible and robust data-quality filters to create clean, analysis-ready datasets.
- Validating filtered datasets for spatial consistency (coordinate checks, structure checks).
- Preparing and standardising Natura2k polygon layers (harmonised fields, CRS unification).
- Performing spatial joins between GBIF occurrences and Natura2k sites.
- Computing basic metrics per Natura site and Member State (occurrences, species richness, temporal coverage).
- Computing advanced metrics, including species-level metrics, site-type metrics, and temporal-gap analysis.
---

## Project structure (high level)

```text
BMD Implementation/
├── data/
│   ├── external/           # External reference data (e.g. Natura2k polygons, EU layers)
│   ├── raw/                # Original GBIF downloads (DwC-A ZIPs)
│   ├── filtered/           # Filtered occurrence tables (CSV)
│   └── processed/          # Intermediate derived data (e.g. prepared Natura2k, spatial joins)
│
├── results/
│   ├── filtering/          # Summary reports for filtered GBIF datasets
│   └── natura2k/           # Outputs from Natura 2000 analyses (sites/MS/species/gaps)
│
├── logs/
│   └── natura2k/
│       ├── 01_validate_spatial_input/        # Spatial validation logs
│       ├── 02_prepare_natura_sites/          # Natura2k preparation logs
│       ├── 03_spatial_join_gbif_natura/      # Spatial join logs
│       ├── 04_basic_metrics_gbif_natura/     # Basic metrics logs
│       └── 05_advanced_metrics_gbif_natura/  # Advanced metrics logs
│
├── src/
│   ├── core/
│   │   ├── 01_download_gbif.py
│   │   ├── 02_filter_gbif_dataset.py
│   │   └── config/        # Species lists (CSV) with usageKey / acceptedUsageKey
│   └── subprojects/
│       └── natura2k/
│           ├── 01_validate_spatial_input.py
│           ├── 02_prepare_natura_sites.py
│           ├── 03_spatial_join_gbif_natura.py
│           ├── 04_basic_metrics_gbif_natura.py
│           └── 05_advanced_metrics_gbif_natura.py
└── environment.yml        # Conda environment specification