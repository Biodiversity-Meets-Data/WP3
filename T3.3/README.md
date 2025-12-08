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

---

## Project structure (high level)

```text
BMD Implementation/
├── data/
│   ├── raw/               # Original GBIF downloads (DwC-A ZIPs)
│   ├── filtered/          # Filtered occurrence tables (CSV)
│   └── processed/         # (reserved) Intermediate derived data
│
├── results/
│   ├── filtering/         # Summary reports for filtered GBIF datasets
│   └── natura2k/          # (reserved) Outputs from Natura 2000 analyses
│
├── logs/
│   ├── natura2k/
│   │   └── 01_validate_spatial_input/   # Spatial validation logs
│   └── ... (future log directories)
│
├── src/
│   ├── core/
│   │   ├── 01_download_gbif.py
│   │   ├── 02_filter_gbif_dataset.py
│   │   └── config/        # Species lists (CSV) with usageKey / acceptedUsageKey
│   └── subprojects/
│       └── natura2k/
│           └── 01_validate_spatial_input.py
└── 