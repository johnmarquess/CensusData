# Census Data Preprocessing

Preprocessing pipeline for Australian Bureau of Statistics (ABS) 2021 Census data, filtered for Queensland. Creates fully denormalized datasets optimized for analysis and Shiny applications.

## Overview

This project transforms raw ABS Census 2021 data from a normalized star-schema format into denormalized, analysis-ready datasets. The pipeline:

- Loads census fact tables and dimension/lookup tables from Parquet files
- Filters data for Queensland (State Code = 3)
- Joins all dimensions to create fully denormalized datasets
- Enriches geography with SA2 → SA3 → SA4 hierarchy and PHN (Primary Health Network) mappings
- Standardizes labels and handles special codes

## Data Sources

### Input Data

The pipeline expects the following files in the `data/` directory:

#### Fact Tables (Parquet)
| File | Description |
|------|-------------|
| `c21_g01_sa2_population.parquet` | Population counts by geography, age, and sex (G01) |
| `c21_g19_sa2_health_conditions.parquet` | Long-term health conditions data (G19) |

#### Lookup/Dimension Tables (Parquet)
| File | Description |
|------|-------------|
| `c21_g01_sa2_geo_lookup.parquet` | Geographic area codes and names |
| `c21_g01_sa2_state_lookup.parquet` | State codes and names |
| `c21_g01_sa2_age_lookup.parquet` | Age group codes and labels |
| `c21_g01_sa2_sex_lookup.parquet` | Sex codes and labels |
| `c21_g01_sa2_geog_type_lookup.parquet` | Geography type codes (SA2, SA3, SA4) |
| `c21_g19_sa2_health_condition_lookup.parquet` | Long-term health condition codes |
| `common_health_condition_lookup.parquet` | Common health condition reference |

#### Geographic Reference (CSV)
| File | Description |
|------|-------------|
| `geo_correspondence_2021.csv` | SA2 → SA3 → SA4 → LGA correspondence |
| `phn_2023_to_SA2_2021.csv` | Primary Health Network to SA2 mapping |

### Output Data

Processed files are saved to `data/processed/`:

| File | Description |
|------|-------------|
| `qld_health_analysis.parquet` | Fully denormalized health conditions dataset |
| `qld_geo_reference.parquet` | Geography reference lookup (SA2, SA3, SA4) |
| `qld_lookups.rds` | R list containing all lookup tables for Shiny dropdowns |

## Output Schema

The `qld_health_analysis.parquet` dataset includes:

| Column | Description |
|--------|-------------|
| `year` | Census year |
| `state_name` | State name (Queensland) |
| `geog_type_name` | Geography level (SA2, SA3, SA4) |
| `geog_id` | Geographic area code |
| `sa4_code`, `sa4_name` | Statistical Area Level 4 |
| `sa3_code`, `sa3_name` | Statistical Area Level 3 |
| `sa2_code`, `sa2_name` | Statistical Area Level 2 |
| `PHN_NAME_2023` | Primary Health Network name |
| `sex_name` | Sex label |
| `age_group`, `age_group_label` | Age group code and standardized label |
| `long_term_health_condition` | Health condition name |
| `persons` | Count of persons |

## Requirements

### R Packages

```r
install.packages(c("arrow", "dplyr", "tidyr", "readr", "here"))
```

## Usage

Run the preprocessing script from the project root:

```r
source("R/preprocess_census_data.R")
```

The script provides progress messages and a summary of the processed data.

## Data Transformations

### Age Group Label Standardization

Age group labels are standardized to a consistent `nn-nn years` format:

| Original | Standardized |
|----------|--------------|
| `Age groups: 25-34 years` | `25-34 years` |
| `Age groups: 35-44 years` | `35-44 years` |
| `0-14 years` | `0-14 years` |

### Special Health Condition Codes

The pipeline handles special LTHC (Long-Term Health Condition) codes not in the standard lookup:

| Code | Label |
|------|-------|
| `_T` | Total Persons |
| `_N` | Not Stated |

### Geographic Hierarchy

SA2 codes are used to derive parent geographies:
- **SA4**: First 3 digits of SA2 code
- **SA3**: First 5 digits of SA2 code
- **SA2**: Full 9-digit code

## License

Data sourced from the Australian Bureau of Statistics under Creative Commons Attribution 4.0 International licence.
