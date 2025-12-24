# ============================================================================
# Census Data Preprocessing Script
# Australian Bureau of Statistics 2021 Census Data
# Filtered for Queensland (State Code = 3)
# Creates fully denormalized datasets for Shiny app
# ============================================================================

# Load required packages
library(arrow)
library(dplyr)
library(tidyr)
library(readr)

# ============================================================================
# Set up paths
# ============================================================================

data_path <- here::here("data")

# ============================================================================
# Import Lookup/Dimension Tables
# ============================================================================

message("Loading lookup tables...")

# Geographic lookup
geo_lookup <- read_parquet(file.path(data_path, "c21_g01_sa2_geo_lookup.parquet"))
message("  - Geographic lookup: ", nrow(geo_lookup), " rows")

# State lookup
state_lookup <- read_parquet(file.path(data_path, "c21_g01_sa2_state_lookup.parquet"))
message("  - State lookup: ", nrow(state_lookup), " rows")

# Age lookup
age_lookup <- read_parquet(file.path(data_path, "c21_g01_sa2_age_lookup.parquet"))
message("  - Age lookup: ", nrow(age_lookup), " rows")

# Sex lookup
sex_lookup <- read_parquet(file.path(data_path, "c21_g01_sa2_sex_lookup.parquet"))
message("  - Sex lookup: ", nrow(sex_lookup), " rows")

# Geography type lookup
geog_type_lookup <- read_parquet(file.path(data_path, "c21_g01_sa2_geog_type_lookup.parquet"))
message("  - Geography type lookup: ", nrow(geog_type_lookup), " rows")

# Health condition lookups
health_condition_lookup <- read_parquet(file.path(data_path, "c21_g19_sa2_health_condition_lookup.parquet"))
message("  - Health condition lookup: ", nrow(health_condition_lookup), " rows")

common_health_lookup <- read_parquet(file.path(data_path, "common_health_condition_lookup.parquet"))
message("  - Common health condition lookup: ", nrow(common_health_lookup), " rows")

# Geographic correspondence (SA2 -> SA3 -> SA4 -> LGA mappings)
geo_correspondence <- read_csv(
    file.path(data_path, "geo_correspondence_2021.csv"),
    col_types = cols(.default = "c")
)
message("  - Geographic correspondence: ", nrow(geo_correspondence), " rows")

# PHN to SA2 mapping
phn_mapping <- read_csv(
    file.path(data_path, "phn_2023_to_SA2_2021.csv"),
    col_select = c(SA2_CODE_2021, SA2_NAME_2021, PHN_NAME_2023),
    col_types = cols(.default = col_character())
)
message("  - PHN to SA2 mapping: ", nrow(phn_mapping), " rows")

# ============================================================================
# Import Fact Tables
# ============================================================================

message("\nLoading fact tables...")

# Population data (G01)
population_raw <- read_parquet(file.path(data_path, "c21_g01_sa2_population.parquet"))
message("  - Population data: ", nrow(population_raw), " rows")

# Health conditions data (G19)
health_conditions_raw <- read_parquet(file.path(data_path, "c21_g19_sa2_health_conditions.parquet"))
message("  - Health conditions data: ", nrow(health_conditions_raw), " rows")

# ============================================================================
# Examine data structures
# ============================================================================

message("\nExamining data structures...")
message("Population columns: ", paste(names(population_raw), collapse = ", "))
message("Health conditions columns: ", paste(names(health_conditions_raw), collapse = ", "))
message("Geo lookup columns: ", paste(names(geo_lookup), collapse = ", "))
message("Age lookup columns: ", paste(names(age_lookup), collapse = ", "))
message("Sex lookup columns: ", paste(names(sex_lookup), collapse = ", "))
message("Health condition lookup columns: ", paste(names(health_condition_lookup), collapse = ", "))
message("Geog type lookup columns: ", paste(names(geog_type_lookup), collapse = ", "))
message("State lookup columns: ", paste(names(state_lookup), collapse = ", "))

# ============================================================================
# Filter for Queensland (State Code = 3)
# ============================================================================

message("\nFiltering for Queensland (State Code = 3)...")

# Filter population data for Queensland
population_qld <- population_raw %>%
    filter(state == 3)
message("  - QLD Population data: ", nrow(population_qld), " rows")

# Filter health conditions data for Queensland
health_conditions_qld <- health_conditions_raw %>%
    filter(state == 3)
message("  - QLD Health conditions data: ", nrow(health_conditions_qld), " rows")

# Filter geographic lookup for Queensland SA2s only
geo_lookup_qld_sa2 <- geo_lookup %>%
    filter(state == 3, geog_type == "SA2")
message("  - QLD SA2 Geographic lookup: ", nrow(geo_lookup_qld_sa2), " rows")

# ============================================================================
# Handle special LTHC codes (_T = Total, _N = Not Stated)
# These may not exist in the lookup table
# ============================================================================

message("\nHandling special LTHC codes...")

# Check unique lthc values in health conditions data
lthc_codes_in_data <- unique(health_conditions_qld$lthc)
lthc_codes_in_lookup <- unique(health_condition_lookup$lthc)

# Identify codes missing from lookup
missing_lthc_codes <- setdiff(lthc_codes_in_data, lthc_codes_in_lookup)
if (length(missing_lthc_codes) > 0) {
    message("  - LTHC codes in data but not in lookup: ", paste(missing_lthc_codes, collapse = ", "))
}

# Create extended lookup with special codes
special_lthc_codes <- tibble(
    lthc = c("_T", "_N"),
    long_term_health_condition = c("Total Persons", "Not Stated")
)

# Determine the name column in the health condition lookup
lthc_name_col <- names(health_condition_lookup)[!names(health_condition_lookup) %in% "lthc"][1]
message("  - LTHC name column detected: ", lthc_name_col)

# Rename to standardise
health_condition_lookup_std <- health_condition_lookup %>%
    rename(long_term_health_condition = !!sym(lthc_name_col))

# Combine standard lookup with special codes
health_condition_lookup_extended <- bind_rows(
    health_condition_lookup_std,
    special_lthc_codes
) %>%
    distinct(lthc, .keep_all = TRUE)

message("  - Extended health condition lookup: ", nrow(health_condition_lookup_extended), " rows")

# ============================================================================
# Build enhanced geography reference with PHN
# Join SA2 geo_lookup with correspondence and PHN mapping
# ============================================================================

message("\nBuilding geography reference...")

# Determine the geography name column
geo_name_col <- names(geo_lookup)[grepl("name|label", names(geo_lookup), ignore.case = TRUE)][1]
if (is.na(geo_name_col)) {
    geo_name_col <- names(geo_lookup)[!names(geo_lookup) %in% c("geog_id", "geog_type", "state")][1]
}
message("  - Geography name column detected: ", geo_name_col)

# Create a clean geography lookup with standardised column name
geo_lookup_clean <- geo_lookup %>%
    filter(state == 3) %>%
    rename(geography_name = !!sym(geo_name_col)) %>%
    distinct(geog_id, geog_type, .keep_all = TRUE)

message("  - QLD geography lookup: ", nrow(geo_lookup_clean), " rows")
message("  - Geography types: ", paste(unique(geo_lookup_clean$geog_type), collapse = ", "))

# Create separate lookups for SA2, SA3, SA4 for name resolution
sa2_names <- geo_lookup_clean %>%
    filter(geog_type == "SA2") %>%
    select(sa2_code = geog_id, sa2_name = geography_name)

sa3_names <- geo_lookup_clean %>%
    filter(geog_type == "SA3") %>%
    select(sa3_code = geog_id, sa3_name = geography_name)

sa4_names <- geo_lookup_clean %>%
    filter(geog_type == "SA4") %>%
    select(sa4_code = geog_id, sa4_name = geography_name)

message("  - SA2 areas: ", nrow(sa2_names))
message("  - SA3 areas: ", nrow(sa3_names))
message("  - SA4 areas: ", nrow(sa4_names))

# PHN mapping - only applies to SA2 level
phn_lookup <- phn_mapping %>%
    select(SA2_CODE_2021, PHN_NAME_2023) %>%
    distinct(SA2_CODE_2021, .keep_all = TRUE)

message("  - PHN mappings: ", nrow(phn_lookup), " SA2 areas")

# ============================================================================
# Standardise lookup column names
# ============================================================================

message("\nStandardising lookup column names...")

# Determine the name columns in each lookup
# Note: health data uses 'age_group' as key, so we need to identify the label column in age_lookup
age_key_col <- intersect(names(age_lookup), c("age", "age_id", "age_group"))[1]
age_label_col <- names(age_lookup)[!names(age_lookup) %in% c("age", "age_id", "age_group")][1]
sex_name_col <- names(sex_lookup)[!names(sex_lookup) %in% c("sex", "sex_id")][1]
state_name_col <- names(state_lookup)[!names(state_lookup) %in% c("state", "state_id")][1]
geog_type_name_col <- names(geog_type_lookup)[!names(geog_type_lookup) %in% c("geog_type", "geog_type_id")][1]

message("  - Age key column: ", age_key_col)
message("  - Age label column: ", age_label_col)
message("  - Sex name column: ", sex_name_col)
message("  - State name column: ", state_name_col)
message("  - Geog type name column: ", geog_type_name_col)

# Rename columns for consistency
# For age: rename key to age_group (to match health data) and label to age_group_label
# Also standardise age_group_label format to "nn-nn years" (remove "Age groups: " prefix)
age_lookup_std <- age_lookup %>%
    rename(age_group = !!sym(age_key_col)) %>%
    {
        if (!is.na(age_label_col)) rename(., age_group_label = !!sym(age_label_col)) else .
    } %>%
    mutate(
        age_group_label = gsub("^Age groups: ", "", age_group_label)
    ) %>%
    distinct(age_group, .keep_all = TRUE)

sex_lookup_std <- sex_lookup %>%
    rename(sex_name = !!sym(sex_name_col)) %>%
    distinct(sex, .keep_all = TRUE)

state_lookup_std <- state_lookup %>%
    rename(state_name = !!sym(state_name_col))

geog_type_lookup_std <- geog_type_lookup %>%
    rename(geog_type_name = !!sym(geog_type_name_col))

# ============================================================================
# Create fully denormalized health conditions dataset
# ============================================================================

message("\nCreating fully denormalized health conditions dataset...")

# Identify the key columns in health_conditions_qld for joins
message("  - Health conditions columns for joining:")
message("    sex: ", "sex" %in% names(health_conditions_qld))
message("    lthc: ", "lthc" %in% names(health_conditions_qld))
message("    age_group: ", "age_group" %in% names(health_conditions_qld))
message("    geog_id: ", "geog_id" %in% names(health_conditions_qld))
message("    geog_type: ", "geog_type" %in% names(health_conditions_qld))
message("    state: ", "state" %in% names(health_conditions_qld))
message("    year: ", "year" %in% names(health_conditions_qld))
message("    persons: ", "persons" %in% names(health_conditions_qld))

# Build the denormalized dataset
# Geography hierarchy is derived from geog_id:
#   - SA4: 3 digits (e.g., "301")
#   - SA3: 5 digits (e.g., "30101")
#   - SA2: 9 digits (e.g., "301011001")
# The first digits are shared up the hierarchy

health_analysis <- health_conditions_qld %>%
    # Derive SA2, SA3, SA4 codes from geog_id based on geog_type
    mutate(
        # For SA2 records, extract parent SA3 and SA4 codes
        sa2_code = case_when(
            geog_type == "SA2" ~ geog_id,
            TRUE ~ NA_character_
        ),
        sa3_code = case_when(
            geog_type == "SA2" ~ substr(geog_id, 1, 5),
            geog_type == "SA3" ~ geog_id,
            TRUE ~ NA_character_
        ),
        sa4_code = case_when(
            geog_type == "SA2" ~ substr(geog_id, 1, 3),
            geog_type == "SA3" ~ substr(geog_id, 1, 3),
            geog_type == "SA4" ~ geog_id,
            TRUE ~ NA_character_
        )
    ) %>%
    # Join sex lookup
    left_join(sex_lookup_std, by = "sex") %>%
    # Join health condition lookup (extended with special codes)
    left_join(health_condition_lookup_extended, by = "lthc") %>%
    # Join age lookup (health data uses age_group as key)
    left_join(age_lookup_std, by = "age_group") %>%
    # Join state lookup
    left_join(state_lookup_std, by = "state") %>%
    # Join geography type lookup
    left_join(geog_type_lookup_std, by = "geog_type") %>%
    # Join geography names at each level
    left_join(sa2_names, by = "sa2_code") %>%
    left_join(sa3_names, by = "sa3_code") %>%
    left_join(sa4_names, by = "sa4_code") %>%
    # Join PHN mapping (SA2 level only)
    left_join(phn_lookup, by = c("sa2_code" = "SA2_CODE_2021")) %>%
    # Select and order final columns
    select(
        # Identifiers
        year,
        state_name,
        # Geography hierarchy
        geog_type_name,
        geog_id,
        # SA4 level
        sa4_code,
        sa4_name,
        # SA3 level
        sa3_code,
        sa3_name,
        # SA2 level
        sa2_code,
        sa2_name,
        # PHN (only for SA2)
        PHN_NAME_2023,
        # Demographics
        sex_name,
        age_group,
        age_group_label,
        # Health condition
        long_term_health_condition,
        # Measure
        persons,
        # Keep original codes for reference
        state,
        geog_type,
        sex,
        lthc
    )

message("  - Denormalized health dataset: ", nrow(health_analysis), " rows x ", ncol(health_analysis), " columns")

# Check for unmatched lookups
message("\n  Checking join quality:")
message("    - Records with missing sex_name: ", sum(is.na(health_analysis$sex_name)))
message("    - Records with missing age_group: ", sum(is.na(health_analysis$age_group)))
message("    - Records with missing long_term_health_condition: ", sum(is.na(health_analysis$long_term_health_condition)))
# message("    - SA2 records with missing sa2_name: ", sum(is.na(health_analysis$sa2_name[health_analysis$geog_type == \"SA2\"])))
message("    - Records with missing PHN: ", sum(is.na(health_analysis$PHN_NAME_2023)))

# ============================================================================
# Data Summary
# ============================================================================

message("\n============================================")
message("Queensland Census Data Summary")
message("============================================")
message("Health conditions analysis records: ", nrow(health_analysis))
message("Unique SA2 areas: ", n_distinct(health_analysis$geog_id[health_analysis$geog_type_name == "SA2" | health_analysis$geog_type == "SA2"]))
message("Unique health conditions: ", n_distinct(health_analysis$long_term_health_condition))
message("Unique age groups: ", n_distinct(health_analysis$age_group))
message("Unique PHNs: ", n_distinct(health_analysis$PHN_NAME_2023, na.rm = TRUE))
message("============================================")

# ============================================================================
# Preview Data
# ============================================================================

message("\nPreview of denormalized health conditions data:")
print(head(health_analysis, 10))


health_analysis %>% View()
# ============================================================================
# Save processed data
# ============================================================================

# Create output directory if needed
output_path <- here::here("data", "processed")
if (!dir.exists(output_path)) {
    dir.create(output_path, recursive = TRUE)
}

# Save the fully denormalized health analysis dataset
write_parquet(health_analysis, file.path(output_path, "qld_health_analysis.parquet"))
message("\nSaved: qld_health_analysis.parquet")

# Save the geography reference lookup
geo_reference <- bind_rows(
    sa2_names %>% mutate(geog_type = "SA2") %>% rename(geog_id = sa2_code, geography_name = sa2_name),
    sa3_names %>% mutate(geog_type = "SA3") %>% rename(geog_id = sa3_code, geography_name = sa3_name),
    sa4_names %>% mutate(geog_type = "SA4") %>% rename(geog_id = sa4_code, geography_name = sa4_name)
)
write_parquet(geo_reference, file.path(output_path, "qld_geo_reference.parquet"))
message("Saved: qld_geo_reference.parquet")

# Save lookups for reference (useful for Shiny dropdown options)
lookups <- list(
    sex = sex_lookup_std,
    age = age_lookup_std,
    health_condition = health_condition_lookup_extended,
    geog_type = geog_type_lookup_std,
    state = state_lookup_std,
    phn = phn_lookup %>% distinct(PHN_NAME_2023),
    sa4 = sa4_names,
    sa3 = sa3_names,
    sa2 = sa2_names
)
saveRDS(lookups, file.path(output_path, "qld_lookups.rds"))
message("Saved: qld_lookups.rds")

message("\nProcessed data saved to: ", output_path)
message("Preprocessing complete!")

health_analysis %>% count(age_group_label)
