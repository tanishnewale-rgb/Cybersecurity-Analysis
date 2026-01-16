# munge.R
# Purpose: Data ingestion and cleaning for MOOC analysis
# Author: Tanish Vikas Newale

library(dplyr)
library(purrr)
library(readr)
library(stringr)
library(ProjectTemplate)



message("Files found in data folder:")
data_files <- list.files("data", full.names = TRUE)
print(basename(data_files))

csv_files <- data_files[str_detect(data_files, "\\.csv$")]

if (length(csv_files) == 0) {
  stop("No CSV files found in data folder")
}



enrolment_files <- csv_files[str_detect(
  tolower(csv_files),
  "enrol|enroll|learner|profile|user"
)]

if (length(enrolment_files) == 0) {
  stop("No enrolment-related CSV files detected after pattern matching")
}

all_enrolments <- enrolment_files %>%
  map_df(~read_csv(.x, show_col_types = FALSE))

names(all_enrolments) <- tolower(names(all_enrolments))


if (!"country" %in% names(all_enrolments)) all_enrolments$country <- NA
if (!"age" %in% names(all_enrolments)) all_enrolments$age <- NA
if (!"gender" %in% names(all_enrolments)) all_enrolments$gender <- NA

all_enrolments <- all_enrolments %>%
  mutate(
    country = ifelse(is.na(country) | country == "", "Unknown", country),
    age = ifelse(is.na(age) | age == "", "Unknown", age),
    gender = ifelse(is.na(gender) | gender == "", "Unknown", gender)
  )



step_files <- csv_files[str_detect(tolower(csv_files), "step|activity|progress")]

if (length(step_files) > 0) {
  all_steps <- step_files %>%
    map_df(~read_csv(.x, show_col_types = FALSE))
  
  names(all_steps) <- tolower(names(all_steps))
  
  
  possible_names <- c("last_completed_at", "last_completed", "count", "completed_at")
  found_col <- names(all_steps)[names(all_steps) %in% possible_names][1]
  
  if (!is.na(found_col)) {
    all_steps <- all_steps %>% rename(last_completed_at_count = !!found_col)
  }
  
  
  all_steps <- all_steps %>%
    mutate(
      week_number = as.numeric(week_number),
      last_completed_at_count = as.numeric(last_completed_at_count)
    )
}


save(all_enrolments, all_steps, file = "processed_data.RData")




save(all_enrolments, all_steps, file = "data/processed_data.RData")

message("munge.R completed successfully. Data saved to data/processed_data.RData")