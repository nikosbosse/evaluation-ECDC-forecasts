# load truth data using the covidHubutils package ------------------------------
devtools::install_github("reichlab/covidHubUtils", force = FALSE)
library(covidHubUtils)
library(dplyr)
library(tidyr)
library(data.table)
library(here)
library(purrr)
library(stringr)
library(scoringutils)
library(magrittr)

truth <- covidHubUtils::load_truth(hub = "ECDC") |>
  filter(target_variable %in% c("inc case", "inc death")) |>
  mutate(target_variable = ifelse(target_variable == "inc case",
                                  "Cases", "Deaths")) |>
  rename(target_type = target_variable,
         true_value = value) |>
  select(-model)

fwrite(truth, "data/weekly-truth-Europe.csv")

# get the correct file paths to all forecasts ----------------------------------
folders <- here("data-processed", list.files("data-processed"))
folders <- folders[
  !(grepl("\\.R", folders) | grepl(".sh", folders) | grepl(".csv", folders))
]

file_paths <- purrr::map(folders,
                         .f = function(folder) {
                           files <- list.files(folder)
                           out <- here::here(folder, files)
                           return(out)}) %>%
  unlist()
file_paths <- file_paths[grepl(".csv", file_paths)]

# load all past forecasts ------------------------------------------------------
# ceate a helper function to get model name from a file path
get_model_name <- function(file_path) {
  split <- str_split(file_path, pattern = "/")[[1]]
  model <- split[length(split) - 1]
  return(model)
}

# load forecasts
prediction_data <- map_dfr(file_paths,
                           .f = function(file_path) {
                             data <- fread(file_path)
                             data[, `:=`(
                               target_end_date = as.Date(target_end_date),
                               quantile = as.numeric(quantile),
                               forecast_date = as.Date(forecast_date),
                               model = get_model_name(file_path)
                             )]
                             return(data)
                           }) %>%
  filter(grepl("case", target) | grepl("death", target)) %>%
  mutate(target_type = ifelse(grepl("death", target),
                              "Deaths", "Cases"),
         horizon = as.numeric(substr(target, 1, 1))) %>%
  rename(prediction = value) %>%
  filter(type == "quantile",
         grepl("inc", target)) %>%
  select(location, forecast_date, quantile, prediction,
         model, target_end_date, target, target_type, horizon)

# merge forecast data and truth data and save
hub_data <- merge_pred_and_obs(prediction_data, truth,
                               by = c("location", "target_end_date",
                                      "target_type")) |>
  filter(target_end_date >= "2021-01-01") |>
  select(-location_name, -target)

# harmonise forecast dates to be the date a submission was made
hub_data <- mutate(hub_data,
                   forecast_date = calc_submission_due_date(forecast_date))

# function that performs some basic filtering to clean the data
filter_hub_data <- function(hub_data) {

  # define the unit of a single forecast
  unit_observation <- c("location", "forecast_date", "target_end_date", "horizon", "model", "target_type", "population")

  h <- hub_data |>
    # filter out unnecessary horizons and dates
    filter(horizon <= 4,
           forecast_date > "2021-03-08") |>
    # filter out all models that don't have all quantiles
    group_by_at(unit_observation) |>
    mutate(n = n()) |>
    ungroup() |>
    filter(n == max(n)) |>
    # filter out models that don't have all horizons
    group_by_at(c(unit_observation, "quantile")) |>
    ungroup(horizon, target_end_date) |>
    mutate(n = length(unique(horizon))) |>
    ungroup() |>
    filter(n == max(n))

  return(h)
}

hub_data <- filter_hub_data(hub_data)

# split forecast data into two to reduce file size
split <- floor(nrow(hub_data) / 3)

fwrite(hub_data[1:split, ],
       file = "data/full-data-european-forecast-hub-1.csv")
fwrite(hub_data[(split + 1):(2 * split), ],
       file = "data/full-data-european-forecast-hub-2.csv")
fwrite(hub_data[(2 *split + 1):(nrow(hub_data)), ],
       file = "data/full-data-european-forecast-hub-3.csv")
