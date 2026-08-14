# Chapter 5 driver: learning exhibits.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript 5_Learning.R
# Inputs:  chapter-2 mined strategies and chapter-3 panels (ret_for_plot0.RDS,
#          plotdat0.RDS, and the versioned LongShort.RData)
# Outputs: MP-style regression tables under ../Results
#
# 5a_MPStyleDecayModels.R estimates the regression models (including
# fixed-effect specifications); 5b_MPStyleDecayTables.R renders their tables.

settings_env <- new.env(parent = globalenv())
sys.source("config.R", envir = settings_env)
version_prefix <- file.path("../Data/Processed", settings_env$globalSettings$dataVersion)
required_files <- c(
  "../Data/Processed/ret_for_plot0.RDS",
  "../Data/Processed/plotdat0.RDS",
  paste0(version_prefix, " LongShort.RData")
)
rm(settings_env)

missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing Chapter 5 input(s): ", paste(missing_files, collapse = ", "),
    ". Run 3_Precompute.R first."
  )
}

run_script <- function(path) {
  message("\n--- Chapter 5: ", path, " ---")
  status <- system2(file.path(R.home("bin"), "Rscript"), path)
  if (!identical(status, 0L)) {
    stop("Chapter 5 script failed (exit ", status, "): ", path)
  }
}

run_script("5a_MPStyleDecayModels.R")
run_script("5b_MPStyleDecayTables.R")
