# Chapter 3 driver: reusable analysis precomputation.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript 3_Precompute.R
# Inputs:  cleaned chapter-1 inputs and chapter-2 mined-strategy caches
# Outputs: exhibit-ready caches under ../Data/Processed
#
# This chapter may take minutes to hours. It does not intentionally write paper
# exhibits; rerun it only when data, matching rules, or statistical analysis
# changes.

# Resolve versioned paths from the project settings.
settings_env <- new.env(parent = globalenv())
sys.source("config.R", envir = settings_env)
version_prefix <- file.path("../Data/Processed", settings_env$globalSettings$dataVersion)
required_files <- c(
  "../Data/Processed/czsum_allpredictors.RDS",
  "../Data/Processed/czret_keeponly.RDS",
  paste0(version_prefix, " LongShort.RData"),
  "../Data/Raw/FamaFrenchFactors.RData",
  "../Data/Processed/ticker_Harvey2017JF.RDS"
)
rm(settings_env)

missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing chapter-1/2 cache(s): ", paste(missing_files, collapse = ", "),
    ". Run the required upstream chapter before 3_Precompute.R."
  )
}
if ("--preflight-only" %in% commandArgs(trailingOnly = TRUE)) {
  message("Chapter 3 preflight passed.")
  quit(save = "no", status = 0)
}

run_script <- function(path) {
  message("\n--- Chapter 3: ", path, " ---")
  status <- system2(file.path(R.home("bin"), "Rscript"), path)
  if (!identical(status, 0L)) {
    stop("Chapter 3 script failed (exit ", status, "): ", path)
  }
}

run_script("3a_PrepDMBenchmarks.R")
run_script("3b_DataMiningSummary.R")
run_script("3c_FactorAdjustedDMPrep.R")
