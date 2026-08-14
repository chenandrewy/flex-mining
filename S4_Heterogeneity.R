# Section 4 driver: heterogeneous research and outperformance exhibits.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript S4_Heterogeneity.R
# Inputs:  cleaned published-predictor data, the chapter-1 denominator cache,
#          chapter-2 mined strategies and matched risk-adjustment cache, and
#          factor returns
# Outputs: Section 4 category, journal, and risk-adjusted tables under ../Results

settings_env <- new.env(parent = globalenv())
sys.source("config.R", envir = settings_env)
version_prefix <- file.path("../Data/Processed", settings_env$globalSettings$dataVersion)
required_files <- c(
  "../Data/Processed/czsum_allpredictors.RDS",
  "../Data/Processed/czret_keeponly.RDS",
  "../Data/Processed/ret_for_plot0.RDS",
  paste0(version_prefix, " MatchPubRiskAdjusted.RData"),
  "../Data/Raw/FamaFrenchFactors.RData",
  "DataInput/SignalsTheoryChecked.csv",
  paste0(version_prefix, " LongShort.RData"),
  "DataIntermediate/freq_obs_1963.csv"
)
rm(settings_env)

missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing Section 4 input(s): ", paste(missing_files, collapse = ", "),
    ". Run the required upstream chapter first."
  )
}
if ("--preflight-only" %in% commandArgs(trailingOnly = TRUE)) {
  message("Section 4 preflight passed.")
  quit(save = "no", status = 0)
}

run_script <- function(path) {
  message("\n--- Section 4: ", path, " ---")
  status <- system2(file.path(R.home("bin"), "Rscript"), path)
  if (!identical(status, 0L)) {
    stop("Section 4 script failed (exit ", status, "): ", path)
  }
}

run_script("S4a_DataCounts.R")
run_script("S4b_RVsDM_ByGroup.R")
