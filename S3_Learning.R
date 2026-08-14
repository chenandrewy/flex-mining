# Section 3 driver: learning exhibits.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript S3_Learning.R
# Inputs:  chapter-3 matched_uncorr_benchmark.RDS and the versioned
#          LongShort.RData
# Outputs: component and manuscript-layout MP-style regression tables under
#          ../Results
#
# S3a_MPStyleDecayModels.R estimates the regression models (including
# fixed-effect specifications); S3b_MPStyleDecayTables.R renders their tables.

settings_env <- new.env(parent = globalenv())
sys.source("config.R", envir = settings_env)
version_prefix <- file.path("../Data/Processed", settings_env$globalSettings$dataVersion)
required_files <- c(
  "../Data/Processed/matched_uncorr_benchmark.RDS",
  paste0(version_prefix, " LongShort.RData")
)
rm(settings_env)

missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing Section 3 input(s): ", paste(missing_files, collapse = ", "),
    ". Run 3_Precompute.R first."
  )
}

run_script <- function(path) {
  message("\n--- Section 3: ", path, " ---")
  status <- system2(file.path(R.home("bin"), "Rscript"), path)
  if (!identical(status, 0L)) {
    stop("Section 3 script failed (exit ", status, "): ", path)
  }
}

run_script("S3a_MPStyleDecayModels.R")
run_script("S3b_MPStyleDecayTables.R")
