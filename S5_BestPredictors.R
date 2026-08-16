# Section 5 driver: renowned research versus data mining exhibits.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript S5_BestPredictors.R
# Inputs:  cleaned published returns and chapter-3 mined-strategy pair catalog
# Outputs: Section 5 inspect-*.tex tables under ../Results

settings_env <- new.env(parent = globalenv())
sys.source("config.R", envir = settings_env)
version_prefix <- file.path(
  "../Data/Processed", settings_env$globalSettings$dataVersion
)
required_files <- c(
  "../Data/Processed/czsum_allpredictors.RDS",
  "../Data/Processed/czret_keeponly.RDS",
  paste0(version_prefix, " LongShort.RData"),
  "../Data/Processed/dmcomp_sumstats.RDS"
)
rm(settings_env)

missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing Section 5 input(s): ", paste(missing_files, collapse = ", "),
    ". Run the required upstream chapter first."
  )
}
if ("--preflight-only" %in% commandArgs(trailingOnly = TRUE)) {
  message("Section 5 preflight passed.")
  quit(save = "no", status = 0)
}

run_script <- function(path) {
  message("\n--- Section 5: ", path, " ---")
  status <- system2(file.path(R.home("bin"), "Rscript"), path)
  if (!identical(status, 0L)) {
    stop("Section 5 script failed (exit ", status, "): ", path)
  }
}

run_script("S5a_InspectTables.R")
