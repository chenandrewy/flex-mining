# Chapter 7 driver: renowned research versus data mining exhibits.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript 7_BestPredictors.R
# Inputs:  cleaned published returns and chapter-2 matched mined strategies
# Outputs: Section 4b inspect-*.tex tables under ../Results

settings_env <- new.env(parent = globalenv())
sys.source("config.R", envir = settings_env)
version_prefix <- file.path(
  "../Data/Processed", settings_env$globalSettings$dataVersion
)
required_files <- c(
  "../Data/Processed/czsum_allpredictors.RDS",
  "../Data/Processed/czret_keeponly.RDS",
  paste0(version_prefix, " LongShort.RData"),
  paste0(version_prefix, " MatchPub.RData")
)
rm(settings_env)

missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing Chapter 7 input(s): ", paste(missing_files, collapse = ", "),
    ". Run the required upstream chapter first."
  )
}

run_script <- function(path) {
  message("\n--- Chapter 7: ", path, " ---")
  status <- system2(file.path(R.home("bin"), "Rscript"), path)
  if (!identical(status, 0L)) {
    stop("Chapter 7 script failed (exit ", status, "): ", path)
  }
}

run_script("4d2_InspectTables.R")
