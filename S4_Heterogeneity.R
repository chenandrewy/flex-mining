# Section 4 driver: heterogeneous research and outperformance exhibits.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript S4_Heterogeneity.R
# Inputs:  cleaned published-predictor data and chapter-2 mined strategies
# Outputs: Section 4 category and journal tables under ../Results

required_files <- c(
  "../Data/Processed/czsum_allpredictors.RDS",
  "../Data/Processed/czret_keeponly.RDS"
)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing Section 4 input(s): ", paste(missing_files, collapse = ", "),
    ". Run the required upstream chapter first."
  )
}

run_script <- function(path) {
  message("\n--- Section 4: ", path, " ---")
  status <- system2(file.path(R.home("bin"), "Rscript"), path)
  if (!identical(status, 0L)) {
    stop("Section 4 script failed (exit ", status, "): ", path)
  }
}

run_script("S4a_DataCounts.R")
