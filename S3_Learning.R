# Section 3 driver: learning exhibits.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript S3_Learning.R
# Inputs:  chapter-3 raw_dm_benchmarks.RDS
# Outputs: component and manuscript-layout MP-style regression tables under
#          ../Results
#
# S3a_MPStyleDecayModels.R estimates the regression models (including
# fixed-effect specifications); S3b_MPStyleDecayTables.R renders their tables.

required_files <- c(
  "../Data/Processed/raw_dm_benchmarks.RDS"
)

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
