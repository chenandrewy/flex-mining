# Chapter 5 driver: learning exhibits.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript 5_Learning.R
# Inputs:  ../Data/Processed/mp_style_decay_models.RDS
# Outputs: MP-style regression tables under ../Results
#
# The regression models, including fixed-effect specifications, are estimated
# by 3f_MPStyleDecayModels.R. This chapter only renders their table outputs.

required_files <- "../Data/Processed/mp_style_decay_models.RDS"
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

run_script("4c8_MPStyleDecayTables.R")
