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

source("helpers/driver.R")

required_files <- c(
  "../Data/Processed/raw_dm_benchmarks.RDS"
)

require_files(required_files, "Section 3", "Run 3_Precompute.R first.")
exit_if_preflight_only("Section 3")

run_script("S3a_MPStyleDecayModels.R", "Section 3")
run_script("S3b_MPStyleDecayTables.R", "Section 3")
