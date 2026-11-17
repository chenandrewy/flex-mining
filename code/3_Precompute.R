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
source("helpers/driver.R")

required_files <- c(
  "../Data/Processed/czsum_allpredictors.RDS",
  "../Data/Processed/czret_keeponly.RDS",
  longshort_path(),
  "../Data/Raw/FamaFrenchFactors.RData",
  "../Data/Processed/ticker_Harvey2017JF.RDS"
)
require_files(
  required_files, "chapter-1/2 cache",
  "Run the required upstream chapter before 3_Precompute.R."
)
exit_if_preflight_only("Chapter 3")

run_script("3a_PrepDMBenchmarks.R", "Chapter 3")
run_script("3b_DataMiningSummary.R", "Chapter 3")
run_script("3c_FactorAdjustedDMPrep.R", "Chapter 3")
run_script("3d_DMCorrelationsPCA.R", "Chapter 3")
run_script("3e_DMSpanPCA.R", "Chapter 3")
