# Section 4 driver: highest-quality research exhibits.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript S4_Quality.R
# Inputs:  cleaned published-predictor data, the chapter-1 denominator cache,
#          chapter-3 raw and factor-adjusted benchmark contracts
# Outputs: Section 4 journal-ranking and renowned-match tables under ../Results

source("helpers/driver.R")

required_files <- c(
  "../Data/Processed/czsum_allpredictors.RDS",
  "../Data/Processed/czret_keeponly.RDS",
  "../Data/Processed/raw_dm_benchmarks.RDS",
  "../Data/Processed/factor_adjusted_dm_benchmarks.RDS",
  "DataInput/SignalsTheoryChecked.csv",
  "../Data/Processed/dmcomp_sumstats.RDS",
  longshort_path()
)
require_files(required_files, "Section 4")
exit_if_preflight_only("Section 4")

run_script("S4a_ByJournal.R", "Section 4")
run_script("S4b_RenownedMatches.R", "Section 4")
