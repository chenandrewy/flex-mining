# Main script for rebuilding the data and paper exhibits.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript MAIN.R
#
# Nine chapters separated at explicit cache and paper-section boundaries:
#
#   1_Download_and_Clean  External acquisition and cleaning; new data vintage.
#   2_DataMining          Construct and match mined strategies; about two hours.
#   3_Precompute          Reusable correlations, PCA, panels, and summaries.
#   4_ResearchVsDataMining  Introduction and Section 2 exhibits.
#   5_Learning              Section 3 regression tables.
#   6_Heterogeneity         Section 4 exhibits.
#   7_BestPredictors        Section 4b exhibits.
#   8_Appendices            Appendix-only exhibits.
#   9_ExportDataToCsv       Shared-data CSV exports.
#
# Iterating on an exhibit normally means running only its chapter. Changes to
# matching or statistical analysis require chapter 3; changes to mined signals
# require chapter 2.
#
# Chapter 1 pulls fresh data from WRDS and Google Drive and OVERWRITES
# ../Data/Raw.
# WRDS is not versioned and offers no as-of retrieval, so setting
# run_download_and_clean = TRUE replaces the current data vintage irreversibly
# and every downstream result moves with it. Archive ../Data/Raw first.
#
# Inputs:  ../Data/Raw (re-created when run_download_and_clean = TRUE)
# Outputs: ../Data/Processed, ../Data/Export, ../Results
#
# Paper contract: Chapters 4-8 rebuild paper exhibits from upstream caches.
# Chapter 5 renders the cached MP regressions; the Section 3 presentation tables
# are being migrated from hand formatting to direct R output.
# See docs/journal/0813c,map,exhibits.md for the script -> exhibit map.

run_download_and_clean       <- FALSE  # Re-pull ../Data/Raw; changes the vintage
run_data_mining              <- FALSE  # Chapter 2; hours
run_precompute               <- TRUE   # Chapter 3; slow reusable analysis
run_research_vs_data_mining  <- TRUE   # Chapter 4; intro and Section 2
run_learning                 <- TRUE   # Chapter 5; Section 3
run_heterogeneity            <- TRUE   # Chapter 6; Section 4
run_best_predictors          <- TRUE   # Chapter 7; Section 4b
run_appendices               <- TRUE   # Chapter 8
run_export_data_to_csv       <- TRUE   # Chapter 9

run_script <- function(path) {
  message("\n=== Running ", path, " ===")
  status <- system2(file.path(R.home("bin"), "Rscript"), path)
  if (!identical(status, 0L)) {
    stop("Pipeline chapter failed (exit ", status, "): ", path)
  }
}

# Chapter 1: acquisition and cleaning ------------------------------------

if (run_download_and_clean) {
  run_script("1_Download_and_Clean.R")
  run_script("1a_ValidDenoms.R")
}

# Chapter 2: mined-strategy construction ---------------------------------

if (run_data_mining) {
  run_script("2_DataMining.R")
}

# Chapter 3: reusable analysis caches ------------------------------------

if (run_precompute) {
  run_script("3_Precompute.R")
}

# Chapter 4: research versus data mining ---------------------------------

if (run_research_vs_data_mining) {
  run_script("4_ResearchVsDataMining.R")
}

# Chapter 5: learning -----------------------------------------------------

if (run_learning) {
  run_script("5_Learning.R")
}

# Chapter 6: heterogeneity ------------------------------------------------

if (run_heterogeneity) {
  run_script("6_Heterogeneity.R")
}

# Chapter 7: best predictors ---------------------------------------------

if (run_best_predictors) {
  run_script("7_BestPredictors.R")
}

# Chapter 8: appendices ---------------------------------------------------

if (run_appendices) {
  run_script("8_Appendices.R")
}

# Chapter 9: data exports -------------------------------------------------

if (run_export_data_to_csv) {
  run_script("9_ExportDataToCsv.R")
}
