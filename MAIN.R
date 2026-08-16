# Main script for rebuilding the data and paper exhibits.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript MAIN.R
#
# Data stages, paper-section exhibit stages, and exports:
#
#   1_Download_and_Clean  External acquisition and cleaning; new data vintage.
#   2_DataMining          Construct and match mined strategies; about two hours.
#   3_Precompute          Reusable correlations, PCA, panels, and summaries.
#   S2_ResearchVsDataMining Introduction and Section 2 exhibits.
#   S3_Learning             Section 3 regression tables.
#   S4_Heterogeneity        Section 4 exhibits.
#   S5_BestPredictors       Section 5 exhibits.
#   SA_Appendices           Appendix-only exhibits.
#   9_ExportDataToCsv       Shared-data CSV exports.
#
# Iterating on an exhibit normally means running only its section. Changes to
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
# Paper contract: S2 through S5 and SA rebuild paper exhibits from upstream caches.
# S3 renders the cached MP regressions, including the Section 3 presentation
# tables, directly from R.
# See docs/exhibit_map.md for the script -> exhibit map.

# Stage switches live in config.R (runStages). Source it directly; this avoids
# loading the analysis packages just to decide which stages to run.
source("config.R")

run_script <- function(path) {
  message("\n=== Running ", path, " ===")
  status <- system2(file.path(R.home("bin"), "Rscript"), path)
  if (!identical(status, 0L)) {
    stop("Pipeline stage failed (exit ", status, "): ", path)
  }
}

# Chapter 1: acquisition and cleaning ------------------------------------

if (runStages$download_and_clean) {
  run_script("1_Download_and_Clean.R")
}

# Chapter 2: mined-strategy construction ---------------------------------

if (runStages$data_mining) {
  run_script("2_DataMining.R")
}

# Chapter 3: reusable analysis caches ------------------------------------

if (runStages$precompute) {
  run_script("3_Precompute.R")
}

# Section 2: research versus data mining ---------------------------------

if (runStages$research_vs_data_mining) {
  run_script("S2_ResearchVsDataMining.R")
}

# Section 3: learning -----------------------------------------------------

if (runStages$learning) {
  run_script("S3_Learning.R")
}

# Section 4: heterogeneity ------------------------------------------------

if (runStages$heterogeneity) {
  run_script("S4_Heterogeneity.R")
}

# Section 5: best predictors ---------------------------------------------

if (runStages$best_predictors) {
  run_script("S5_BestPredictors.R")
}

# Appendices --------------------------------------------------------------

if (runStages$appendices) {
  run_script("SA_Appendices.R")
}

# Chapter 9: data exports -------------------------------------------------

if (runStages$export_data_to_csv) {
  run_script("9_ExportDataToCsv.R")
}
