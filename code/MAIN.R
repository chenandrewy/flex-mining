# Main script for rebuilding the data and paper exhibits.
#
# How to run: set the working directory to code/, then
#   Rscript MAIN.R
#
# Data stages, paper-section exhibit stages, and exports, grouped by the
# runStages switch that controls them (see config.R):
#
#   download_and_clean
#     1_Download_and_Clean  External acquisition and cleaning; new data vintage.
#   data_mining
#     2_DataMining          Construct and match mined strategies; about four hours.
#   precompute
#     3_Precompute          Reusable correlations, PCA, panels, and summaries.
#   exhibits
#     S2_ResearchVsDataMining Introduction and Section 2 exhibits.
#     S3_Learning             Section 3 regression tables.
#     S4_Quality              Section 4 journal and renowned-match exhibits.
#     SA_Appendices           Appendix-only exhibits, including SA11 plots.
#     9_ExportDataToCsv       Shared-data CSV exports.
#   time_fe_robustness
#     TimeFERobustness        Large time-FE robustness appendix.
#
# Iterating on one exhibit normally means running its driver directly, e.g.
#   Rscript S3_Learning.R
# Every driver above is standalone; the switches exist for the stages where
# skipping is a real decision. Changes to matching or statistical analysis
# require chapter 3; changes to mined signals require chapter 2.
#
# Chapter 1 pulls fresh data from WRDS and Hugging Face and OVERWRITES
# ../Data/Raw.
# WRDS is not versioned and offers no as-of retrieval, so setting
# run_download_and_clean = TRUE replaces the current data vintage irreversibly
# and every downstream result moves with it. Archive ../Data/Raw first.
#
# Inputs:  ../Data/Raw (re-created when run_download_and_clean = TRUE)
# Outputs: ../Data/Processed, ../Data/Export, ../Results
#
# Paper contract: S2 through S4 and SA rebuild paper exhibits from upstream caches.
# S3 renders the cached MP regressions, including the Section 3 presentation
# tables, directly from R.
# See docs/exhibit_map.md for the script -> exhibit map.

# Stage switches live in config.R (runStages). Source it directly; this avoids
# loading the analysis packages just to decide which stages to run.
source("config.R")
source("helpers/driver.R")

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

# Exhibits: main-text sections, appendices, and exports ------------------
# These read the upstream caches and take about ten minutes together, so they
# share one switch. While iterating on a single exhibit, run its driver
# directly instead -- e.g. `Rscript S3_Learning.R`.

if (runStages$exhibits) {
  run_script("S2_ResearchVsDataMining.R")
  run_script("S3_Learning.R")
  run_script("S4_Quality.R")
  run_script("SA_Appendices.R")
  run_script("9_ExportDataToCsv.R")
}

# Time-fixed-effects robustness appendix ---------------------------------
# This is separate from the normal appendix rebuild because it uses pinned
# external data, multi-gigabyte caches, and may require a WRDS connection.

if (runStages$time_fe_robustness) {
  run_script("Appendices/TimeFERobustness/run.R")
}

