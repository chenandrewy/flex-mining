# Main script for rebuilding the data and paper exhibits.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript MAIN.R
#
# Four chapters separated at explicit cache boundaries:
#
#   1_Download_and_Clean  External acquisition and cleaning; new data vintage.
#   2_DataMining          Construct and match mined strategies; about two hours.
#   3_Precompute          Reusable correlations, PCA, panels, and summaries.
#   4_Exhibits            Read caches and render PDFs, TeX, and exports.
#
# Iterating on an exhibit normally means running chapter 4 alone. Changes to
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
# Paper contract: a full run rebuilds every exhibit
# \input/\includegraphics'd by ../risk-vs-writing/latex-risk-vs (53 files),
# except the two HandTable_MPStyleRegs*.tex, which are hand-transcribed from the
# Table_MPStyleRegs{Main,Unscaled} tables that 4c8_MPStyleDecayTables.R writes.
# See docs/journal/0813c,map,exhibits.md for the script -> exhibit map.

run_download_and_clean <- FALSE  # Re-pull ../Data/Raw; changes the vintage
run_data_mining        <- FALSE  # Chapter 2; hours
run_precompute         <- TRUE   # Chapter 3; slow reusable analysis
run_exhibits           <- TRUE   # Chapter 4; cache-only rendering

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

# Chapter 4: paper exhibits ----------------------------------------------

if (run_exhibits) {
  run_script("4_Exhibits.R")
}
