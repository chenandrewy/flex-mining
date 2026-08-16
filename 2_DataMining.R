# Chapter 2 driver: construct the accounting and ticker mined universes.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript 2_DataMining.R
# Inputs:  cleaned chapter-1 inputs
# Outputs: core mined-strategy caches in ../Data/Processed
#
# 2a takes roughly two hours. Each child runs in a separate R process so the
# allocator's high-water memory is returned to the operating system between
# scripts.

run_script <- function(path) {
  message("\n--- Chapter 2: ", path, " ---")
  status <- system2(file.path(R.home("bin"), "Rscript"), path)
  if (!identical(status, 0L)) {
    stop("Chapter 2 script failed (exit ", status, "): ", path)
  }
}

run_script("2a_CompustatToLongshort.R")
run_script("2c_TickerToLongshort.R")
