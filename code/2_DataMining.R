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
#
# 2a derives its valid-denominator metadata in memory from
# ../Data/Raw/CompustatAnnual.RData (see derive_valid_denoms in 2a), so chapter
# 2 is self-contained and needs no precomputed DataIntermediate CSVs.

source("helpers/driver.R")

run_script("2a_CompustatToLongshort.R", "Chapter 2")
run_script("2c_TickerToLongshort.R", "Chapter 2")
