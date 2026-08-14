# Main script for rebuilding the data and paper exhibits.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript MAIN.R
#
# Two stages, split at the ../Data/Processed cache so a figure or table tweak
# does not pay for the multi-hour data build:
#
#   Stage A, build    (run_build)     1a, 2  [+ 1 if run_downloads]
#                     Constructs the ../Data/Processed cache from ../Data/Raw.
#                     Hours (2a alone was ~2h of a 2h38m full run). Rerun only
#                     when the underlying data changes.
#   Stage B, exhibits (run_exhibits)  3, 4, 8, 99
#                     Reads that cache and emits every paper exhibit to
#                     ../Results (chapter 4 also writes its own cheap prep RDS).
#                     Rerun freely while iterating on an exhibit.
#
# Iterating on a figure: build once (run_build = TRUE), then set
# run_build = FALSE and rerun with run_exhibits = TRUE against the cache.
#
# run_downloads is the deepest part of Stage A: it runs 1_Download_and_Clean.R,
# which pulls fresh data from WRDS and Google Drive and OVERWRITES ../Data/Raw.
# WRDS is not versioned and offers no as-of retrieval, so run_downloads = TRUE
# replaces the current data vintage irreversibly and every downstream result
# moves with it. Archive ../Data/Raw before setting it to TRUE.
#
# Inputs:  ../Data/Raw (re-created when run_downloads = TRUE)
# Outputs: ../Data/Processed, ../Data/Export, ../Results
#
# Paper contract: a full run (both stages) rebuilds every exhibit
# \input/\includegraphics'd by ../risk-vs-writing/latex-risk-vs (53 files),
# except the two HandTable_MPStyleRegs*.tex, which are hand-transcribed from the
# Table_MPStyleRegs{Main,Unscaled} tables that 4c6_MPStyleDecayTables.R writes.
# See docs/journal/260813c,map,exhibits.md for the script -> exhibit map.

run_downloads <- FALSE  # Re-pull ../Data/Raw (overwrites it)
run_build     <- TRUE   # Build the ../Data/Processed cache
run_exhibits  <- TRUE   # Read the cache and emit the paper exhibits

# Environment -------------------------------------------------------------

source("0_Environment.R", echo = TRUE)

# Stage A: build the processed-data cache ---------------------------------

if (run_build) {
  if (run_downloads) {
    source("1_Download_and_Clean.R", echo = TRUE)
  }

  source("1a_ValidDenoms.R", echo = TRUE)
  source("2_DataMining.R", echo = TRUE)
}

# Stage B: generate the paper exhibits -----------------------------------

if (run_exhibits) {
  source("3_RiskVsMispricing.R", echo = TRUE)
  source("4_ResearchVsDataMining.R", echo = TRUE)
  source("8_DMThemes.R", echo = TRUE)
  source("99_ExportDataToCsv.R", echo = TRUE)
}
