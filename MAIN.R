# Main script to run the numbered R scripts (letter-indexed scripts are sourced
# from within them).
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
#   Stage B, exhibits (run_exhibits)  3, 4, 6, 8, 99
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

run_downloads <- FALSE   # Stage A: re-pull ../Data/Raw (overwrites it)
run_build     <- TRUE    # Stage A: build the ../Data/Processed cache
run_exhibits  <- TRUE    # Stage B: read the cache and emit the paper exhibits

# Stage lists -------------------------------------------------------------

build_scripts <- c(
  if (run_downloads) "1_Download_and_Clean.R",
  "1a_ValidDenoms.R",
  "2_DataMining.R"
)

exhibit_scripts <- c(
  "3_RiskVsMispricing.R",
  "4_ResearchVsDataMining.R",
  "6_TextAnalysis.R",
  "8_DMThemes.R",
  "99_ExportDataToCsv.R"
)

main_scripts <- c(
  if (run_build)    build_scripts,
  if (run_exhibits) exhibit_scripts
)

# Run each script in order ------------------------------------------------

# 0_Environment.R (paths, packages, helpers) is sourced by every script below,
# so each stage stands alone; run it up front too for a bare interactive start.
source("0_Environment.R", echo = TRUE)

for (script in main_scripts) {
    source(script, echo = TRUE)
}
