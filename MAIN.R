# Main script to run all numbered R scripts (excluding letter-indexed scripts)
#
# How to run: set the working directory to flex-mining/, then
#   Rscript MAIN.R
#
# Two paths, selected by run_downloads below:
#
#   TRUE   Full run from scratch. Runs 1_Download_and_Clean.R, which pulls
#          fresh data from WRDS and Google Drive and OVERWRITES ../Data/Raw.
#   FALSE  All calculations after the downloads. Skips the download stage and
#          uses the existing ../Data/Raw as fixed, read-only input.
#
# WRDS is not versioned and offers no as-of retrieval, so a run with
# run_downloads = TRUE replaces the current data vintage irreversibly, and
# every downstream result moves with it. Archive ../Data/Raw before setting
# this to TRUE.
#
# Inputs:  ../Data/Raw (both paths; re-created when run_downloads = TRUE)
# Outputs: ../Data/Processed, ../Data/Export, ../Results
#
# Paper contract: running this rebuilds every exhibit \input/\includegraphics'd
# by ../risk-vs-writing/latex-risk-vs (53 files), except the two
# HandTable_MPStyleRegs*.tex, which are hand-transcribed from the
# Table_MPStyleRegs{Main,Unscaled} tables that 4c6_MPStyleDecayTables.R writes.
# See docs/journal/260813c,map,exhibits.md for the script -> exhibit map.

run_downloads <- FALSE

# Script List -------------------------------------------------------------

main_scripts <- c(
  "0_Environment.R",
  if (run_downloads) "1_Download_and_Clean.R",
  "1a_ValidDenoms.R",
  "2_DataMining.R",
  "3_RiskVsMispricing.R",
  "4_ResearchVsDataMining.R",
  "6_TextAnalysis.R",
  "8_DMThemes.R",
  "99_ExportDataToCsv.R"
)

# Run each script in order ------------------------------------------------

for (script in main_scripts) {
    source(script, echo = TRUE)
}
