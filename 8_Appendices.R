# Chapter 8 driver: appendix-only exhibits and diagnostics.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript 8_Appendices.R
# Inputs:  cleaned data and chapter-2/3 caches
# Outputs: appendix PDFs and TeX under ../Results
#
# Appendix variants emitted alongside a main-text exhibit remain owned by that
# main-text chapter and are not rerun here.

required_files <- c(
  "../Data/Processed/czsum_allpredictors.RDS",
  "../Data/Processed/czret_keeponly.RDS",
  "../Data/Processed/ret_for_plot0.RDS",
  "../Data/Processed/ret_for_plot1.RDS",
  "../Data/Processed/dm_span_analysis.RDS",
  "../Data/Processed/PairwiseCorrelationsActualAndMatches.RDS"
)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing Chapter 8 input(s): ", paste(missing_files, collapse = ", "),
    ". Run the required upstream chapter first."
  )
}

run_script <- function(path) {
  message("\n--- Chapter 8: ", path, " ---")
  status <- system2(file.path(R.home("bin"), "Rscript"), path)
  if (!identical(status, 0L)) {
    stop("Chapter 8 script failed (exit ", status, "): ", path)
  }
}

run_script("4a1_RiskVsMispricingPlots.R")
run_script("4a2_RegDecayTable.R")
run_script("4a4_StructuralBreak.R")
run_script("4a5_DecayVsWordcountPlot.R")
run_script("4a6_DecayVsModelcountPlot.R")
run_script("4a7_DecayVsJournal.R")
run_script("4c5_FullSampleRiskAdjustedResearchVsDMPlots.R")
run_script("4c6_AccountingOnlyPlots.R")
run_script("4c7_AccountingOnlyAlphaPlots.R")
run_script("4d1_ResearchVsDMRobustnessCorrelationsEtc.R")
run_script("4d3_DMSpanPCAPlots.R")
run_script("4e2_EZThemesRobustness.R")
