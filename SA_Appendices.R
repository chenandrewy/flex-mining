# Appendix driver: appendix-only exhibits and diagnostics.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript SA_Appendices.R
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
    "Missing appendix input(s): ", paste(missing_files, collapse = ", "),
    ". Run the required upstream chapter first."
  )
}
if ("--preflight-only" %in% commandArgs(trailingOnly = TRUE)) {
  message("Appendix preflight passed.")
  quit(save = "no", status = 0)
}

run_script <- function(path) {
  message("\n--- Appendices: ", path, " ---")
  status <- system2(file.path(R.home("bin"), "Rscript"), path)
  if (!identical(status, 0L)) {
    stop("Appendix script failed (exit ", status, "): ", path)
  }
}

run_script("Appendices/SA01_RiskVsMispricingPlots.R")
run_script("Appendices/SA02_RegDecayTable.R")
run_script("Appendices/SA03_StructuralBreak.R")
run_script("Appendices/SA04_DecayVsWordcountPlot.R")
run_script("Appendices/SA05_DecayVsModelcountPlot.R")
run_script("Appendices/SA06_DecayVsJournal.R")
run_script("Appendices/SA07_FullSampleRiskAdjustedResearchVsDMPlots.R")
run_script("Appendices/SA08_AccountingOnlyPlots.R")
run_script("Appendices/SA09_AccountingOnlyAlphaPlots.R")
run_script("Appendices/SA10_ResearchVsDMRobustnessCorrelationsEtc.R")
run_script("Appendices/SA11_DMSpanPCAPlots.R")
run_script("Appendices/SA12_EZThemesRobustness.R")
