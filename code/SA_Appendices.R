# Appendix driver: appendix-only exhibits and diagnostics.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript SA_Appendices.R
# Inputs:  cleaned data and chapter-2/3 caches
# Outputs: appendix PDFs and TeX under ../Results
#
# Appendix variants emitted alongside a main-text exhibit remain owned by that
# main-text chapter and are not rerun here. SA11 spanning plots read caches from Chapter 3.

source("helpers/driver.R")

required_files <- c(
  "../Data/Processed/czsum_allpredictors.RDS",
  "../Data/Processed/czret_keeponly.RDS",
  "../Data/Processed/ret_for_plot0.RDS",
  "../Data/Processed/dmcomp_sumstats.RDS",
  "../Data/Processed/raw_dm_benchmarks.RDS",
  "../Data/Processed/dm_span_analysis.RDS",
  longshort_path()
)
require_files(required_files, "appendix")
exit_if_preflight_only("Appendices")

run_script("Appendices/SA03_StructuralBreak.R", "Appendices")
run_script("Appendices/SA11_DMSpanPCAPlots.R", "Appendices")
run_script("Appendices/SA12_EZThemesRobustness.R", "Appendices")
run_script("Appendices/SA13_MPStyleRegsIndividualDM.R", "Appendices")
run_script("Appendices/SA14_MPStyleRegsAccountingOnly.R", "Appendices")
