# Section 2 driver: research versus data mining exhibits.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript S2_ResearchVsDataMining.R
# Inputs:  cleaned data and chapter-3 caches under ../Data/Processed
# Outputs: the introduction figure and Section 2 PDFs/TeX under ../Results
#
# Some renderers also emit closely related appendix variants. Each child runs
# in a separate R process so memory is returned before the next exhibit.

source("helpers/driver.R")

required_files <- c(
  "../Data/Processed/czsum_allpredictors.RDS",
  "../Data/Processed/czret_keeponly.RDS",
  "../Data/Processed/ret_for_plot0.RDS",
  "../Data/Processed/sumsignal_oos_30y_ew_unit_level.csv",
  "../Data/Processed/sumsignal_oos_30y_vw_unit_level.csv",
  "../Data/Processed/sumsignal_oos_30y_post_2003_ew_unit_level.csv",
  "../Data/Processed/sumsignal_oos_30y_post_2003_vw_unit_level.csv",
  "../Data/Processed/raw_dm_benchmarks.RDS",
  "../Data/Processed/factor_adjusted_dm_benchmarks.RDS",
  "../Data/Processed/dm_correlation_quantiles.RDS",
  "../Data/Processed/dm_pca_table.RDS"
)
require_files(required_files, "Section 2")
exit_if_preflight_only("Section 2")

# The headline comparison is displayed in the introduction but belongs to the
# research-versus-data-mining empirical chapter.
run_script("S2a_ResearchVsDMPlots.R", "Section 2")
run_script("S2b_DataMiningSummaryTables.R", "Section 2")
run_script("S2c_DMCorrelationsPCATables.R", "Section 2")
run_script("S2d_EZThemes.R", "Section 2")
run_script("S2e_Fig2Plots.R", "Section 2")
