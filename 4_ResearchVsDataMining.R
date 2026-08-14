# Chapter 4 driver: research versus data mining exhibits.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript 4_ResearchVsDataMining.R
# Inputs:  cleaned data and chapter-3 caches under ../Data/Processed
# Outputs: the introduction figure and Section 2 PDFs/TeX under ../Results
#
# Some renderers also emit closely related appendix variants. Each child runs
# in a separate R process so memory is returned before the next exhibit.

required_files <- c(
  "../Data/Processed/czsum_allpredictors.RDS",
  "../Data/Processed/czret_keeponly.RDS",
  "../Data/Processed/dmcomp_sumstats.RDS",
  "../Data/Processed/dmtic_sumstats.RDS",
  "../Data/Processed/ret_for_plot0.RDS",
  "../Data/Processed/ret_for_plot1.RDS",
  "../Data/Processed/ret_for_plot_MaxPredictors.RDS",
  "../Data/Processed/sumsignal_oos_30y_ew_unit_level.csv",
  "../Data/Processed/sumsignal_oos_30y_vw_unit_level.csv",
  "../Data/Processed/sumsignal_oos_30y_post_2003_ew_unit_level.csv",
  "../Data/Processed/sumsignal_oos_30y_post_2003_vw_unit_level.csv",
  "../Data/Processed/dm_correlation_quantiles.RDS",
  "../Data/Processed/dm_pca_table.RDS",
  "../Data/Processed/fig2_panel_agg.RDS"
)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing Chapter 4 input(s): ", paste(missing_files, collapse = ", "),
    ". Run the required upstream chapter first."
  )
}

run_script <- function(path) {
  message("\n--- Chapter 4: ", path, " ---")
  status <- system2(file.path(R.home("bin"), "Rscript"), path)
  if (!identical(status, 0L)) {
    stop("Chapter 4 script failed (exit ", status, "): ", path)
  }
}

# The headline comparison is displayed in the introduction but belongs to the
# research-versus-data-mining empirical chapter.
run_script("4c2_ResearchVsDMPlots.R")
run_script("4b1_DataMiningSummaryTables.R")
run_script("4b2_DMCorrelationsPCATables.R")
run_script("4e1_EZThemes.R")
run_script("4c9_Fig2Plots.R")
