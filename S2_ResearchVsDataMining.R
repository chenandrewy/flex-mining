# Section 2 driver: research versus data mining exhibits.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript S2_ResearchVsDataMining.R
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
  "../Data/Processed/sumsignal_oos_30y_ew_unit_level.csv",
  "../Data/Processed/sumsignal_oos_30y_vw_unit_level.csv",
  "../Data/Processed/sumsignal_oos_30y_post_2003_ew_unit_level.csv",
  "../Data/Processed/sumsignal_oos_30y_post_2003_vw_unit_level.csv",
  "../Data/Processed/raw_dm_benchmarks.RDS",
  "../Data/Processed/factor_adjusted_dm_benchmarks.RDS"
)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing Section 2 input(s): ", paste(missing_files, collapse = ", "),
    ". Run the required upstream chapter first."
  )
}
if ("--preflight-only" %in% commandArgs(trailingOnly = TRUE)) {
  message("Section 2 preflight passed.")
  quit(save = "no", status = 0)
}

run_script <- function(path) {
  message("\n--- Section 2: ", path, " ---")
  status <- system2(file.path(R.home("bin"), "Rscript"), path)
  if (!identical(status, 0L)) {
    stop("Section 2 script failed (exit ", status, "): ", path)
  }
}

# The headline comparison is displayed in the introduction but belongs to the
# research-versus-data-mining empirical chapter.
run_script("S2a_ResearchVsDMPlots.R")
run_script("S2b_DataMiningSummaryTables.R")
run_script("S2d_EZThemes.R")
run_script("S2e_Fig2Plots.R")
