# Chapter 4 driver: render paper exhibits from precomputed data.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript 4_Exhibits.R
# Inputs:  chapter-1/2 data and chapter-3 caches under ../Data/Processed
# Outputs: PDFs and TeX under ../Results, plus explicit CSV exports
#
# This driver treats ../Data/Processed as read-only. Each child runs in a
# separate R process so memory is returned before the next exhibit.

required_files <- c(
  "../Data/Processed/czsum_allpredictors.RDS",
  "../Data/Processed/czret_keeponly.RDS",
  "../Data/Processed/dmcomp_sumstats.RDS",
  "../Data/Processed/dmtic_sumstats.RDS",
  "../Data/Processed/plotdat0.RDS",
  "../Data/Processed/ret_for_plot0.RDS",
  "../Data/Processed/ret_for_plot1.RDS",
  "../Data/Processed/ret_for_plot_MaxPredictors.RDS",
  "../Data/Processed/sumsignal_oos_30y_ew_unit_level.csv",
  "../Data/Processed/sumsignal_oos_30y_vw_unit_level.csv",
  "../Data/Processed/sumsignal_oos_30y_post_2003_ew_unit_level.csv",
  "../Data/Processed/sumsignal_oos_30y_post_2003_vw_unit_level.csv",
  "../Data/Processed/dm_correlation_quantiles.RDS",
  "../Data/Processed/dm_pca_table.RDS",
  "../Data/Processed/fig2_panel_agg.RDS",
  "../Data/Processed/dm_span_analysis.RDS",
  "../Data/Processed/mp_style_decay_models.RDS",
  "../Data/Processed/PairwiseCorrelationsActualAndMatches.RDS"
)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing chapter-3 exhibit cache(s): ", paste(missing_files, collapse = ", "),
    ". Run 3_Precompute.R first."
  )
}

run_script <- function(path) {
  message("\n--- Chapter 4: ", path, " ---")
  status <- system2(file.path(R.home("bin"), "Rscript"), path)
  if (!identical(status, 0L)) {
    stop("Chapter 4 script failed (exit ", status, "): ", path)
  }
}

# Risk versus mispricing.
run_script("4a1_RiskVsMispricingPlots.R")
run_script("4a2_RegDecayTable.R")
run_script("4a3_DataCounts.R")
run_script("4a4_StructuralBreak.R")
run_script("4a5_DecayVsWordcountPlot.R")
run_script("4a6_DecayVsModelcountPlot.R")
run_script("4a7_DecayVsJournal.R")

# Research versus data mining.
run_script("4b1_DataMiningSummaryTables.R")
run_script("4b2_DMCorrelationsPCATables.R")
run_script("4c2_ResearchVsDMPlots.R")
run_script("4c5_FullSampleRiskAdjustedResearchVsDMPlots.R")
run_script("4c6_AccountingOnlyPlots.R")
run_script("4c7_AccountingOnlyAlphaPlots.R")
run_script("4c8_MPStyleDecayTables.R")
run_script("4d1_ResearchVsDMRobustnessCorrelationsEtc.R")
run_script("4d2_InspectTables.R")
run_script("4d3_DMSpanPCAPlots.R")
run_script("4c9_Fig2Plots.R")

# Theme tables and data exports.
run_script("4e1_EZThemes.R")
run_script("4e2_EZThemesRobustness.R")
run_script("4f_ExportDataToCsv.R")
