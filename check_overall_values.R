# Check the exact overall CAPM values for both TV and FS approaches

# Load libraries
library(dplyr)
library(data.table)

# Load helper functions
source('helpers/risk_adjusted_helpers_tv.R')
source('helpers/risk_adjusted_helpers_fs.R')

# Load the data that was computed by 4c4 and 4c5
cat("Loading data files...\n")

# Get the plot data from 4c4 (time-varying)
load("../Results/RiskAdjusted/TstatFilter/plot_data_capm_tv_t2.RData")
ret_for_plot0_capm_tv_t2 <- plot_data
rm(plot_data)

# Get the plot data from 4c5 (full sample)
load("../Results/RiskAdjusted/FullSampleTstatFilter/plot_data_capm_fs_t2.RData")
ret_for_plot0_capm_fs_t2 <- plot_data
rm(plot_data)

# Compute overall summaries with more precision
cat("\n=== TIME-VARYING CAPM (4c4) ===\n")
overall_tv <- compute_overall_summary(
  plot_data = ret_for_plot0_capm_tv_t2,
  ret_col = "abnormal_capm_tv_normalized",
  dm_col = "matchRet_capm_tv_t2_normalized"
)

cat("Published signals (Post-Sample Return):", sprintf("%.3f", overall_tv$pub_oos), "\n")
cat("Data-mined signals (Post-Sample Return):", sprintf("%.3f", overall_tv$dm_oos), "\n")
cat("Outperformance:", sprintf("%.3f", overall_tv$outperform), "\n")
cat("Published SE:", sprintf("%.3f", overall_tv$pub_oos_se), "\n")
cat("Outperformance SE:", sprintf("%.3f", overall_tv$outperform_se), "\n")

cat("\n=== FULL SAMPLE CAPM (4c5) ===\n")
overall_fs <- compute_overall_summary_fs(
  plot_data = ret_for_plot0_capm_fs_t2,
  ret_col = "abnormal_capm_fs_normalized",
  dm_col = "matchRet_capm_fs_t2_normalized"
)

cat("Published signals (Post-Sample Return):", sprintf("%.3f", overall_fs$pub_oos), "\n")
cat("Data-mined signals (Post-Sample Return):", sprintf("%.3f", overall_fs$dm_oos), "\n")
cat("Outperformance:", sprintf("%.3f", overall_fs$outperform), "\n")
cat("Published SE:", sprintf("%.3f", overall_fs$pub_oos_se), "\n")
cat("Outperformance SE:", sprintf("%.3f", overall_fs$outperform_se), "\n")

cat("\n=== DIFFERENCE (TV - FS) ===\n")
cat("Published Return Difference:", sprintf("%.3f", overall_tv$pub_oos - overall_fs$pub_oos), "\n")
cat("Data-mined Return Difference:", sprintf("%.3f", overall_tv$dm_oos - overall_fs$dm_oos), "\n")
cat("Outperformance Difference:", sprintf("%.3f", overall_tv$outperform - overall_fs$outperform), "\n")

# Check number of signals in each dataset
cat("\n=== DATASET SIZES ===\n")
cat("Time-varying dataset:\n")
cat("  Unique published signals:", length(unique(ret_for_plot0_capm_tv_t2$pubname)), "\n")
cat("  Total observations:", nrow(ret_for_plot0_capm_tv_t2), "\n")

cat("Full sample dataset:\n")
cat("  Unique published signals:", length(unique(ret_for_plot0_capm_fs_t2$pubname)), "\n")
cat("  Total observations:", nrow(ret_for_plot0_capm_fs_t2), "\n")

# Check which signals are in each dataset
tv_signals <- unique(ret_for_plot0_capm_tv_t2$pubname)
fs_signals <- unique(ret_for_plot0_capm_fs_t2$pubname)

cat("\nSignals in TV but not FS:", length(setdiff(tv_signals, fs_signals)), "\n")
cat("Signals in FS but not TV:", length(setdiff(fs_signals, tv_signals)), "\n")
cat("Signals in both:", length(intersect(tv_signals, fs_signals)), "\n")

if(length(setdiff(tv_signals, fs_signals)) > 0) {
  cat("TV-only signals:", paste(head(setdiff(tv_signals, fs_signals), 5), collapse=", "), "\n")
}
if(length(setdiff(fs_signals, tv_signals)) > 0) {
  cat("FS-only signals:", paste(head(setdiff(fs_signals, tv_signals), 5), collapse=", "), "\n")
}