# Run risk-adjusted analysis with both t-stat and return filters

# # Run with t-stat filter
# cat("=== Running with t-stat filter ===\n")
# Sys.setenv(FILTER_TYPE = "tstat")
# source("4c4_RiskAdjustedResearchVsDMPlots.R")

# # Clear environment except for necessary objects
# rm(list = setdiff(ls(), c("globalSettings")))

# Run with return filter
cat("\n\n=== Running with return filter ===\n")
Sys.setenv(FILTER_TYPE = "return")
source("4c4_RiskAdjustedResearchVsDMPlots.R")

cat("\n\nAnalysis complete!\n")
cat("Results saved in:\n")
# cat("- ../Results/RiskAdjusted/TstatFilter/\n")
cat("- ../Results/RiskAdjusted/ReturnFilter_0.15/\n")