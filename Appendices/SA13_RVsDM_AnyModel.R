# Render the sample-specific Any Model versus No Model appendix table (IA.10).
#
# How to run: set the working directory to flex-mining/, then
#   Rscript Appendices/SA13_RVsDM_AnyModel.R
# Inputs:  czsum_allpredictors.RDS, czret_keeponly.RDS, ret_for_plot0.RDS,
#          the versioned MatchPubRiskAdjusted.RData cache, Fama-French factors,
#          and DataInput/SignalsTheoryChecked.csv
# Outputs: Table_RiskAdjusted_TimeVarying_AnyModelVsNoModel_ff4_t2.{csv,tex}
#          under ../Results/RiskAdjusted/TstatFilter

settings_env <- new.env(parent = globalenv())
sys.source("config.R", envir = settings_env)
version <- settings_env$globalSettings$dataVersion
rm(settings_env)

required_files <- c(
  "../Data/Processed/czsum_allpredictors.RDS",
  "../Data/Processed/czret_keeponly.RDS",
  "../Data/Processed/ret_for_plot0.RDS",
  paste0("../Data/Processed/", version, " MatchPubRiskAdjusted.RData"),
  "../Data/Raw/FamaFrenchFactors.RData",
  "DataInput/SignalsTheoryChecked.csv"
)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing IA.10 risk-adjustment input(s): ",
    paste(missing_files, collapse = ", "),
    ". Run the required upstream chapter first."
  )
}

source("helpers/risk_adjusted_by_group_frozen.R")

file_suffix <- paste0("_ff4_t", t_threshold)
headers <- list(
  list(title = "Raw", span = 2),
  list(title = "CAPM", span = 2),
  list(title = "FF4", span = 2)
)
export_audit_tabular(
  export_table_tv_am_ff4,
  file.path(
    results_dir,
    paste0("Table_RiskAdjusted_TimeVarying_AnyModelVsNoModel", file_suffix)
  ),
  headers
)

message("Wrote the Table IA.10 audit CSV and tabular fragment.")
