# Validate the focused helper-file inventory and explicit specialized sources.
#
# How to run: from flex-mining/, run
#   Rscript tests/test_helper_inventory.R
# Inputs: helpers/*.R and live R source files
# Outputs: none; exits nonzero on an obsolete or unexpected helper contract.

expected_helpers <- sort(c(
  "matching.R",
  "plotting.R",
  "risk_adjusted_tables.R",
  "stats.R"
))
stopifnot(identical(sort(list.files("helpers", pattern = "\\.R$")), expected_helpers))
stopifnot(
  !file.exists("helpers/risk_adjusted_helpers_tv.R"),
  !file.exists("helpers/risk_adjusted_helpers_fs.R"),
  !file.exists("helpers/factor_adjusted_dm.R"),
  !file.exists("helpers/fig2_helpers.R"),
  !file.exists("helpers/implied_category.R"),
  !file.exists("helpers/mining.R"),
  !file.exists("helpers/mp_table_helpers.R"),
  !file.exists("helpers/utils.R")
)

live_files <- list.files(
  ".", pattern = "\\.R$", recursive = TRUE, full.names = TRUE
)
live_files <- live_files[!grepl("(^|/)(CodeArchive|tests)/", live_files)]
live_source <- unlist(lapply(live_files, readLines, warn = FALSE))
stopifnot(
  !any(grepl("risk_adjusted_helpers_(tv|fs)\\.R", live_source)),
  any(grepl('source\\("helpers/risk_adjusted_tables\\.R"\\)', live_source)),
  !any(grepl('source\\("helpers/factor_adjusted_dm\\.R"\\)', live_source))
)

message("Focused helper inventory contract passed.")
