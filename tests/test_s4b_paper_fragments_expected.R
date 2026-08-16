# Validate the three main-text and three appendix risk-adjusted tabular fragments.
#
# How to run: set the working directory to flex-mining/, run
#   Rscript S4b_RVsDM_ByGroup.R
#   Rscript Appendices/SA07_FullSampleRiskAdjustedResearchVsDMPlots.R
#   Rscript tests/test_s4b_paper_fragments_expected.R
# Inputs:  the six live paper-facing .tex files under
#          ../Results and their exact fixtures under tests/fixtures
# Outputs: no files; exits nonzero on content or fragment-contract changes

live_dir <- "../Results"
fixture_dir <- "tests/fixtures/s4b_paper_fragments"
files <- c(
  "Table_RiskAdjusted_TimeVarying_ff4_t2.tex",
  "Table_RiskAdjusted_TimeVarying_DisciplineJournal_ff4_t2.tex",
  "Table_RiskAdjusted_TimeVarying_AnyModelVsNoModel_ff4_t2.tex",
  "Table_RiskAdjusted_FullSample_Appendix.tex",
  "Table_RiskAdjusted_FullSample_DisciplineJournal_Appendix.tex",
  "Table_RiskAdjusted_FullSample_AnyModelVsNoModel_Appendix.tex"
)

for (file in files) {
  live <- readLines(file.path(live_dir, file), warn = FALSE)
  expected <- readLines(file.path(fixture_dir, file), warn = FALSE)
  if (!identical(live, expected)) {
    stop("Paper-facing TeX differs from exact fixture: ", file)
  }
  if (sum(grepl("^\\\\begin\\{tabular\\}", live)) != 1L ||
      sum(grepl("^\\\\end\\{tabular\\}", live)) != 1L) {
    stop("Paper-facing output is not one complete tabular: ", file)
  }
  forbidden <- "\\\\(begin\\{table\\}|end\\{table\\}|caption|label|begin\\{document\\}|end\\{document\\})"
  if (any(grepl(forbidden, live))) {
    stop("Paper-facing output contains float or document wrapper markup: ", file)
  }
}

message("All six paper-facing fragments match their exact expectations.")
