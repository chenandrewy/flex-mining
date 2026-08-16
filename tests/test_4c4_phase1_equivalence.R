# Guard against accidentally restoring the known-bug restricted-universe outputs.
#
# How to run: set the working directory to flex-mining/, generate the Section 4
#   and IA.10 outputs, then run `Rscript tests/test_4c4_phase1_equivalence.R`.
# Inputs:  six fixtures under tests/fixtures/4c4_phase1_baseline and the six
#          corresponding live files under ../Results/RiskAdjusted/TstatFilter
# Outputs: no files; exits nonzero if a live table reverts exactly to the
#   frozen 10%/30% restricted-universe fixture.

fixture_dir <- "tests/fixtures/4c4_phase1_baseline"
live_dir <- "../Results/RiskAdjusted/TstatFilter"
bases <- c(
  "Table_RiskAdjusted_TimeVarying_ff4_t2",
  "Table_RiskAdjusted_TimeVarying_DisciplineJournal_ff4_t2",
  "Table_RiskAdjusted_TimeVarying_AnyModelVsNoModel_ff4_t2"
)

extract_tabular <- function(path) {
  lines <- readLines(path, warn = FALSE)
  first <- grep("^\\\\begin\\{tabular\\}", lines)[1]
  last <- tail(grep("^\\\\end\\{tabular\\}", lines), 1)
  if (is.na(first) || is.na(last) || first > last) {
    stop("No complete tabular environment in ", path)
  }
  lines[first:last]
}

for (base in bases) {
  fixture_csv <- file.path(fixture_dir, paste0(base, ".csv"))
  live_csv <- file.path(live_dir, paste0(base, ".csv"))
  fixture_tex <- file.path(fixture_dir, paste0(base, ".tex"))
  live_tex <- file.path(live_dir, paste0(base, ".tex"))

  if (identical(readLines(fixture_csv, warn = FALSE),
                readLines(live_csv, warn = FALSE))) {
    stop("Live CSV reverted to the known-bug phase-one fixture: ", base)
  }
  if (identical(extract_tabular(fixture_tex), extract_tabular(live_tex))) {
    stop("Live TeX reverted to the known-bug phase-one fixture: ", base)
  }
}

expected_files <- sort(c(paste0(bases, ".csv"), paste0(bases, ".tex")))
actual_files <- sort(list.files(live_dir, all.files = FALSE, no.. = TRUE))
if (!identical(expected_files, actual_files)) {
  stop(
    "Risk-adjusted output contract differs. Expected: ",
    paste(expected_files, collapse = ", "),
    "; found: ", paste(actual_files, collapse = ", ")
  )
}

message("No live risk-adjusted output matches the known-bug phase-one baseline.")
