# Validate the retained phase-one risk-adjusted outputs against frozen fixtures.
#
# How to run: set the working directory to flex-mining/, generate the Section 4
#   and IA.10 outputs, then run `Rscript tests/test_4c4_phase1_equivalence.R`.
# Inputs:  six fixtures under tests/fixtures/4c4_phase1_baseline and the six
#          corresponding live files under ../Results/RiskAdjusted/TstatFilter
# Outputs: no files; exits nonzero on any CSV or displayed-TeX difference

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

  if (!identical(readLines(fixture_csv, warn = FALSE),
                 readLines(live_csv, warn = FALSE))) {
    stop("CSV differs from phase-one fixture: ", base)
  }
  if (!identical(extract_tabular(fixture_tex), extract_tabular(live_tex))) {
    stop("Displayed TeX differs from phase-one fixture: ", base)
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

accidental_temps <- file.path(
  "../Results",
  c("temp__raw_returns_t2.pdf", "temp__capm_alpha_t2.pdf",
    "temp__ff4_alpha_t2.pdf")
)
if (any(file.exists(accidental_temps))) {
  stop("Legacy temporary PDF(s) remain: ",
       paste(accidental_temps[file.exists(accidental_temps)], collapse = ", "))
}

message("All six retained outputs match the frozen phase-one baseline.")
