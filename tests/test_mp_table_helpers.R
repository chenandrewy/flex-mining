# Focused checks for the combined MP-style table renderer.
#
# How to run: from flex-mining/, run
#   Rscript tests/test_mp_table_helpers.R
# Inputs:  helpers/mp_table_helpers.R
# Outputs: temporary TeX files only; exits nonzero on failure.

source("helpers/mp_table_helpers.R")

set.seed(42)
n <- 240L
test_data <- data.frame(
  predictor = rep(seq_len(12L), length.out = n),
  postSample = c(rep(0, n / 4L), rep(1, 3L * n / 4L)),
  postPub = c(rep(0, 3L * n / 4L), rep(1, n / 4L))
)
test_data$return <- with(
  test_data,
  0.5 * postSample - 0.25 * postPub + as.numeric(predictor) / 10 + rnorm(n)
)
fit <- fixest::feols(return ~ postSample + postPub | predictor, data = test_data)
fits <- rep(list(fit), 6L)

output_dir <- tempfile("mp-table-helper-test-")
dir.create(output_dir)
write_combined_mp_tables(fits, fits, n_signals = 12L, output_dir)

main_path <- file.path(output_dir, "Table_MPStyleRegsNoTimeFE.tex")
time_path <- file.path(output_dir, "Table_MPStyleRegsTimeFE.tex")
stopifnot(file.exists(main_path), file.exists(time_path))

main_lines <- readLines(main_path)
time_lines <- readLines(time_path)
stopifnot(
  identical(main_lines[[1]], "% GENERATED -- do not hand-edit (helpers/mp_table_helpers.R)."),
  "\\begin{tabular}{lcccccc}" %in% main_lines,
  any(grepl("& (1) & (2) & (3) & (4) & (5) & (6)", main_lines, fixed = TRUE)),
  any(grepl("Signals          & 12 & 12 & 12 & 12 & 12 & 12", main_lines, fixed = TRUE)),
  any(grepl("Signals          & 12 & 12 & 12 & 12 & 12 & 12", time_lines, fixed = TRUE)),
  any(grepl("Time F.E.        & No & No & No & No & No & No", main_lines, fixed = TRUE)),
  any(grepl("Time F.E.        & Yes & Yes & Yes & Yes & Yes & Yes", time_lines, fixed = TRUE))
)

bad_fit_count <- try(make_combined_table(fits[1:5], 12L, FALSE, tempfile()), silent = TRUE)
stopifnot(inherits(bad_fit_count, "try-error"))
