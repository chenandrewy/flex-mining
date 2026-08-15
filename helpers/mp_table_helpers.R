# Helpers for rendering the manuscript's MP-style decay tables.
#
# How to run: source this file from an R script.
# Inputs: six fixest models ordered as scaled pub/DM/difference, then unscaled
#         pub/DM/difference, plus n_signals (distinct published signals in the
#         estimation sample).
# Outputs: make_combined_table() writes one six-column TeX table.

# Preserve the manuscript's combined six-column layout: columns (1)--(3) are
# scaled published/DM/difference returns, and columns (4)--(6) are unscaled.
make_combined_table <- function(fits, n_signals, timeFE, file) {
  if (length(fits) != 6L) {
    stop("Combined MP table requires exactly six models; got ", length(fits), ".")
  }

  # Three significant digits, retaining meaningful trailing zeros so the
  # generated tables stay byte-stable with the manuscript's formatting.
  fmt <- function(x) {
    sub("\\.$", "", formatC(signif(x, 3), format = "fg", flag = "#", digits = 3))
  }
  fmt3 <- function(x) formatC(x, format = "f", digits = 3)
  cells <- sapply(fits, function(fit) {
    ct <- fixest::coeftable(fit)
    c(
      ps = fmt(ct["postSample", "Estimate"]),
      ps_se = paste0("(", fmt(ct["postSample", "Std. Error"]), ")"),
      pp = fmt(ct["postPub", "Estimate"]),
      pp_se = paste0("(", fmt(ct["postPub", "Std. Error"]), ")"),
      n = formatC(as.numeric(fixest::fitstat(fit, "n")$n), format = "d", big.mark = ","),
      # Distinct published signals in the estimation sample. A property of the
      # sample, so it is the same across all six columns; passed in rather than
      # read off each fit because the DM-benchmark columns cluster on dmname.
      signals = formatC(n_signals, format = "d", big.mark = ","),
      r2 = fmt3(as.numeric(fixest::fitstat(fit, "r2")$r2)),
      wr2 = fmt3(as.numeric(fixest::fitstat(fit, "wr2")$wr2))
    )
  })
  row <- function(label, key) {
    paste0(
      "   ", format(label, width = 16), " & ",
      paste(cells[key, ], collapse = " & "), " \\\\"
    )
  }
  header_row <- function(values) {
    paste0(
      "                    & ",
      paste(values, collapse = "\n                    & "), " \\\\"
    )
  }
  lines <- c(
    "% GENERATED -- do not hand-edit (helpers/mp_table_helpers.R).",
    "\\begingroup", "\\setlength{\\tabcolsep}{0.55ex}%", "\\centering",
    "\\begin{tabular}{lcccccc}", "   \\toprule",
    "                    & \\multicolumn{3}{c}{(a) LHS = Scaled Return} & \\multicolumn{3}{c}{(b) LHS = Unscaled Return} \\\\",
    "   \\cmidrule(lr){2-4}\\cmidrule(lr){5-7}",
    header_row(rep(c("Academic", "Data Mining", "Academic"), 2)),
    header_row(rep(c("Signal", "Benchmark", "Minus"), 2)),
    header_row(rep(c("Return", "(Uncorr)", "Data Mining"), 2)),
    paste0("                    & ", paste0("(", 1:6, ")", collapse = " & "), " \\\\"),
    "   \\midrule",
    row("Post-Sample", "ps"), row("SE", "ps_se"),
    row("Post-Pub (Add'l)", "pp"), row("SE", "pp_se"),
    paste0("   ", "\\\\"),
    row("Observations", "n"), row("Signals", "signals"),
    row("R$^2$", "r2"), row("Within R$^2$", "wr2"),
    paste0("   ", "\\\\"),
    paste0("   Predictor F.E.   & ", paste(rep("Yes", 6), collapse = " & "), " \\\\"),
    paste0(
      "   Time F.E.        & ",
      paste(rep(if (timeFE) "Yes" else "No", 6), collapse = " & "), " \\\\"
    ),
    "   \\bottomrule", "\\end{tabular}", "\\par\\endgroup"
  )

  dir.create(dirname(file), recursive = TRUE, showWarnings = FALSE)
  writeLines(lines, file)
  message("Wrote ", file)
}

# Convert etable's adjacent no-time-FE/time-FE ordering into the manuscript
# ordering, then write the two official presentation tables.
write_combined_mp_tables <- function(scaled_fits, unscaled_fits, n_signals, output_dir) {
  if (length(scaled_fits) != 6L || length(unscaled_fits) != 6L) {
    stop("Each MP table variant requires six scaled and six unscaled models.")
  }

  make_combined_table(
    c(scaled_fits[c(1, 3, 5)], unscaled_fits[c(1, 3, 5)]),
    n_signals,
    timeFE = FALSE,
    file = file.path(output_dir, "Table_MPStyleRegsNoTimeFE.tex")
  )
  make_combined_table(
    c(scaled_fits[c(2, 4, 6)], unscaled_fits[c(2, 4, 6)]),
    n_signals,
    timeFE = TRUE,
    file = file.path(output_dir, "Table_MPStyleRegsTimeFE.tex")
  )
}
