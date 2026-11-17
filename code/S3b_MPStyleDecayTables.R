# Render Section 3 MP-style decay tables from the cached regression models.
#
# How to run: normally run through S3_Learning.R with the working directory set to
#   flex-mining/.
# Inputs:  ../Data/Processed/mp_style_decay_models.RDS
# Outputs: ../Results/Table_MPStyleRegsNoTimeFE.tex
#          ../Results/Table_MPStyleRegsTimeFE.tex

source("0_Environment.R")

# Helpers for rendering the manuscript's MP-style decay tables.
#
# How to run: source this file from an R script.
# Inputs: six fixest models ordered as Panel (a) academic/DM/difference then
#         Panel (b) academic/DM/difference (Panel (a) scaled, Panel (b)
#         unscaled), plus a length-six vector of distinct published-signal
#         counts per column.
# Outputs: make_combined_table() writes one six-column TeX table.

# Two panels side by side: columns (1)--(3) are SCALED returns versus the broad
# "excluding correlated" data-mined benchmark; columns (4)--(6) are UNSCALED
# returns versus the performance-matched benchmark (matching on in-sample stats
# controls for magnitude in place of scaling).
make_combined_table <- function(fits, n_signals, timeFE, file) {
  if (length(fits) != 6L) {
    stop("Combined MP table requires exactly six models; got ", length(fits), ".")
  }
  if (length(n_signals) != 6L) {
    stop("Combined MP table requires six per-column signal counts; got ",
         length(n_signals), ".")
  }

  # Three significant digits, retaining meaningful trailing zeros so the
  # generated tables stay byte-stable with the manuscript's formatting.
  fmt <- function(x) {
    sub("\\.$", "", formatC(signif(x, 3), format = "fg", flag = "#", digits = 3))
  }
  fmt3 <- function(x) formatC(x, format = "f", digits = 3)
  cells <- mapply(function(fit, nsig) {
    ct <- fixest::coeftable(fit)
    c(
      ps = fmt(ct["postSample", "Estimate"]),
      ps_se = paste0("(", fmt(ct["postSample", "Std. Error"]), ")"),
      pp = fmt(ct["postPub", "Estimate"]),
      pp_se = paste0("(", fmt(ct["postPub", "Std. Error"]), ")"),
      n = formatC(as.numeric(fixest::fitstat(fit, "n")$n), format = "d", big.mark = ","),
      # Distinct published signals in the estimation sample. A property of the
      # sample rather than the fit (the DM-benchmark columns cluster on dmname),
      # so it is passed in per column; it differs across the two benchmark
      # panels because they retain different predictors.
      signals = formatC(nsig, format = "d", big.mark = ","),
      r2 = fmt3(as.numeric(fixest::fitstat(fit, "r2")$r2)),
      wr2 = fmt3(as.numeric(fixest::fitstat(fit, "wr2")$wr2))
    )
  }, fits, n_signals)
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
    "% GENERATED -- do not hand-edit (S3b_MPStyleDecayTables.R).",
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

# Each benchmark supplies six models (academic/DM/difference, without and with
# time FE). Table 3 takes the no-time-FE set from both benchmarks; Table 4 the
# time-FE set. Columns (1)--(3) are Panel (a), columns (4)--(6) Panel (b).
write_combined_mp_tables <- function(panel_a, panel_b, n_signals_a, n_signals_b,
                                     output_dir) {
  if (length(panel_a) != 6L || length(panel_b) != 6L) {
    stop("Each benchmark panel requires six models.")
  }
  sig6 <- c(rep(n_signals_a, 3), rep(n_signals_b, 3))

  make_combined_table(
    c(panel_a[c(1, 3, 5)], panel_b[c(1, 3, 5)]),
    sig6,
    timeFE = FALSE,
    file = file.path(output_dir, "Table_MPStyleRegsNoTimeFE.tex")
  )
  make_combined_table(
    c(panel_a[c(2, 4, 6)], panel_b[c(2, 4, 6)]),
    sig6,
    timeFE = TRUE,
    file = file.path(output_dir, "Table_MPStyleRegsTimeFE.tex")
  )
}

# Sourcing with MP_TABLE_FUNCTIONS_ONLY=true exposes the renderer to appendix
# variants without rewriting the main-text tables.
if (!identical(Sys.getenv("MP_TABLE_FUNCTIONS_ONLY"), "true")) {
  # An override permits render-only validation without touching ../Results.
  output_dir <- Sys.getenv("MP_TABLE_OUTPUT_DIR", unset = "../Results")
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

  cache_path <- "../Data/Processed/mp_style_decay_models.RDS"
  require_files(
    cache_path, "MP-style model cache",
    "Run S3a_MPStyleDecayModels.R first."
  )
  models <- readRDS(cache_path)
  stopifnot(
    !is.null(models$metadata$panel_a$pair_fingerprint_sha256),
    !is.null(models$metadata$panel_b$pair_fingerprint_sha256),
    length(models$panel_a) == 6L,
    length(models$panel_b) == 6L
  )

  # Manuscript Tables 3 and 4: the two benchmark panels side by side, without
  # and with time fixed effects.
  write_combined_mp_tables(
    models$panel_a,
    models$panel_b,
    n_signals_a = models$metadata$panel_a$predictor_count,
    n_signals_b = models$metadata$panel_b$predictor_count,
    output_dir = output_dir
  )
}
