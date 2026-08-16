# Focused checks for Figure 2 benchmark ownership and cache schemas.
#
# How to run: from flex-mining/, run
#   Rscript tests/test_fig2_benchmark_contracts.R
# Inputs: Chapter 3 producers, the Section 2 assembler, and optional generated
#         benchmark caches under ../Data/Processed
# Outputs: none; exits nonzero on failure.

raw_producer <- readLines("3a_PrepDMBenchmarks.R")
risk_producer <- readLines("3e_FactorAdjustedDMPrep.R")
matched_producer <- readLines("3d_MatchedUncorrData.R")
span_producer <- readLines("3f_DMSpanPCA.R")
figure_producer <- readLines("S2e_Fig2Plots.R")
precompute <- readLines("3_Precompute.R")
section_driver <- readLines("S2_ResearchVsDataMining.R")
section_renderer <- readLines("S2a_ResearchVsDMPlots.R")

stopifnot(
  any(grepl("raw_dm_benchmarks.RDS", raw_producer, fixed = TRUE)),
  any(grepl("risk_adjusted_dm_benchmarks.RDS", risk_producer, fixed = TRUE)),
  any(grepl("matched_uncorr_benchmark.RDS", matched_producer, fixed = TRUE)),
  any(grepl('run_script("3a_PrepDMBenchmarks.R")', precompute, fixed = TRUE)),
  any(grepl('run_script("3d_MatchedUncorrData.R")', precompute, fixed = TRUE)),
  any(grepl('run_script("3e_FactorAdjustedDMPrep.R")', precompute, fixed = TRUE)),
  !file.exists("3a_ResearchVsDMPrep.R"),
  !any(grepl("risk_adjusted|FamaFrenchFactors|risk_adjusted_helpers",
             raw_producer)),
  all(vapply(
    c("raw_dm_benchmarks.RDS", "matched_uncorr_benchmark.RDS",
      "risk_adjusted_dm_benchmarks.RDS"),
    function(cache) any(grepl(cache, figure_producer, fixed = TRUE)),
    logical(1)
  )),
  !any(grepl("MatchPub|PairwiseCorrelations|make_DM_event_returns|SelectDMStrats",
             figure_producer)),
  !any(grepl("3d_Fig2Data.R", precompute, fixed = TRUE)),
  !file.exists("3d_Fig2Data.R"),
  !any(grepl("ret_for_plot_MaxPredictors", raw_producer, fixed = TRUE)),
  !any(grepl("ret_for_plot_MaxPredictors", section_driver, fixed = TRUE)),
  !any(grepl("ret_for_plot_MaxPredictors", section_renderer, fixed = TRUE)),
  !any(grepl("plotdat0", raw_producer, fixed = TRUE)),
  !any(grepl("dm_pca_span_classification.RDS", span_producer, fixed = TRUE)),
  !any(grepl("MaxDMpredsPerPublished", section_renderer, fixed = TRUE))
)

raw_path <- "../Data/Processed/raw_dm_benchmarks.RDS"
if (file.exists(raw_path)) {
  raw <- readRDS(raw_path)
  stopifnot(
    all(c("published", "accounting_t2", "accounting_top5", "ticker_top5",
          "metadata") %in% names(raw)),
    all(vapply(
      raw[c("published", "accounting_t2", "accounting_top5", "ticker_top5")],
      function(x) all(c("pubname", "eventDate", "calendarDate", "return") %in%
                        names(x)), logical(1)
    )),
    identical(raw$metadata$schema_version, 1L)
  )
}

risk_path <- "../Data/Processed/risk_adjusted_dm_benchmarks.RDS"
if (file.exists(risk_path)) {
  risk <- readRDS(risk_path)
  stopifnot(
    all(c("capm", "ff4", "published_stats", "pair_stats", "metadata") %in%
          names(risk)),
    all(vapply(risk[c("capm", "ff4")], function(model) {
      all(c("pubname", "eventDate", "calendarDate", "published_return",
            "dm_return", "n_eligible_pairs", "n_pairs_available") %in%
            names(model$panel))
    }, logical(1))),
    all(c("eligible_raw_t2", "eligible_capm_t2", "eligible_ff4_t2") %in%
          names(risk$published_stats)),
    all(c("eligible_capm_t2", "eligible_ff4_t2") %in%
          names(risk$pair_stats)),
    identical(risk$metadata$schema_version, 1L)
  )
}

# The pre-refactor display caches remain useful characterization oracles until
# a full Chapter 3 regeneration has established equivalence.
legacy_long_path <- "../Data/Processed/fig2_panel_long.RDS"
legacy_agg_path <- "../Data/Processed/fig2_panel_agg.RDS"
if (file.exists(legacy_long_path) && file.exists(legacy_agg_path)) {
  legacy_long <- readRDS(legacy_long_path)
  legacy_agg <- readRDS(legacy_agg_path)
  expected_panel_rows <- c(a = 306774L, b = 161368L,
                           c = 244212L, d = 310944L)
  actual_panel_rows <- table(legacy_long$panel)
  stopifnot(
    identical(names(legacy_long),
              c("label", "pubname", "eventDate", "calendarDate", "return", "panel")),
    identical(names(legacy_agg),
              c("panel", "label", "eventDate", "roll_rbar", "se", "upper", "lower")),
    identical(sort(unique(legacy_long$panel)), letters[1:4]),
    identical(as.integer(actual_panel_rows[names(expected_panel_rows)]),
              unname(expected_panel_rows)),
    nrow(legacy_long) == 1023298L,
    nrow(legacy_agg) == 17229L,
    range(legacy_long$eventDate) == c(-701, 660)
  )
}

# To compare a candidate S2e assembly with the legacy oracle, run S2e with
# FIG2_DATA_OUTPUT_DIR set and point this test at that directory.
candidate_dir <- Sys.getenv("FIG2_CANDIDATE_DIR", unset = "")
if (nzchar(candidate_dir)) {
  if (!file.exists(legacy_long_path) || !file.exists(legacy_agg_path)) {
    stop("FIG2_CANDIDATE_DIR comparison requires the legacy Figure 2 oracles.")
  }
  candidate_long <- readRDS(file.path(candidate_dir, "fig2_panel_long.RDS"))
  candidate_agg <- readRDS(file.path(candidate_dir, "fig2_panel_agg.RDS"))
  sort_long <- function(x) {
    x[order(x$panel, x$label, x$pubname, x$eventDate), ]
  }
  sort_agg <- function(x) {
    x[order(x$panel, x$label, x$eventDate), ]
  }
  stopifnot(
    isTRUE(all.equal(sort_long(candidate_long), sort_long(legacy_long),
                     tolerance = 1e-12, check.attributes = FALSE)),
    isTRUE(all.equal(sort_agg(candidate_agg), sort_agg(legacy_agg),
                     tolerance = 1e-12, check.attributes = FALSE))
  )
}
