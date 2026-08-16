# Render appendix-only full-sample CAPM/FF3 comparisons and grouped tables.
#
# How to run: normally run through SA_Appendices.R after 3_Precompute.R.
# Inputs: raw_dm_benchmarks.RDS,
#   appendix_full_sample_dm_benchmarks.RDS, and signal classifications
# Outputs: Figure IA.1 PDFs, full-sample audit tables, and paper-facing TeX
#   fragments under ../Results.

rm(list = ls())
pdf(NULL)
source("0_Environment.R")
source("helpers/factor_adjusted_tables.R")

t_threshold <- 2
full_path <- "../Data/Processed/appendix_full_sample_dm_benchmarks.RDS"
raw_path <- "../Data/Processed/raw_dm_benchmarks.RDS"
if (!all(file.exists(c(full_path, raw_path)))) {
  stop("Missing full-sample appendix inputs; rerun SA_Appendices.R.")
}
full <- readRDS(full_path)
raw <- readRDS(raw_path)
stopifnot(identical(
  full$metadata$base_pair_fingerprint_sha256,
  raw$metadata$accounting_t2$pair_fingerprint_sha256
))

incl_signals <- restrictInclSignals(
  globalSettings$restrictType, globalSettings$topT
)
mappings <- load_signal_mappings(
  "DataInput/SignalsTheoryChecked.csv", incl_signals
)
theory_mapping <- mappings$theory_mapping
model_mapping <- mappings$model_mapping
discipline_mapping <- mappings$discipline_mapping_filtered
journal_mapping <- mappings$journal_mapping_filtered
anymodel_mapping <- mappings$czcat_full %>% transmute(
  signalname,
  model_binary = if_else(NoModel == 1, "No Model", "Any Model")
)
theory <- fread("DataInput/SignalsTheoryChecked.csv") %>%
  select(signalname, theory)

raw_eligible <- full$published_stats %>%
  filter(eligible_raw_t2) %>% pull(signalname)
raw_panel <- raw$published %>%
  filter(pubname %in% raw_eligible) %>%
  transmute(pubname, eventDate, date = calendarDate, ret = return) %>%
  left_join(
    raw$accounting_t2 %>% transmute(pubname, eventDate, matchRet = return),
    by = c("pubname", "eventDate")
  )

factor_panel <- function(model, published_name, dm_name) {
  published <- full[[model]]$published_panel %>%
    transmute(
      pubname, eventDate, date = calendarDate,
      !!published_name := published_return
    )
  dm <- full[[model]]$panel %>%
    transmute(pubname, eventDate, !!dm_name := dm_return)
  published %>% left_join(dm, by = c("pubname", "eventDate"))
}
capm_panel <- factor_panel(
  "capm", "abnormal_capm_fs_normalized",
  "matchRet_capm_fs_t2_normalized"
)
ff3_panel <- factor_panel(
  "ff3", "abnormal_ff3_fs_normalized",
  "matchRet_ff3_fs_t2_normalized"
)

render_plot <- function(model, label, output) {
  plot_data <- full[[model]]$panel %>%
    left_join(theory, by = c("pubname" = "signalname")) %>%
    transmute(
      pubname, eventDate, calendarDate,
      ret = published_return, matchRet = dm_return, theory
    )
  plot <- ReturnPlotsWithDM_std_errors_indicators(
    dt = plot_data,
    basepath = "../Results/Fig_FullSampleFactorAdj",
    suffix = paste0(tolower(label), "_alpha_fs_t", t_threshold),
    rollmonths = 60, colors = colors, labelmatch = FALSE,
    yl = 0, yh = 125, xl = -360, xh = 300,
    legendlabels = c(
      paste0("Published (", label, ")"),
      paste0("Data-Mined (", label, ")"), "N/A"
    ),
    legendpos = c(35, 20) / 100, fontsize = 28,
    yaxislab = paste0("Trailing 5-Year ", label, " (bps pm)"),
    linesize = 1.5
  )
  ggsave(output, plot, width = 10, height = 8)
  message("Wrote ", output)
}
render_plot(
  "capm", "CAPM Alpha",
  "../Results/Fig_FullSampleFactorAdj_capm_alpha_fs_t2.pdf"
)
render_plot(
  "ff3", "FF3 Alpha",
  "../Results/Fig_FullSampleFactorAdj_ff3_alpha_fs_t2.pdf"
)

summaries_for <- function(mapping, group_col) list(
  raw = compute_outperformance(raw_panel, "ret", "matchRet", mapping, group_col),
  capm = compute_outperformance(
    capm_panel, "abnormal_capm_fs_normalized",
    "matchRet_capm_fs_t2_normalized", mapping, group_col
  ),
  ff3 = compute_outperformance(
    ff3_panel, "abnormal_ff3_fs_normalized",
    "matchRet_ff3_fs_t2_normalized", mapping, group_col
  )
)
theory_s <- summaries_for(theory_mapping, "theory_group")
model_s <- summaries_for(model_mapping, "modeltype_grouped")
discipline_s <- summaries_for(discipline_mapping, "discipline")
journal_s <- summaries_for(journal_mapping, "journal_rank")
anymodel_s <- summaries_for(anymodel_mapping, "model_binary")
overall_s <- list(
  raw = compute_overall_summary(raw_panel, "ret", "matchRet"),
  capm = compute_overall_summary(
    capm_panel, "abnormal_capm_fs_normalized",
    "matchRet_capm_fs_t2_normalized"
  ),
  ff3 = compute_overall_summary(
    ff3_panel, "abnormal_ff3_fs_normalized",
    "matchRet_ff3_fs_t2_normalized"
  )
)

row_for <- function(x, group_col, group) {
  row <- x[x[[group_col]] == group, , drop = FALSE]
  if (nrow(row) != 1L) stop("Expected one summary row for ", group)
  row
}
bundle <- function(x, group_col, group) {
  r <- row_for(x$raw, group_col, group)
  c <- row_for(x$capm, group_col, group)
  f <- row_for(x$ff3, group_col, group)
  list(
    raw_pub_oos = r$pub_oos, raw_pub_oos_se = r$pub_oos_se,
    raw_outperform = r$outperform, raw_outperform_se = r$outperform_se,
    capm_fs_pub_oos = c$pub_oos, capm_fs_pub_oos_se = c$pub_oos_se,
    capm_fs_outperform = c$outperform,
    capm_fs_outperform_se = c$outperform_se,
    ff3_fs_pub_oos = f$pub_oos, ff3_fs_pub_oos_se = f$pub_oos_se,
    ff3_fs_outperform = f$outperform,
    ff3_fs_outperform_se = f$outperform_se
  )
}
overall <- list(
  raw_pub_oos = overall_s$raw$pub_oos,
  raw_pub_oos_se = overall_s$raw$pub_oos_se,
  raw_outperform = overall_s$raw$outperform,
  raw_outperform_se = overall_s$raw$outperform_se,
  capm_fs_pub_oos = overall_s$capm$pub_oos,
  capm_fs_pub_oos_se = overall_s$capm$pub_oos_se,
  capm_fs_outperform = overall_s$capm$outperform,
  capm_fs_outperform_se = overall_s$capm$outperform_se,
  ff3_fs_pub_oos = overall_s$ff3$pub_oos,
  ff3_fs_pub_oos_se = overall_s$ff3$pub_oos_se,
  ff3_fs_outperform = overall_s$ff3$outperform,
  ff3_fs_outperform_se = overall_s$ff3$outperform_se
)

groups_theory <- c("Risk", "Mispricing", "Agnostic")
groups_model <- c("No Model", "Stylized", "Dynamic or Quantitative")
groups_discipline <- c("Finance", "Accounting")
groups_journal <- c("JF, JFE, RFS", "AR, JAR, JAE", "Other")
groups_anymodel <- c("No Model", "Any Model")
theory_data <- setNames(lapply(groups_theory, function(g) {
  bundle(theory_s, "theory_group", g)
}), groups_theory)
model_data <- setNames(lapply(groups_model, function(g) {
  bundle(model_s, "modeltype_grouped", g)
}), groups_model)
discipline_data <- setNames(lapply(groups_discipline, function(g) {
  bundle(discipline_s, "discipline", g)
}), groups_discipline)
journal_data <- setNames(lapply(groups_journal, function(g) {
  bundle(journal_s, "journal_rank", g)
}), groups_journal)
anymodel_data <- setNames(lapply(groups_anymodel, function(g) {
  bundle(anymodel_s, "model_binary", g)
}), groups_anymodel)

theory_model_table <- build_full_sample_summary_table(
  c(rep("Theoretical Explanation", 3), rep("Modeling Formalism", 3), "Overall"),
  c(groups_theory, groups_model, "All"),
  c(theory_data, model_data, list(overall))
)
discipline_journal_table <- build_full_sample_summary_table(
  c(rep("Discipline", 2), rep("Journal Rank", 3)),
  c(groups_discipline, groups_journal),
  c(discipline_data, journal_data)
)
anymodel_table <- build_full_sample_summary_table(
  c("", ""), groups_anymodel, anymodel_data
)

audit_dir <- "../Results/FactorAdjusted/FullSampleTstatFilter"
dir.create(audit_dir, recursive = TRUE, showWarnings = FALSE)
headers <- list(
  list(title = "Raw", span = 2),
  list(title = "CAPM", span = 2),
  list(title = "FF3", span = 2)
)
export_audit_tabular(
  theory_model_table,
  file.path(audit_dir, "Table_FactorAdjusted_FullSample_t2"), headers
)
export_audit_tabular(
  discipline_journal_table,
  file.path(audit_dir, "Table_FactorAdjusted_FullSample_DisciplineJournal_t2"),
  headers
)
export_audit_tabular(
  anymodel_table,
  file.path(audit_dir, "Table_FactorAdjusted_FullSample_AnyModelVsNoModel_t2"),
  headers
)
write_paper_fullsample_theory_model_tabular(
  theory_data, model_data, overall,
  "../Results/Table_FactorAdjusted_FullSample_Appendix.tex"
)
write_paper_fullsample_discipline_journal_tabular(
  discipline_data, journal_data,
  "../Results/Table_FactorAdjusted_FullSample_DisciplineJournal_Appendix.tex"
)
write_paper_fullsample_anymodel_tabular(
  anymodel_data,
  "../Results/Table_FactorAdjusted_FullSample_AnyModelVsNoModel_Appendix.tex"
)
message("Wrote appendix-only full-sample figures and tables.")
