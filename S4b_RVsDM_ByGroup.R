# Render sample-specific risk-adjusted by-group tables for Section 4.
#
# How to run: normally run through S4_Heterogeneity.R.
# Inputs: raw_dm_benchmarks.RDS, risk_adjusted_dm_benchmarks.RDS, and
#   DataInput/SignalsTheoryChecked.csv
# Outputs: sample-specific CAPM/FF4 audit tables under
#   ../Results/RiskAdjusted/TstatFilter and paper-facing fragments under
#   ../Results.
#
# Factor fitting and DM alpha screening belong to 3c_FactorAdjustedDMPrep.R.
# Published-only post-sample means use the complete eligible published panel;
# paired differences use only months with an available DM benchmark.

rm(list = ls())
pdf(NULL)
source("0_Environment.R")
source("helpers/risk_adjusted_tables.R")

t_threshold <- 2
results_dir <- "../Results/RiskAdjusted/TstatFilter"
dir.create(results_dir, recursive = TRUE, showWarnings = FALSE)

required_files <- c(
  "../Data/Processed/raw_dm_benchmarks.RDS",
  "../Data/Processed/risk_adjusted_dm_benchmarks.RDS",
  "DataInput/SignalsTheoryChecked.csv"
)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0L) {
  stop("Missing Section 4 input(s): ", paste(missing_files, collapse = ", "))
}

raw_contract <- readRDS(required_files[1])
risk_contract <- readRDS(required_files[2])
if (!identical(risk_contract$metadata$schema_version, 2L)) {
  stop("Section 4 requires risk_adjusted_dm_benchmarks schema version 2.")
}
stopifnot(identical(
  raw_contract$metadata$accounting_t2$pair_fingerprint_sha256,
  risk_contract$metadata$base_pair_fingerprint_sha256
))

incl_signals <- restrictInclSignals(
  restrictType = globalSettings$restrictType,
  topT = globalSettings$topT
)
mappings <- load_signal_mappings(
  "DataInput/SignalsTheoryChecked.csv", incl_signals
)
theory_mapping <- mappings$theory_mapping
model_mapping <- mappings$model_mapping
discipline_mapping <- mappings$discipline_mapping_filtered
journal_mapping <- mappings$journal_mapping_filtered
anymodel_mapping <- mappings$czcat_full %>%
  transmute(
    signalname,
    model_binary = if_else(NoModel == 1, "No Model", "Any Model")
  )

# Build panels with all eligible published observations. DM values remain NA
# where no factor-adjusted comparator is available, so published-only means do
# not inherit the paired sample restriction.
raw_eligible <- risk_contract$published_stats %>%
  filter(eligible_raw_t2) %>%
  pull(signalname)
raw_panel <- raw_contract$published %>%
  filter(pubname %in% raw_eligible) %>%
  transmute(pubname, eventDate, date = calendarDate, ret = return) %>%
  left_join(
    raw_contract$accounting_t2 %>%
      transmute(pubname, eventDate, matchRet = return),
    by = c("pubname", "eventDate")
  )

factor_panel <- function(model, published_name, dm_name) {
  published <- risk_contract[[model]]$published_panel %>%
    transmute(
      pubname, eventDate, date = calendarDate,
      !!published_name := published_return
    )
  dm <- risk_contract[[model]]$panel %>%
    transmute(pubname, eventDate, !!dm_name := dm_return)
  published %>% left_join(dm, by = c("pubname", "eventDate"))
}
capm_panel <- factor_panel(
  "capm", "abnormal_capm_tv_normalized",
  "matchRet_capm_tv_t2_normalized"
)
ff4_panel <- factor_panel(
  "ff4", "abnormal_ff4_tv_normalized",
  "matchRet_ff4_tv_t2_normalized"
)

summaries_for <- function(mapping, group_col) {
  list(
    raw = compute_outperformance(raw_panel, "ret", "matchRet", mapping, group_col),
    capm = compute_outperformance(
      capm_panel, "abnormal_capm_tv_normalized",
      "matchRet_capm_tv_t2_normalized", mapping, group_col
    ),
    ff4 = compute_outperformance(
      ff4_panel, "abnormal_ff4_tv_normalized",
      "matchRet_ff4_tv_t2_normalized", mapping, group_col
    )
  )
}

theory_summaries <- summaries_for(theory_mapping, "theory_group")
model_summaries <- summaries_for(model_mapping, "modeltype_grouped")
discipline_summaries <- summaries_for(discipline_mapping, "discipline")
journal_summaries <- summaries_for(journal_mapping, "journal_rank")
anymodel_summaries <- summaries_for(anymodel_mapping, "model_binary")
overall_summaries <- list(
  raw = compute_overall_summary(raw_panel, "ret", "matchRet"),
  capm = compute_overall_summary(
    capm_panel, "abnormal_capm_tv_normalized",
    "matchRet_capm_tv_t2_normalized"
  ),
  ff4 = compute_overall_summary(
    ff4_panel, "abnormal_ff4_tv_normalized",
    "matchRet_ff4_tv_t2_normalized"
  )
)

extract_row <- function(summary, group_col, group) {
  row <- summary[summary[[group_col]] == group, , drop = FALSE]
  if (nrow(row) != 1L) stop("Expected one summary row for ", group)
  row
}
bundle <- function(summaries, group_col, group) {
  raw <- extract_row(summaries$raw, group_col, group)
  capm <- extract_row(summaries$capm, group_col, group)
  ff4 <- extract_row(summaries$ff4, group_col, group)
  list(
    raw_pub_oos = raw$pub_oos, raw_pub_oos_se = raw$pub_oos_se,
    raw_outperform = raw$outperform, raw_outperform_se = raw$outperform_se,
    capm_tv_pub_oos = capm$pub_oos, capm_tv_pub_oos_se = capm$pub_oos_se,
    capm_tv_outperform = capm$outperform,
    capm_tv_outperform_se = capm$outperform_se,
    ff3_tv_pub_oos = NA_real_, ff3_tv_pub_oos_se = NA_real_,
    ff3_tv_outperform = NA_real_, ff3_tv_outperform_se = NA_real_,
    ff4_tv_pub_oos = ff4$pub_oos, ff4_tv_pub_oos_se = ff4$pub_oos_se,
    ff4_tv_outperform = ff4$outperform,
    ff4_tv_outperform_se = ff4$outperform_se
  )
}
bundle_overall <- function(summaries) {
  list(
    raw_pub_oos = summaries$raw$pub_oos,
    raw_pub_oos_se = summaries$raw$pub_oos_se,
    raw_outperform = summaries$raw$outperform,
    raw_outperform_se = summaries$raw$outperform_se,
    capm_tv_pub_oos = summaries$capm$pub_oos,
    capm_tv_pub_oos_se = summaries$capm$pub_oos_se,
    capm_tv_outperform = summaries$capm$outperform,
    capm_tv_outperform_se = summaries$capm$outperform_se,
    ff3_tv_pub_oos = NA_real_, ff3_tv_pub_oos_se = NA_real_,
    ff3_tv_outperform = NA_real_, ff3_tv_outperform_se = NA_real_,
    ff4_tv_pub_oos = summaries$ff4$pub_oos,
    ff4_tv_pub_oos_se = summaries$ff4$pub_oos_se,
    ff4_tv_outperform = summaries$ff4$outperform,
    ff4_tv_outperform_se = summaries$ff4$outperform_se
  )
}

groups_theory <- c("Risk", "Mispricing", "Agnostic")
groups_model <- c("No Model", "Stylized", "Dynamic or Quantitative")
groups_discipline <- c("Finance", "Accounting")
groups_journal <- c("JF, JFE, RFS", "AR, JAR, JAE", "Other")
groups_anymodel <- c("No Model", "Any Model")

tv_theory_data <- setNames(lapply(
  groups_theory, function(x) bundle(theory_summaries, "theory_group", x)
), groups_theory)
tv_model_data <- setNames(lapply(
  groups_model, function(x) bundle(model_summaries, "modeltype_grouped", x)
), groups_model)
tv_discipline_data <- setNames(lapply(
  groups_discipline, function(x) bundle(discipline_summaries, "discipline", x)
), groups_discipline)
tv_journal_data <- setNames(lapply(
  groups_journal, function(x) bundle(journal_summaries, "journal_rank", x)
), groups_journal)
tv_anymodel_data <- setNames(lapply(
  groups_anymodel, function(x) bundle(anymodel_summaries, "model_binary", x)
), groups_anymodel)
tv_overall_data <- bundle_overall(overall_summaries)

theory_model_table <- build_tv_summary_table(
  c(rep("Theoretical Foundation", 3), rep("Modeling Formalism", 3), "Overall"),
  c(groups_theory, groups_model, "All"),
  c(tv_theory_data, tv_model_data, list(tv_overall_data))
) %>% select(-matches("^FF3_"))
discipline_journal_table <- build_tv_summary_table(
  c(rep("Discipline", 2), rep("Journal Rank", 3)),
  c(groups_discipline, groups_journal),
  c(tv_discipline_data, tv_journal_data)
) %>% select(-matches("^FF3_"))
anymodel_table <- build_tv_summary_table(
  c("", ""), groups_anymodel, tv_anymodel_data
) %>% select(-matches("^FF3_"))

suffix <- paste0("_ff4_t", t_threshold)
headers <- list(
  list(title = "Raw", span = 2),
  list(title = "CAPM", span = 2),
  list(title = "FF4", span = 2)
)
export_audit_tabular(
  theory_model_table,
  file.path(results_dir, paste0("Table_RiskAdjusted_TimeVarying", suffix)),
  headers
)
export_audit_tabular(
  discipline_journal_table,
  file.path(results_dir, paste0(
    "Table_RiskAdjusted_TimeVarying_DisciplineJournal", suffix
  )), headers
)
export_audit_tabular(
  anymodel_table,
  file.path(results_dir, paste0(
    "Table_RiskAdjusted_TimeVarying_AnyModelVsNoModel", suffix
  )), headers
)

write_paper_theory_model_tabular(
  tv_theory_data, tv_model_data, tv_overall_data,
  file.path("../Results", paste0(
    "Table_RiskAdjusted_TimeVarying", suffix, ".tex"
  ))
)
write_paper_discipline_journal_tabular(
  tv_discipline_data, tv_journal_data,
  file.path("../Results", paste0(
    "Table_RiskAdjusted_TimeVarying_DisciplineJournal", suffix, ".tex"
  ))
)
write_paper_anymodel_tabular(
  tv_anymodel_data,
  file.path("../Results", paste0(
    "Table_RiskAdjusted_TimeVarying_AnyModelVsNoModel", suffix, ".tex"
  ))
)

message("Wrote corrected sample-specific source artifacts for Tables 6, 7, and IA.10.")
