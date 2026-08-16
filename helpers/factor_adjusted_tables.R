# Shared inference and table helpers for factor-adjusted exhibits.
#
# How to run: source after 0_Environment.R from the Section 4 or SA07
#   renderer.
# Inputs: prepared published/DM panels, group mappings, and summary objects
#   supplied to the functions below.
# Outputs: in-memory clustered estimates and formatted tables; export and
#   write functions create only their explicitly requested output files.
# Means are intercept-only OLS estimates. Inference uses sandwich::vcovCL with
# two-way clustering by calendar month and published predictor, HC1 scaling,
# and the package default G/(G - 1) cluster adjustment (cadjust = TRUE).
estimate_clustered_mean <- function(data, value_col, context) {
  required <- c("pubname", "date", value_col)
  missing <- setdiff(required, names(data))
  if (length(missing) > 0) {
    stop(context, ": missing estimation column(s): ", paste(missing, collapse = ", "))
  }

  estimation_data <- data %>%
    transmute(
      pubname = as.character(pubname),
      date = date,
      value = .data[[value_col]]
    ) %>%
    filter(complete.cases(pubname, date, value))
  estimation_data <- as.data.frame(estimation_data)

  if (nrow(estimation_data) == 0) stop(context, ": no complete observations")
  duplicate_keys <- estimation_data %>% count(pubname, date) %>% filter(n != 1)
  if (nrow(duplicate_keys) > 0) {
    examples <- duplicate_keys %>%
      head(10) %>%
      transmute(key = paste0(pubname, "@", date, " (n=", n, ")")) %>%
      pull(key)
    stop(
      context, ": expected one row per (pubname, date), found ",
      nrow(duplicate_keys), " duplicated key(s): ", paste(examples, collapse = ", ")
    )
  }

  n_months <- n_distinct(estimation_data$date)
  n_predictors <- n_distinct(estimation_data$pubname)
  if (n_months < 2 || n_predictors < 2) {
    stop(
      context, ": two-way clustered inference needs at least two nonempty ",
      "clusters in each dimension; months=", n_months,
      ", predictors=", n_predictors, ", observations=", nrow(estimation_data)
    )
  }

  fit <- stats::lm(value ~ 1, data = estimation_data)
  variance <- unname(sandwich::vcovCL(
    fit,
    cluster = estimation_data[c("date", "pubname")],
    type = "HC1",
    cadjust = TRUE,
    multi0 = FALSE,
    fix = FALSE
  )[1, 1])
  if (!is.finite(variance) || variance < -sqrt(.Machine$double.eps)) {
    stop(context, ": invalid two-way clustered variance: ", variance)
  }

  list(
    estimate = unname(stats::coef(fit)[1]),
    se = sqrt(max(variance, 0)),
    n_obs = nrow(estimation_data),
    n_months = n_months,
    n_predictors = n_predictors
  )
}

summarize_outperformance_group <- function(data, ret_col, dm_ret_col, context) {
  oos_rows <- if ("sampend" %in% names(data)) {
    !is.na(data$date > data$sampend) & data$date > data$sampend
  } else {
    !is.na(data$eventDate > 0) & data$eventDate > 0
  }
  oos <- data[oos_rows, , drop = FALSE]
  pub <- estimate_clustered_mean(oos, ret_col, paste0(context, " published"))
  dm <- estimate_clustered_mean(oos, dm_ret_col, paste0(context, " data-mined"))

  paired <- oos %>%
    filter(complete.cases(.data[[ret_col]], .data[[dm_ret_col]])) %>%
    mutate(.paired_difference = .data[[ret_col]] - .data[[dm_ret_col]])
  difference <- estimate_clustered_mean(
    paired, ".paired_difference", paste0(context, " paired difference")
  )

  pub_complete <- !is.na(oos[[ret_col]])
  dm_complete <- !is.na(oos[[dm_ret_col]])
  if (identical(pub_complete, dm_complete)) {
    separate_difference <- pub$estimate - dm$estimate
    if (!isTRUE(all.equal(
      difference$estimate, separate_difference,
      tolerance = 1e-10, check.attributes = FALSE
    ))) {
      stop(
        context, ": paired mean ", difference$estimate,
        " disagrees with difference of means ", separate_difference,
        " despite identical samples"
      )
    }
  }

  tibble(
    n_signals = n_distinct(oos$pubname),
    n_pub_obs = pub$n_obs,
    n_dm_obs = dm$n_obs,
    n_pairs = difference$n_obs,
    n_month_clusters = difference$n_months,
    n_predictor_clusters = difference$n_predictors,
    pub_oos = pub$estimate,
    pub_oos_se = pub$se,
    dm_oos = dm$estimate,
    dm_oos_se = dm$se,
    outperform = difference$estimate,
    outperform_se = difference$se
  )
}

compute_outperformance <- function(plot_data, ret_col, dm_ret_col, group_map,
                                   group_col = "theory_group") {
  if (anyDuplicated(group_map$signalname)) {
    duplicate_signals <- unique(group_map$signalname[duplicated(group_map$signalname)])
    stop(
      "Group mapping has duplicate signalname rows: ",
      paste(head(duplicate_signals, 10), collapse = ", ")
    )
  }

  plot_data %>%
    left_join(group_map, by = c("pubname" = "signalname")) %>%
    filter(!is.na(.data[[group_col]])) %>%
    group_by(.data[[group_col]]) %>%
    group_modify(~ summarize_outperformance_group(
      .x, ret_col, dm_ret_col,
      paste0(group_col, "=", .y[[group_col]])
    )) %>%
    ungroup()
}

round_zero <- function(x, digits = 0) {
  rounded <- round(x, digits)
  if (abs(rounded) < 10^(-digits)) return(0)
  return(rounded)
}
format_value_se <- function(value, se, digits = 0, latex = FALSE) {
  if (is.null(value) || is.null(se) || length(value) == 0 || length(se) == 0) return(NA)
  if (is.na(value) || is.na(se)) return(NA)
  value_rounded <- round_zero(value, digits)
  se_rounded <- round_zero(se, digits)
  if (latex) {
    return(sprintf("$%.*f$ ($%.*f$)", digits, value_rounded, digits, se_rounded))
  } else {
    return(sprintf("%.*f (%.*f)", digits, value_rounded, digits, se_rounded))
  }
}
create_formatted_latex_table <- function(table_data, caption = "", label = "",
                                        group_headers = NULL, placement = "htbp",
                                        separate_se_rows = TRUE,
                                        show_category_as_section = TRUE) {
  # Build a pretty LaTeX table manually with optional grouped headers
  # Expect first columns to be Category and Group (if available); remaining columns are metric columns
  if (!is.data.frame(table_data)) {
    stop("table_data must be a data.frame")
  }
  n_cols <- ncol(table_data)
  if (n_cols < 1) {
    stop("table_data must have at least one column")
  }
  # Auto-detect table shape
  has_category <- "Category" %in% names(table_data)
  has_group <- "Group" %in% names(table_data)
  mode <- if (show_category_as_section && has_category) {
    "section"
  } else if (has_category && has_group) {
    "columns"
  } else {
    "group_only"
  }
  # Determine visible columns and column spec
  if (mode == "section") {
    visible_cols <- names(table_data)[-1]  # drop Category from visible; keep Group + metrics
    num_metric_cols <- length(visible_cols) - 1
    col_spec <- paste0("l", paste(rep("c", max(0, num_metric_cols)), collapse = ""))
  } else if (mode == "columns") {
    visible_cols <- names(table_data)
    num_metric_cols <- length(visible_cols) - 2
    col_spec <- paste0("ll", paste(rep("c", max(0, num_metric_cols)), collapse = ""))
  } else { # group_only
    visible_cols <- names(table_data)
    num_metric_cols <- length(visible_cols) - 1
    col_spec <- paste0("l", paste(rep("c", max(0, num_metric_cols)), collapse = ""))
  }
  # Start LaTeX
  lines <- c()
  lines <- c(lines, sprintf("\\begin{table}[%s]", placement))
  lines <- c(lines, "\\centering")
  lines <- c(lines, "\\small")
  if (nzchar(caption)) lines <- c(lines, sprintf("\\caption{%s}", caption))
  if (nzchar(label))   lines <- c(lines, sprintf("\\label{%s}", label))
  lines <- c(lines, sprintf("\\begin{tabular}{%s}", col_spec))
  lines <- c(lines, "\\toprule")
  # Grouped headers row (optional)
  if (!is.null(group_headers) && length(group_headers) > 0) {
    header_cells <- if (mode == "columns") c(" ", " ") else c(" ")
    for (gh in group_headers) {
      title <- gh$title
      span  <- gh$span
      header_cells <- c(header_cells, sprintf("\\multicolumn{%d}{c}{%s}", span, title))
    }
    lines <- c(lines, paste(header_cells, collapse = " & "), "\\\\")
    # cmidrules under grouped headers
    start_col <- if (mode == "columns") 3 else 2
    cmids <- c()
    offset <- 0
    for (gh in group_headers) {
      span <- gh$span
      left <- start_col + offset
      right <- left + span - 1
      cmids <- c(cmids, sprintf("\\cmidrule(lr){%d-%d}", left, right))
      offset <- offset + span
    }
    lines <- c(lines, paste(cmids, collapse = " "))
  }
  # Second header row with metric labels
  metric_labels <- character()
  if (num_metric_cols > 0) {
    if (mode == "section") {
      metric_cols <- visible_cols[-1]
    } else if (mode == "columns") {
      metric_cols <- visible_cols[-(1:2)]
    } else { # group_only
      metric_cols <- visible_cols[-1]
    }
    for (cn in metric_cols) {
      if (grepl("_Return$", cn)) {
        metric_labels <- c(metric_labels, "Return")
      } else if (grepl("_Outperformance$", cn)) {
        metric_labels <- c(metric_labels, "Outperf.")
      } else {
        metric_labels <- c(metric_labels, cn)
      }
    }
  }
  header_row <- switch(mode,
    section = c("Group", metric_labels),
    columns = c("Category", "Group", metric_labels),
    group_only = c("Group", metric_labels)
  )
  lines <- c(lines, paste(header_row, collapse = " & "), "\\\\")
  lines <- c(lines, "\\midrule")
  # Helper to split "value (se)" into separate lines
  split_val_se <- function(cell) {
    if (is.na(cell) || is.null(cell) || cell == "") return(list(val = "", se = ""))
    s <- as.character(cell)
    m <- regexec("^\\s*([^\\(]+?)\\s*(?:\\(([^\\)]*)\\))?\\s*$", s)
    reg <- regmatches(s, m)
    if (length(reg) > 0 && length(reg[[1]]) >= 2) {
      val <- trimws(reg[[1]][2])
      se  <- if (length(reg[[1]]) >= 3) trimws(reg[[1]][3]) else ""
      return(list(val = val, se = se))
    }
    list(val = s, se = "")
  }
  # Rows
  prev_cat <- NULL
  for (i in seq_len(nrow(table_data))) {
    row <- table_data[i, , drop = FALSE]
    # Optional category section header
    if (mode == "section") {
      cat_val <- as.character(row[[1]])
      if (is.null(prev_cat) || (!identical(prev_cat, cat_val))) {
        lines <- c(lines, sprintf("\\multicolumn{%d}{l}{\\textbf{%s}}\\\\", max(1, 1 + num_metric_cols), cat_val))
        lines <- c(lines, "\\addlinespace[2pt]")
        prev_cat <- cat_val
      }
    }
    # Values row
    vals <- c()
    if (mode == "section") {
      vals <- c(vals, as.character(row[[2]]))  # Group
      value_cols <- names(row)[-(1:2)]
    } else if (mode == "columns") {
      vals <- c(vals, as.character(row[[1]]), as.character(row[[2]]))
      value_cols <- names(row)[-(1:2)]
    } else { # group_only
      vals <- c(vals, as.character(row[[1]]))
      value_cols <- names(row)[-1]
    }
    for (cn in value_cols) {
      sp <- split_val_se(row[[cn]])
      vals <- c(vals, sp$val)
    }
    lines <- c(lines, paste(vals, collapse = " & "), "\\\\")
    # SE row
    if (separate_se_rows && length(value_cols) > 0) {
      ses <- switch(mode,
        section = c(""),
        columns = c("", ""),
        group_only = c("")
      )
      for (cn in value_cols) {
        sp <- split_val_se(row[[cn]])
        ses <- c(ses, if (nzchar(sp$se)) sprintf("(%s)", sp$se) else "")
      }
      lines <- c(lines, paste(ses, collapse = " & "), "\\\\[2pt]")
    }
  }
  lines <- c(lines, "\\bottomrule")
  lines <- c(lines, "\\end{tabular}")
  lines <- c(lines, "\\end{table}")
  paste(lines, collapse = "\n")
}

build_tv_summary_table <- function(categories, groups, summaries, digits = 0) {
  result_df <- data.frame(
    Category = categories,
    Group = groups,
    stringsAsFactors = FALSE
  )
  if (all(sapply(summaries, function(x) all(c("raw_pub_oos", "raw_pub_oos_se", "raw_outperform", "raw_outperform_se") %in% names(x))))) {
    result_df[["Raw_Return"]] <- mapply(function(grp, cat_data) {
      val <- cat_data[["raw_pub_oos"]]
      se <- cat_data[["raw_pub_oos_se"]]
      format_value_se(val, se, digits, FALSE)
    }, groups, summaries, SIMPLIFY = TRUE)
    result_df[["Raw_Outperformance"]] <- mapply(function(grp, cat_data) {
      val <- cat_data[["raw_outperform"]]
      se <- cat_data[["raw_outperform_se"]]
      format_value_se(val, se, digits, FALSE)
    }, groups, summaries, SIMPLIFY = TRUE)
  }
  for (analysis in c("capm_tv", "ff3_tv", "ff4_tv")) {
    analysis_label <- switch(analysis,
                           "capm_tv" = "CAPM",
                           "ff3_tv" = "FF3",
                           "ff4_tv" = "FF4",
                           analysis)
    col_name <- paste0(analysis_label, "_Return")
    result_df[[col_name]] <- mapply(function(grp, cat_data) {
      val <- cat_data[[paste0(analysis, "_pub_oos")]]
      se <- cat_data[[paste0(analysis, "_pub_oos_se")]]
      format_value_se(val, se, digits, FALSE)
    }, groups, summaries, SIMPLIFY = TRUE)
    col_name <- paste0(analysis_label, "_Outperformance")
    result_df[[col_name]] <- mapply(function(grp, cat_data) {
      val <- cat_data[[paste0(analysis, "_outperform")]]
      se <- cat_data[[paste0(analysis, "_outperform_se")]]
      format_value_se(val, se, digits, FALSE)
    }, groups, summaries, SIMPLIFY = TRUE)
  }
  return(result_df)
}

build_full_sample_summary_table <- function(categories, groups, summaries,
                                            digits = 0) {
  result_df <- data.frame(
    Category = categories,
    Group = groups,
    stringsAsFactors = FALSE
  )
  metric_specs <- list(
    Raw_Return = c("raw_pub_oos", "raw_pub_oos_se"),
    Raw_Outperformance = c("raw_outperform", "raw_outperform_se"),
    CAPM_Return = c("capm_fs_pub_oos", "capm_fs_pub_oos_se"),
    CAPM_Outperformance = c("capm_fs_outperform", "capm_fs_outperform_se"),
    FF3_Return = c("ff3_fs_pub_oos", "ff3_fs_pub_oos_se"),
    FF3_Outperformance = c("ff3_fs_outperform", "ff3_fs_outperform_se")
  )
  for (column in names(metric_specs)) {
    fields <- metric_specs[[column]]
    result_df[[column]] <- vapply(
      summaries,
      function(summary) {
        format_value_se(summary[[fields[1]]], summary[[fields[2]]], digits, FALSE)
      },
      character(1)
    )
  }
  result_df
}
# Write the phase-one paper-source contract: a review CSV and a LaTeX tabular
# fragment. Captions, labels, notes, and the table float belong to the writing
# repository, so only the tabular environment is retained here.
export_audit_tabular <- function(table_data, base_filename, group_headers) {
  csv_file <- paste0(base_filename, ".csv")
  tex_file <- paste0(base_filename, ".tex")

  write.csv(table_data, csv_file, row.names = FALSE)
  latex <- create_formatted_latex_table(
    table_data = table_data,
    group_headers = group_headers
  )
  latex_lines <- strsplit(latex, "\n", fixed = TRUE)[[1]]
  first <- grep("^\\\\begin\\{tabular\\}", latex_lines)[1]
  last <- tail(grep("^\\\\end\\{tabular\\}", latex_lines), 1)
  if (is.na(first) || is.na(last) || first > last) {
    stop("Could not isolate tabular environment for ", tex_file)
  }
  writeLines(latex_lines[first:last], tex_file)
  invisible(c(csv = csv_file, tex = tex_file))
}

# Render the paper-facing versions of Tables 6, 7, and IA.10. These functions
# take the unrounded summary lists rather than the formatted audit data frames,
# so the paper and audit outputs share estimands but own separate presentation.
paper_summary_cells <- function(summary, adjustment = c("time_varying", "full_sample")) {
  adjustment <- match.arg(adjustment)
  adjusted_metrics <- if (adjustment == "time_varying") {
    c("capm_tv", "ff4_tv")
  } else {
    c("capm_fs", "ff3_fs")
  }
  metric_names <- c(
    "raw_pub_oos", "raw_outperform",
    paste0(adjusted_metrics[1], c("_pub_oos", "_outperform")),
    paste0(adjusted_metrics[2], c("_pub_oos", "_outperform"))
  )
  se_names <- paste0(metric_names, "_se")
  list(
    values = as.character(round(unlist(summary[metric_names], use.names = FALSE))),
    ses = paste0("(", round(unlist(summary[se_names], use.names = FALSE)), ")")
  )
}

paper_table_header <- function(main_paper = TRUE, third_adjustment = "FF4",
                               outperformance_label = "Outperf.") {
  if (main_paper) {
    c(
      "\\begin{tabular}{lcccccc}",
      "   \\toprule",
      "   & \\multicolumn{2}{c}{Long-Short Return} & \\multicolumn{2}{c}{CAPM Alpha} & \\multicolumn{2}{c}{FF3 + Mom Alpha} \\\\",
      "   \\cmidrule(lr){2-3}\\cmidrule(lr){4-5}\\cmidrule(lr){6-7}",
      "   & Post- & \\multicolumn{1}{c}{Versus} & Post- & \\multicolumn{1}{c}{Versus} & Post- & \\multicolumn{1}{c}{Versus} \\\\",
      "   & Sample & \\multicolumn{1}{c}{Data Mining} & Sample & \\multicolumn{1}{c}{Data Mining} & Sample & \\multicolumn{1}{c}{Data Mining} \\\\",
      "   \\midrule"
    )
  } else {
    c(
      "\\begin{tabular}{lcccccc}",
      "   \\toprule",
      paste0(
        "   & \\multicolumn{2}{c}{Raw} & \\multicolumn{2}{c}{CAPM} & ",
        "\\multicolumn{2}{c}{", third_adjustment, "} \\\\"
      ),
      "   \\cmidrule(lr){2-3}\\cmidrule(lr){4-5}\\cmidrule(lr){6-7}",
      paste0(
        "   & Return & ", outperformance_label,
        " & Return & ", outperformance_label,
        " & Return & ", outperformance_label, " \\\\"
      ),
      "   \\midrule"
    )
  }
}

paper_standard_row <- function(label, summary, bold = FALSE,
                               adjustment = "time_varying") {
  cells <- paper_summary_cells(summary, adjustment)
  shown_label <- if (bold) paste0("\\textbf{", label, "}") else label
  c(
    paste0("   ", shown_label, " & ", paste(cells$values, collapse = " & "), " \\\\"),
    paste0("   & ", paste(cells$ses, collapse = " & "), " \\\\")
  )
}

write_paper_theory_model_tabular <- function(theory, model, overall, file) {
  lines <- c(
    paper_table_header(TRUE),
    "   \\multicolumn{7}{l}{\\textbf{Theoretical Foundation}} \\\\",
    paper_standard_row("Agnostic", theory[["Agnostic"]]),
    paper_standard_row("Mispricing", theory[["Mispricing"]]),
    paper_standard_row("Risk", theory[["Risk"]]),
    "   \\addlinespace",
    "   \\multicolumn{7}{l}{\\textbf{Equilibrium Modeling}} \\\\",
    paper_standard_row("No Model", model[["No Model"]]),
    paper_standard_row("Stylized", model[["Stylized"]])
  )
  dynamic <- paper_summary_cells(model[["Dynamic or Quantitative"]])
  lines <- c(
    lines,
    paste0("   Dynamic or & ", paste(dynamic$values, collapse = " & "), " \\\\"),
    paste0("   Quantitative & ", paste(dynamic$ses, collapse = " & "), " \\\\"),
    "   \\addlinespace",
    paper_standard_row("Overall", overall, bold = TRUE),
    "   \\bottomrule",
    "\\end{tabular}"
  )
  writeLines(lines, file)
  invisible(file)
}

write_paper_discipline_journal_tabular <- function(discipline, journal, file) {
  lines <- c(
    paper_table_header(TRUE),
    "   \\multicolumn{7}{l}{\\textbf{Discipline}} \\\\",
    paper_standard_row("Finance", discipline[["Finance"]]),
    paper_standard_row("Accounting", discipline[["Accounting"]]),
    "   \\addlinespace",
    "   \\multicolumn{7}{l}{\\textbf{Journal Rank}} \\\\",
    paper_standard_row("JF, JFE, RFS", journal[["JF, JFE, RFS"]]),
    paper_standard_row("AR, JAR, JAE", journal[["AR, JAR, JAE"]]),
    paper_standard_row("Other", journal[["Other"]]),
    "   \\bottomrule",
    "\\end{tabular}"
  )
  writeLines(lines, file)
  invisible(file)
}

write_paper_anymodel_tabular <- function(anymodel, file) {
  lines <- c(
    paper_table_header(FALSE),
    paper_standard_row("No Model", anymodel[["No Model"]]),
    paper_standard_row("Any Model", anymodel[["Any Model"]]),
    "   \\bottomrule",
    "\\end{tabular}"
  )
  writeLines(lines, file)
  invisible(file)
}

write_paper_fullsample_theory_model_tabular <- function(theory, model, overall, file) {
  row <- function(label, summary) {
    paper_standard_row(label, summary, adjustment = "full_sample")
  }
  lines <- c(
    paper_table_header(FALSE, third_adjustment = "FF3"),
    "   \\multicolumn{7}{l}{\\textbf{Theoretical Explanation}} \\\\",
    row("Risk", theory[["Risk"]]),
    row("Mispricing", theory[["Mispricing"]]),
    row("Agnostic", theory[["Agnostic"]]),
    "   \\addlinespace",
    "   \\multicolumn{7}{l}{\\textbf{Modeling Formalism}} \\\\",
    row("No Model", model[["No Model"]]),
    row("Stylized", model[["Stylized"]]),
    row("Dynamic or Quantitative", model[["Dynamic or Quantitative"]]),
    "   \\addlinespace",
    "   \\multicolumn{7}{l}{\\textbf{Overall}} \\\\",
    row("All", overall),
    "   \\bottomrule",
    "\\end{tabular}"
  )
  writeLines(lines, file)
  invisible(file)
}

write_paper_fullsample_discipline_journal_tabular <- function(discipline, journal, file) {
  row <- function(label, summary) {
    paper_standard_row(label, summary, adjustment = "full_sample")
  }
  lines <- c(
    paper_table_header(FALSE, third_adjustment = "FF3"),
    "   \\multicolumn{7}{l}{\\textbf{Discipline}} \\\\",
    row("Finance", discipline[["Finance"]]),
    row("Accounting", discipline[["Accounting"]]),
    "   \\addlinespace",
    "   \\multicolumn{7}{l}{\\textbf{Journal Rank}} \\\\",
    row("JF, JFE, RFS", journal[["JF, JFE, RFS"]]),
    row("AR, JAR, JAE", journal[["AR, JAR, JAE"]]),
    row("Other", journal[["Other"]]),
    "   \\bottomrule",
    "\\end{tabular}"
  )
  writeLines(lines, file)
  invisible(file)
}

write_paper_fullsample_anymodel_tabular <- function(anymodel, file) {
  lines <- c(
    paper_table_header(FALSE, third_adjustment = "FF3"),
    paper_standard_row(
      "No Model", anymodel[["No Model"]], adjustment = "full_sample"
    ),
    paper_standard_row(
      "Any Model", anymodel[["Any Model"]], adjustment = "full_sample"
    ),
    "   \\bottomrule",
    "\\end{tabular}"
  )
  writeLines(lines, file)
  invisible(file)
}

compute_overall_summary <- function(plot_data, ret_col, dm_col) {
  result <- summarize_outperformance_group(
    plot_data, ret_col, dm_col, "group=Overall"
  )
  result %>% mutate(group = "Overall", .before = 1)
} 
load_signal_mappings <- function(signals_checked_csv, incl_signals) {
  czcat_full <- fread(signals_checked_csv) %>%
    select(signalname, Year, theory, Journal, NoModel, Stylized, Dynamic, Quantitative) %>%
    filter(signalname %in% incl_signals) %>%
    mutate(
      modeltype = case_when(
        NoModel == 1 ~ "No Model",
        Stylized == 1 ~ "Stylized",
        Dynamic == 1 ~ "Dynamic",
        Quantitative == 1 ~ "Quantitative"
      ),
      modeltype_grouped = case_when(
        modeltype == "No Model" ~ "No Model",
        modeltype == "Stylized" ~ "Stylized",
        modeltype %in% c("Dynamic", "Quantitative") ~ "Dynamic or Quantitative"
      ),
      theory_group = case_when(
        theory %in% c("risk", "Risk") ~ "Risk",
        theory %in% c("mispricing", "Mispricing") ~ "Mispricing",
        TRUE ~ "Agnostic"
      ),
      # Discipline uses the complete accounting-journal list. Journal rank is
      # deliberately narrower and continues to use the top-three lists below.
      discipline = case_when(
        Journal %in% globalSettings$acctlistAll ~ "Accounting",
        Journal %in% c("QJE", "JPE") ~ "Economics",
        TRUE ~ "Finance"
      ),
      journal_rank = case_when(
        Journal %in% globalSettings$top3Finance ~ "JF, JFE, RFS",
        Journal %in% globalSettings$top3Accounting ~ "AR, JAR, JAE",
        Journal %in% c("QJE", "JPE") ~ "Economics",
        TRUE ~ "Other"
      )
    )
  list(
    czcat_full = czcat_full,
    theory_mapping = czcat_full %>% select(signalname, theory_group) %>% distinct(),
    model_mapping = czcat_full %>% select(signalname, modeltype_grouped) %>% distinct(),
    discipline_mapping = czcat_full %>% select(signalname, discipline) %>% distinct(),
    journal_mapping = czcat_full %>% select(signalname, journal_rank) %>% distinct(),
    discipline_mapping_filtered = czcat_full %>% select(signalname, discipline) %>% distinct() %>% filter(discipline %in% c("Finance", "Accounting")),
    journal_mapping_filtered = czcat_full %>% select(signalname, journal_rank) %>% distinct() %>% filter(journal_rank != "Economics")
  )
}
