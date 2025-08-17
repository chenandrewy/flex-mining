# Helper functions for risk-adjusted plots and tables

# Basic extraction helpers -------------------------------------------------
extract_beta <- function(ret, mktrf) {
  if (sum(stats::complete.cases(ret, mktrf)) < 60) return(NA_real_)
  model <- lm(ret ~ mktrf)
  bet <- coef(model)[2]
  return(bet)
}

extract_ff3_coeffs <- function(ret, mktrf, smb, hml) {
  if (sum(stats::complete.cases(ret, mktrf, smb, hml)) < 60) return(c(NA_real_, NA_real_, NA_real_))
  model <- lm(ret ~ mktrf + smb + hml)
  coeffs <- coef(model)
  return(coeffs[2:4])
}

# DM aggregation helpers ---------------------------------------------------
normalize_and_aggregate_dm <- function(dm_data, abnormal_col, suffix_name) {
  dm_normalized <- dm_data %>%
    group_by(actSignal, candSignalname) %>%
    mutate(
      # FIXED: Use sampstart/sampend-based periods for consistency with published signals
      insample_mean = if("sampstart" %in% names(dm_data) && "sampend" %in% names(dm_data)) {
        mean(.data[[abnormal_col]][date >= sampstart & date <= sampend], na.rm = TRUE)
      } else {
        mean(.data[[abnormal_col]][samptype == "insamp"], na.rm = TRUE)
      },
      normalized_ind = ifelse(abs(insample_mean) > 1e-10,
                             100 * .data[[abnormal_col]] / insample_mean,
                             NA)
    ) %>%
    ungroup()
  
  dm_aggregated <- dm_normalized %>%
    group_by(actSignal, eventDate) %>%
    summarise(
      !!sym(paste0("matchRet_", suffix_name)) := mean(normalized_ind, na.rm = TRUE),
      !!sym(paste0("n_matches_", suffix_name)) := n_distinct(candSignalname),
      .groups = 'drop'
    )
  
  return(dm_aggregated)
}

create_filtered_plot_data <- function(ret_for_plot0_adj, signals_list, dm_aggregated,
                                     pub_col, dm_col, suffix_name) {
  plot_data <- ret_for_plot0_adj %>%
    filter(pubname %in% signals_list) %>%
    left_join(
      dm_aggregated,
      by = c("pubname" = "actSignal", "eventDate" = "eventDate")
    ) %>%
    filter(!is.na(!!sym(dm_col)))
  
  return(plot_data)
}

aggregate_dm_no_norm <- function(dm_data, abnormal_col, suffix_name) {
  dm_aggregated <- dm_data %>%
    group_by(actSignal, eventDate) %>%
    summarise(
      !!sym(paste0("matchRet_", suffix_name)) := mean(.data[[abnormal_col]], na.rm = TRUE),
      !!sym(paste0("n_matches_", suffix_name)) := n_distinct(candSignalname),
      .groups = 'drop'
    )
  return(dm_aggregated)
}

# Plotting helper ----------------------------------------------------------
create_risk_adjusted_plot <- function(plot_data, pub_col, dm_col,
                                     adjustment_type, t_threshold,
                                     y_axis_label, y_high = 125,
                                     filter_type = "tstat") {
  
  if(nrow(plot_data) > 0) {
    plot_data %>%
      summarise(
        # FIXED: Use sampstart/sampend-based periods for consistency
        pub_mean_insamp = if("sampstart" %in% names(plot_data) && "sampend" %in% names(plot_data)) {
          mean(.data[[pub_col]][date >= sampstart & date <= sampend], na.rm = TRUE)
        } else {
          mean(.data[[pub_col]][eventDate <= 0], na.rm = TRUE)
        },
        pub_mean_oos = if("sampend" %in% names(plot_data)) {
          mean(.data[[pub_col]][date > sampend], na.rm = TRUE)
        } else {
          mean(.data[[pub_col]][eventDate > 0], na.rm = TRUE)
        },
        dm_mean_insamp = if("sampstart" %in% names(plot_data) && "sampend" %in% names(plot_data)) {
          mean(.data[[dm_col]][date >= sampstart & date <= sampend], na.rm = TRUE)
        } else {
          mean(.data[[dm_col]][eventDate <= 0], na.rm = TRUE)
        },
        dm_mean_oos = if("sampend" %in% names(plot_data)) {
          mean(.data[[dm_col]][date > sampend], na.rm = TRUE)
        } else {
          mean(.data[[dm_col]][eventDate > 0], na.rm = TRUE)
        }
      ) %>% print()
    
    suffix <- paste0(tolower(adjustment_type), "_t", t_threshold)
    
    plot_obj <- ReturnPlotsWithDM_std_errors_indicators(
      dt = plot_data %>%
        transmute(eventDate, pubname, theory,
                 ret = !!sym(pub_col),
                 matchRet = !!sym(dm_col)) %>%
        left_join(czret %>% select(signalname, eventDate, date) %>% distinct(),
                 by = c("pubname" = "signalname", "eventDate" = "eventDate")) %>%
        rename(calendarDate = date),
      basepath = "../Results/temp_",
      suffix = suffix,
      rollmonths = 60,
      colors = colors,
      labelmatch = FALSE,
      yl = 0,
      yh = y_high,
      xl = global_xl,
      xh = global_xh,
      legendlabels = c(
        paste0("Published (", adjustment_type, ", ",
               paste0("t>", t_threshold), ")"),
        paste0("Data-Mined (", adjustment_type, ", ",
               paste0("t>", t_threshold), ")"),
        'N/A'
      ),
      legendpos = c(35,20)/100,
      fontsize = fontsizeall,
      yaxislab = y_axis_label,
      linesize = linesizeall
    )
    
    ggsave(filename = paste0(results_dir, "/Fig_RiskAdj_", suffix, ".pdf"),
           plot_obj, width = 10, height = 8)
    
    temp_file <- paste0("../Results/temp_", suffix, ".pdf")
    if(file.exists(temp_file)) file.remove(temp_file)
    
    return(plot_obj)
  } else {
    cat("No data available for plotting.\n")
    return(NULL)
  }
}

# Summary helpers ----------------------------------------------------------
compute_outperformance <- function(plot_data, ret_col, dm_ret_col, group_map, group_col = "theory_group") {
  plot_data %>%
    left_join(group_map, by = c("pubname" = "signalname")) %>%
    filter(!is.na(.data[[group_col]])) %>%
    group_by(.data[[group_col]]) %>%
    summarise(
      n_signals = n_distinct(pubname),
      # FIXED: Use sampstart/sampend-based periods for consistency
      pub_oos = if("sampend" %in% names(plot_data)) {
        mean(.data[[ret_col]][date > sampend], na.rm = TRUE)
      } else {
        mean(.data[[ret_col]][eventDate > 0], na.rm = TRUE)
      },
      pub_oos_se = {
        if("sampend" %in% names(plot_data)) {
          n <- sum(date > sampend & !is.na(.data[[ret_col]]))
          if (n > 1) sd(.data[[ret_col]][date > sampend], na.rm = TRUE) / sqrt(n) else NA_real_
        } else {
          n <- sum(eventDate > 0 & !is.na(.data[[ret_col]]))
          if (n > 1) sd(.data[[ret_col]][eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
        }
      },
      dm_oos = if("sampend" %in% names(plot_data)) {
        mean(.data[[dm_ret_col]][date > sampend], na.rm = TRUE)
      } else {
        mean(.data[[dm_ret_col]][eventDate > 0], na.rm = TRUE)
      },
      dm_oos_se = {
        if("sampend" %in% names(plot_data)) {
          n <- sum(date > sampend & !is.na(.data[[dm_ret_col]]))
          if (n > 1) sd(.data[[dm_ret_col]][date > sampend], na.rm = TRUE) / sqrt(n) else NA_real_
        } else {
          n <- sum(eventDate > 0 & !is.na(.data[[dm_ret_col]]))
          if (n > 1) sd(.data[[dm_ret_col]][eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
        }
      },
      outperform = pub_oos - dm_oos,
      outperform_se = sqrt(pub_oos_se^2 + dm_oos_se^2),
      .groups = 'drop'
    )
}

create_summary_tables <- function(plot_data_list, group_mappings, table_name = "Analysis",
                                  filter_desc = "", overall_summaries = NULL) {
  results <- list()
  for (group_type in names(group_mappings)) {
    group_map <- group_mappings[[group_type]]
    group_col <- names(group_map)[2]
    results[[group_type]] <- list()
    for (analysis_type in names(plot_data_list)) {
      plot_data <- plot_data_list[[analysis_type]]
      ret_col <- switch(analysis_type,
                       "raw" = "ret",
                       "capm" = "abnormal_capm_normalized",
                       "ff3" = "abnormal_ff3_normalized",
                       "capm_tv" = "abnormal_capm_tv_normalized",
                       "ff3_tv" = "abnormal_ff3_tv_normalized",
                       "ret")
      dm_col <- switch(analysis_type,
                      "raw" = "matchRet",
                      "capm" = "matchRet_capm_t2_normalized",
                      "ff3" = "matchRet_ff3_t2_normalized",
                      "capm_tv" = "matchRet_capm_tv_t2_normalized",
                      "ff3_tv" = "matchRet_ff3_tv_t2_normalized",
                      "matchRet")
      if (!is.null(plot_data) && nrow(plot_data) > 0) {
        results[[group_type]][[analysis_type]] <- compute_outperformance(
          plot_data, ret_col, dm_col, group_map, group_col
        )
      }
    }
  }
  if (!is.null(overall_summaries)) {
    results[["overall"]] <- overall_summaries
  }
  return(results)
}

print_summary_table <- function(summaries, groups, group_col, table_title,
                               analysis_types = c("raw", "capm", "ff3"),
                               analysis_labels = c("Raw", "CAPM", "FF3")) {
  cat("\n", table_title, "\n", sep="")
  cat(strrep("-", nchar(table_title)), "\n")
  cat(sprintf("%-25s", ""))
  for (label in analysis_labels) {
    cat(sprintf("   %s          ", label))
  }
  cat("\n")
  cat(sprintf("%-25s", "Group"))
  for (i in 1:length(analysis_types)) {
    cat("  Post-Samp  Outperf")
  }
  cat("\n")
  get_value <- function(summary, group_col, group, metric) {
    if (is.null(summary) || nrow(summary) == 0) return(NA)
    val <- summary[[metric]][summary[[group_col]] == group]
    if (length(val) == 0) return(NA)
    return(val[1])
  }
  for (group in groups) {
    cat(sprintf("%-25s", group))
    for (analysis in analysis_types) {
      summary <- summaries[[analysis]]
      pub_oos <- get_value(summary, group_col, group, "pub_oos")
      outperform <- get_value(summary, group_col, group, "outperform")
      cat(sprintf("  %8.2f  %8.2f",
                 ifelse(is.na(pub_oos), NA, pub_oos),
                 ifelse(is.na(outperform), NA, outperform)))
    }
    cat("\n")
    cat(sprintf("%-25s", ""))
    for (analysis in analysis_types) {
      summary <- summaries[[analysis]]
      pub_oos_se <- get_value(summary, group_col, group, "pub_oos_se")
      outperform_se <- get_value(summary, group_col, group, "outperform_se")
      cat(sprintf("  (%6.2f)  (%6.2f)",
                 ifelse(is.na(pub_oos_se), NA, pub_oos_se),
                 ifelse(is.na(outperform_se), NA, outperform_se)))
    }
    cat("\n")
  }
}

export_summary_tables <- function(summaries, filename, filter_desc = "") {
  all_results <- list()
  for (category in names(summaries)) {
    if (category == "overall") next
    for (analysis_type in names(summaries[[category]])) {
      summary <- summaries[[category]][[analysis_type]]
      if (!is.null(summary) && nrow(summary) > 0) {
        summary$category <- category
        summary$analysis_type <- analysis_type
        if (filter_desc != "") {
          summary$filter <- filter_desc
        }
        if (length(all_results) == 0) {
          all_results <- summary
        } else {
          all_results <- bind_rows(all_results, summary)
        }
      }
    }
  }
  if (length(all_results) > 0) {
    write.csv(all_results, filename, row.names = FALSE)
    cat("\nSummary tables exported to:", filename, "\n")
  } else {
    cat("\nNo summary data to export.\n")
  }
}

# Additional helpers -------------------------------------------------------
get_values <- function(summary_df, group_col, group_val, value_col) {
  idx <- which(summary_df[[group_col]] == group_val)
  if(length(idx) > 0) return(summary_df[[value_col]][idx]) else return(NA)
}

round_zero <- function(x) {
  rounded <- round(x)
  if (rounded == 0) return(0)
  return(rounded)
}

build_table_row <- function(summaries, group_val, group_col, metrics = c("pub_oos", "pub_oos_se", "outperform", "outperform_se")) {
  row_data <- list()
  for (analysis in names(summaries)) {
    for (metric in metrics) {
      col_name <- paste0(analysis, "_", metric)
      row_data[[col_name]] <- get_values(summaries[[analysis]], group_col, group_val, metric)
    }
  }
  return(row_data)
}

format_value_se <- function(value, se, digits = 0, latex = FALSE) {
  if (is.na(value) || is.na(se)) return(NA)
  value_rounded <- round_zero(value)
  se_rounded <- round_zero(se)
  if (latex) {
    return(sprintf("$%.*f$ ($%.*f$)", digits, value_rounded, digits, se_rounded))
  } else {
    return(sprintf("%.*f (%.*f)", digits, value_rounded, digits, se_rounded))
  }
}

create_latex_table <- function(table_data, caption = "", label = "",
                              column_spec = NULL, booktabs = TRUE,
                              size = "\\small", placement = "htbp") {
  if (!requireNamespace("xtable", quietly = TRUE)) {
    stop("xtable package is required but not installed. Please install it using renv::install('xtable')")
  }
  library(xtable)
  xt <- xtable(table_data, caption = caption, label = label)
  if (is.null(column_spec)) {
    column_spec <- paste0("l", paste(rep("r", ncol(table_data)), collapse = ""))
  }
  xtable::align(xt) <- column_spec
  latex_code <- print(xt,
                     booktabs = booktabs,
                     include.rownames = FALSE,
                     floating.environment = "table",
                     table.placement = placement,
                     size = size,
                     sanitize.text.function = identity,
                     print.results = FALSE)
  return(latex_code)
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
        metric_labels <- c(metric_labels, "Outperformance")
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
  for (analysis in c("capm_tv", "ff3_tv")) {
    analysis_label <- switch(analysis,
                           "capm_tv" = "CAPM_TV",
                           "ff3_tv" = "FF3_TV",
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

build_summary_table <- function(categories, groups, summaries,
                               analysis_types = c("raw", "capm", "ff3"),
                               metrics_config = list(
                                 return = c("pub_oos", "pub_oos_se"),
                                 outperform = c("outperform", "outperform_se")
                               ),
                               format_latex = FALSE, digits = 0) {
  result_df <- data.frame(
    Category = categories,
    Group = groups,
    stringsAsFactors = FALSE
  )
  for (analysis in analysis_types) {
    analysis_label <- switch(analysis,
                           "raw" = "Raw",
                           "capm" = "CAPM",
                           "ff3" = "FF3",
                           analysis)
    if ("return" %in% names(metrics_config)) {
      col_name <- paste0(analysis_label, "_Return")
      result_df[[col_name]] <- mapply(function(grp, cat_data) {
        val <- cat_data[[paste0(analysis, "_", metrics_config$return[1])]]
        se <- cat_data[[paste0(analysis, "_", metrics_config$return[2])]]
        format_value_se(val, se, digits, format_latex)
      }, groups, summaries, SIMPLIFY = TRUE)
    }
  }
  for (analysis in analysis_types) {
    analysis_label <- switch(analysis,
                           "raw" = "Raw",
                           "capm" = "CAPM",
                           "ff3" = "FF3",
                           analysis)
    if ("outperform" %in% names(metrics_config)) {
      col_name <- paste0(analysis_label, "_Outperformance")
      result_df[[col_name]] <- mapply(function(grp, cat_data) {
        val <- cat_data[[paste0(analysis, "_", metrics_config$outperform[1])]]
        se <- cat_data[[paste0(analysis, "_", metrics_config$outperform[2])]]
        format_value_se(val, se, digits, format_latex)
      }, groups, summaries, SIMPLIFY = TRUE)
    }
  }
  return(result_df)
}

export_tables_multi_format <- function(table_data, base_filename,
                                      formats = c("csv", "latex", "txt"),
                                      latex_options = list()) {
  results_files <- list()
  if ("csv" %in% formats) {
    csv_file <- paste0(base_filename, ".csv")
    write.csv(table_data, csv_file, row.names = FALSE)
    results_files$csv <- csv_file
    cat("Exported CSV:", csv_file, "\n")
  }
  if ("latex" %in% formats) {
    latex_file <- paste0(base_filename, ".tex")
    default_opts <- list(
      caption = "",
      label = "",
      group_headers = list(
        list(title = "Post-Sample Return", span = 3),
        list(title = "Outperformance vs Data-Mining", span = 3)
      ),
      placement = "htbp"
    )
    latex_opts <- modifyList(default_opts, latex_options)
    if (!is.null(latex_options$group_headers)) {
      latex_opts$group_headers <- latex_options$group_headers
    }
    latex_code <- do.call(create_formatted_latex_table, c(list(table_data = table_data), latex_opts))
    writeLines(latex_code, latex_file)
    results_files$latex <- latex_file
    cat("Exported LaTeX:", latex_file, "\n")
  }
  if ("txt" %in% formats) {
    txt_file <- paste0(base_filename, ".txt")
    sink(txt_file)
    print(table_data, row.names = FALSE)
    sink()
    results_files$txt <- txt_file
    cat("Exported TXT:", txt_file, "\n")
  }
  return(results_files)
} 

compute_overall_summary <- function(plot_data, ret_col, dm_col) {
  # FIXED: Use sampstart/sampend-based periods for consistency
  # Subset to post-sample observations
  oos_rows <- if("sampend" %in% names(plot_data)) {
    plot_data$date > plot_data$sampend
  } else {
    plot_data$eventDate > 0
  }
  ret_vals <- plot_data[[ret_col]][oos_rows]
  dm_vals  <- plot_data[[dm_col]][oos_rows]
  n_ret <- sum(!is.na(ret_vals))
  n_dm  <- sum(!is.na(dm_vals))
  pub_oos <- mean(ret_vals, na.rm = TRUE)
  dm_oos  <- mean(dm_vals, na.rm = TRUE)
  pub_oos_se <- if (n_ret > 1) sd(ret_vals, na.rm = TRUE) / sqrt(n_ret) else NA_real_
  dm_oos_se  <- if (n_dm > 1) sd(dm_vals,  na.rm = TRUE) / sqrt(n_dm)  else NA_real_
  result <- data.frame(
    group = "Overall",
    n_signals = length(unique(plot_data$pubname)),
    pub_oos = pub_oos,
    pub_oos_se = pub_oos_se,
    dm_oos = dm_oos,
    dm_oos_se = dm_oos_se,
    outperform = NA_real_,
    outperform_se = NA_real_
  )
  result$outperform <- result$pub_oos - result$dm_oos
  result$outperform_se <- sqrt(result$pub_oos_se^2 + result$dm_oos_se^2)
  return(result)
} 

prepare_dm_filters <- function(candidateReturns_adj, czret, filter_type, t_threshold) {
  # Ensure samptype exists and is consistent with sampstart/sampend approach
  if(!"samptype" %in% names(candidateReturns_adj)) {
    candidateReturns_adj <- candidateReturns_adj %>%
      left_join(
        czret %>% select(signalname, eventDate, samptype) %>% distinct(),
        by = c("actSignal" = "signalname", "eventDate" = "eventDate")
      )
  }
  
  # FIXED: Ensure samptype is based on sampstart/sampend for consistency
  if(!"sampstart" %in% names(candidateReturns_adj) || !"sampend" %in% names(candidateReturns_adj)) {
    # If sampstart/sampend not available, use existing samptype
    cat("Warning: sampstart/sampend not available, using existing samptype\n")
  } else {
    # Update samptype to be consistent with sampstart/sampend approach
    candidateReturns_adj <- candidateReturns_adj %>%
      mutate(samptype = ifelse(date >= sampstart & date <= sampend, 'insamp', 'oos'))
  }
  
  setDT(candidateReturns_adj)
  # TV-only stats per DM signal (IS only) - FIXED: Use sampstart/sampend-based IS period
  dm_stats <- candidateReturns_adj[
    (date >= sampstart & date <= sampend) & !is.na(abnormal_capm_tv),
    .(
      abar_capm_tv_dm_t = {
        m <- mean(abnormal_capm_tv, na.rm = TRUE)
        s <- sd(abnormal_capm_tv, na.rm = TRUE)
        n <- sum(!is.na(abnormal_capm_tv))
        if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
      },
      abar_ff3_tv_dm_t = {
        m <- mean(abnormal_ff3_tv, na.rm = TRUE)
        s <- sd(abnormal_ff3_tv, na.rm = TRUE)
        n <- sum(!is.na(abnormal_ff3_tv))
        if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
      },
      abar_capm_tv_dm = mean(abnormal_capm_tv, na.rm = TRUE),
      abar_ff3_tv_dm = mean(abnormal_ff3_tv, na.rm = TRUE)
    ),
    by = .(actSignal, candSignalname)
  ]
  # Only need raw signals list for 4c4
  signals_raw <- unique(czret[rbar_t > t_threshold]$signalname)
  list(
    dm_stats = dm_stats,
    signals_raw = signals_raw
  )
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
      discipline = case_when(
        Journal %in% globalSettings$top3Accounting ~ "Accounting",
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