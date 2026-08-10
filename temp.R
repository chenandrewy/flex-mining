
# Function to add risk adjustments to ret_for_plot data
add_risk_adjustments <- function(plot_data, czret_data, dm_risk_adj) {
  plot_data %>%
    left_join(
      czret_data %>% select(signalname, eventDate, abnormal_capm_normalized, abnormal_ff3_normalized,
                           rbar_scaled_t, abar_capm_t, abar_ff3_t),
      by = c("pubname" = "signalname", "eventDate" = "eventDate")
    ) %>%
    left_join(
      dm_risk_adj %>% select(actSignal, eventDate, matchRet_capm_normalized, matchRet_ff3_normalized),
      by = c("pubname" = "actSignal", "eventDate" = "eventDate")
    )
}

ret_for_plot0_adj <- add_risk_adjustments(ret_for_plot0, czret, matched_risk_adj)
ret_for_plot1_adj <- add_risk_adjustments(ret_for_plot1, czret, matched_risk_adj)


# Compute Risk Adjustments ------------------------------------------------
# Add samptype to matched_risk_adj for normalization
matched_risk_adj <- matched_risk_adj %>%
  left_join(
    czret %>% select(signalname, eventDate, samptype) %>% distinct(),
    by = c("actSignal" = "signalname", "eventDate" = "eventDate")
  )

# Compute in-sample means, t-stats, and normalize
matched_risk_adj <- matched_risk_adj %>%
  group_by(actSignal) %>%
  mutate(
    # Compute in-sample means and t-stats
    abar_capm_dm = mean(matchRet_capm[samptype == "insamp"], na.rm = TRUE),
    abar_ff3_dm = mean(matchRet_ff3[samptype == "insamp"], na.rm = TRUE),
    abar_capm_dm_t = mean(matchRet_capm[samptype == "insamp"], na.rm = TRUE) / 
                     sd(matchRet_capm[samptype == "insamp"], na.rm = TRUE) * 
                     sqrt(sum(samptype == "insamp" & !is.na(matchRet_capm))),
    abar_ff3_dm_t = mean(matchRet_ff3[samptype == "insamp"], na.rm = TRUE) / 
                    sd(matchRet_ff3[samptype == "insamp"], na.rm = TRUE) * 
                    sqrt(sum(samptype == "insamp" & !is.na(matchRet_ff3))),
    # Forward fill the in-sample means
    abar_capm_dm = ifelse(is.na(abar_capm_dm), NA, abar_capm_dm),
    abar_ff3_dm = ifelse(is.na(abar_ff3_dm), NA, abar_ff3_dm),
    # Normalize
    matchRet_capm_normalized = 100 * matchRet_capm / abar_capm_dm,
    matchRet_ff3_normalized = 100 * matchRet_ff3 / abar_ff3_dm
  ) %>%
  ungroup()

# Add risk adjustments to plotting data ----------------------------------



## 2. CAPM-Adjusted Returns ----------------------------------------------
tempsuffix = "capm_adjusted"

# Create CAPM-adjusted version of plotting data
ret_for_plot0_capm <- ret_for_plot0_adj %>%
  filter(!is.na(abnormal_capm_normalized), !is.na(matchRet_capm_normalized))

printme_capm = ReturnPlotsWithDM_std_errors_indicators(
  dt = ret_for_plot0_capm %>% 
    transmute(eventDate, pubname, theory, ret = abnormal_capm_normalized, matchRet = matchRet_capm_normalized) %>%
    left_join(czret %>% select(signalname, eventDate, date) %>% distinct(), by = c("pubname" = "signalname", "eventDate" = "eventDate")) %>%
    rename(calendarDate = date),
  basepath = "../Results/temp_",
  suffix = tempsuffix,
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  xl = global_xl,
  xh = global_xh,
  legendlabels =
    c(
      paste0("Published (CAPM Alpha)"),
      paste0("Data-Mined for |t|>2.0 (CAPM Alpha)"),
      'N/A'
    ),
  legendpos = c(35,20)/100,
  fontsize = fontsizeall,
  yaxislab = "Trailing 5-Year CAPM Alpha (% of In-Sample Alpha)",
  linesize = linesizeall
)

ggsave(filename = paste0(results_dir, "/Fig_RiskAdj_", tempsuffix, '.pdf'), 
       printme_capm, width = 10, height = 8)

# Print summary statistics
cat("\n=== CAPM NORMALIZED ALPHA PLOT STATISTICS ===\n")
ret_for_plot0_capm %>% 
  summarise(
    pub_mean_insamp = mean(abnormal_capm_normalized[eventDate <= 0], na.rm = TRUE),
    pub_mean_oos = mean(abnormal_capm_normalized[eventDate > 0], na.rm = TRUE),
    dm_mean_insamp = mean(matchRet_capm_normalized[eventDate <= 0], na.rm = TRUE),
    dm_mean_oos = mean(matchRet_capm_normalized[eventDate > 0], na.rm = TRUE)
  ) %>% print()

cat("\nNote: Values are % of in-sample alpha (100 = same as in-sample average)\n")

## 3. FF3-Adjusted Returns -----------------------------------------------
tempsuffix = "ff3_adjusted"

# Create FF3-adjusted version of plotting data  
ret_for_plot0_ff3 <- ret_for_plot0_adj %>%
  filter(!is.na(abnormal_ff3_normalized), !is.na(matchRet_ff3_normalized))

printme_ff3 = ReturnPlotsWithDM_std_errors_indicators(
  dt = ret_for_plot0_ff3 %>% 
    transmute(eventDate, pubname, theory, ret = abnormal_ff3_normalized, matchRet = matchRet_ff3_normalized) %>%
    left_join(czret %>% select(signalname, eventDate, date) %>% distinct(), by = c("pubname" = "signalname", "eventDate" = "eventDate")) %>%
    rename(calendarDate = date),
  basepath = "../Results/temp_",
  suffix = tempsuffix,
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  xl = global_xl,
  xh = global_xh,
  legendlabels =
    c(
      paste0("Published (FF3 Alpha)"),
      paste0("Data-Mined for |t|>2.0 (FF3 Alpha)"),
      'N/A'
    ),
  legendpos = c(35,20)/100,
  fontsize = fontsizeall,
  yaxislab = "Trailing 5-Year FF3 Alpha (% of In-Sample Alpha)",
  linesize = linesizeall
)

ggsave(filename = paste0(results_dir, "/Fig_RiskAdj_", tempsuffix, '.pdf'), 
       printme_ff3, width = 10, height = 8)

# Print summary statistics
cat("\n=== FF3 NORMALIZED ALPHA PLOT STATISTICS ===\n")
ret_for_plot0_ff3 %>% 
  summarise(
    pub_mean_insamp = mean(abnormal_ff3_normalized[eventDate <= 0], na.rm = TRUE),
    pub_mean_oos = mean(abnormal_ff3_normalized[eventDate > 0], na.rm = TRUE),
    dm_mean_insamp = mean(matchRet_ff3_normalized[eventDate <= 0], na.rm = TRUE),
    dm_mean_oos = mean(matchRet_ff3_normalized[eventDate > 0], na.rm = TRUE)
  ) %>% print()

cat("\nNote: Values are % of in-sample alpha (100 = same as in-sample average)\n")

# Clean up temp files
file.remove(paste0("../Results/temp__", "raw_returns", ".pdf"))
file.remove(paste0("../Results/temp__", "capm_adjusted", ".pdf"))  
file.remove(paste0("../Results/temp__", "ff3_adjusted", ".pdf"))

print("Risk-adjusted plots created successfully!")
print(paste("Files saved in", results_dir, ":"))
print("- Fig_RiskAdj_raw_returns.pdf")
print("- Fig_RiskAdj_capm_adjusted.pdf") 
print("- Fig_RiskAdj_ff3_adjusted.pdf")


# Raw returns (basis points) - by theory
raw_summary_theory <- compute_outperformance(
  ret_for_plot0 %>% filter(!is.na(matchRet)), 
  "ret", "matchRet", theory_mapping, "theory_group"
)

# CAPM adjusted (% of in-sample alpha) - by theory
capm_summary_theory <- compute_outperformance(
  ret_for_plot0_adj %>% filter(!is.na(abnormal_capm_normalized), !is.na(matchRet_capm_normalized)),
  "abnormal_capm_normalized", "matchRet_capm_normalized", theory_mapping, "theory_group"
)

# FF3 adjusted (% of in-sample alpha) - by theory
ff3_summary_theory <- compute_outperformance(
  ret_for_plot0_adj %>% filter(!is.na(abnormal_ff3_normalized), !is.na(matchRet_ff3_normalized)),
  "abnormal_ff3_normalized", "matchRet_ff3_normalized", theory_mapping, "theory_group"
)

# Raw returns - by model
raw_summary_model <- compute_outperformance(
  ret_for_plot0 %>% filter(!is.na(matchRet)), 
  "ret", "matchRet", model_mapping, "modeltype_grouped"
)

# CAPM adjusted - by model
capm_summary_model <- compute_outperformance(
  ret_for_plot0_adj %>% filter(!is.na(abnormal_capm_normalized), !is.na(matchRet_capm_normalized)),
  "abnormal_capm_normalized", "matchRet_capm_normalized", model_mapping, "modeltype_grouped"
)

# FF3 adjusted - by model
ff3_summary_model <- compute_outperformance(
  ret_for_plot0_adj %>% filter(!is.na(abnormal_ff3_normalized), !is.na(matchRet_ff3_normalized)),
  "abnormal_ff3_normalized", "matchRet_ff3_normalized", model_mapping, "modeltype_grouped"
)

# Overall summaries
overall_summary_raw <- data.frame(
  group = "Overall",
  n_signals = length(unique(ret_for_plot0$pubname[!is.na(ret_for_plot0$matchRet)])),
  pub_oos = mean(ret_for_plot0$ret[ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet)], na.rm = TRUE),
  pub_oos_se = sd(ret_for_plot0$ret[ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet)], na.rm = TRUE) / 
               sqrt(sum(ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet))),
  dm_oos = mean(ret_for_plot0$matchRet[ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet)], na.rm = TRUE),
  dm_oos_se = sd(ret_for_plot0$matchRet[ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet)], na.rm = TRUE) / 
              sqrt(sum(ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet))),
  outperform = NA,
  outperform_se = NA
)
overall_summary_raw$outperform <- overall_summary_raw$pub_oos - overall_summary_raw$dm_oos
overall_summary_raw$outperform_se <- sqrt(overall_summary_raw$pub_oos_se^2 + overall_summary_raw$dm_oos_se^2)

overall_summary_capm <- data.frame(
  group = "Overall",
  n_signals = length(unique(ret_for_plot0_adj$pubname[!is.na(ret_for_plot0_adj$abnormal_capm_normalized) & !is.na(ret_for_plot0_adj$matchRet_capm_normalized)])),
  pub_oos = mean(ret_for_plot0_adj$abnormal_capm_normalized[ret_for_plot0_adj$eventDate > 0 & !is.na(ret_for_plot0_adj$abnormal_capm_normalized) & !is.na(ret_for_plot0_adj$matchRet_capm_normalized)], na.rm = TRUE),
  pub_oos_se = sd(ret_for_plot0_adj$abnormal_capm_normalized[ret_for_plot0_adj$eventDate > 0 & !is.na(ret_for_plot0_adj$abnormal_capm_normalized) & !is.na(ret_for_plot0_adj$matchRet_capm_normalized)], na.rm = TRUE) / 
               sqrt(sum(ret_for_plot0_adj$eventDate > 0 & !is.na(ret_for_plot0_adj$abnormal_capm_normalized) & !is.na(ret_for_plot0_adj$matchRet_capm_normalized))),
  dm_oos = mean(ret_for_plot0_adj$matchRet_capm_normalized[ret_for_plot0_adj$eventDate > 0 & !is.na(ret_for_plot0_adj$abnormal_capm_normalized) & !is.na(ret_for_plot0_adj$matchRet_capm_normalized)], na.rm = TRUE),
  dm_oos_se = sd(ret_for_plot0_adj$matchRet_capm_normalized[ret_for_plot0_adj$eventDate > 0 & !is.na(ret_for_plot0_adj$abnormal_capm_normalized) & !is.na(ret_for_plot0_adj$matchRet_capm_normalized)], na.rm = TRUE) / 
              sqrt(sum(ret_for_plot0_adj$eventDate > 0 & !is.na(ret_for_plot0_adj$abnormal_capm_normalized) & !is.na(ret_for_plot0_adj$matchRet_capm_normalized))),
  outperform = NA,
  outperform_se = NA
)
overall_summary_capm$outperform <- overall_summary_capm$pub_oos - overall_summary_capm$dm_oos
overall_summary_capm$outperform_se <- sqrt(overall_summary_capm$pub_oos_se^2 + overall_summary_capm$dm_oos_se^2)

overall_summary_ff3 <- data.frame(
  group = "Overall",
  n_signals = length(unique(ret_for_plot0_adj$pubname[!is.na(ret_for_plot0_adj$abnormal_ff3_normalized) & !is.na(ret_for_plot0_adj$matchRet_ff3_normalized)])),
  pub_oos = mean(ret_for_plot0_adj$abnormal_ff3_normalized[ret_for_plot0_adj$eventDate > 0 & !is.na(ret_for_plot0_adj$abnormal_ff3_normalized) & !is.na(ret_for_plot0_adj$matchRet_ff3_normalized)], na.rm = TRUE),
  pub_oos_se = sd(ret_for_plot0_adj$abnormal_ff3_normalized[ret_for_plot0_adj$eventDate > 0 & !is.na(ret_for_plot0_adj$abnormal_ff3_normalized) & !is.na(ret_for_plot0_adj$matchRet_ff3_normalized)], na.rm = TRUE) / 
               sqrt(sum(ret_for_plot0_adj$eventDate > 0 & !is.na(ret_for_plot0_adj$abnormal_ff3_normalized) & !is.na(ret_for_plot0_adj$matchRet_ff3_normalized))),
  dm_oos = mean(ret_for_plot0_adj$matchRet_ff3_normalized[ret_for_plot0_adj$eventDate > 0 & !is.na(ret_for_plot0_adj$abnormal_ff3_normalized) & !is.na(ret_for_plot0_adj$matchRet_ff3_normalized)], na.rm = TRUE),
  dm_oos_se = sd(ret_for_plot0_adj$matchRet_ff3_normalized[ret_for_plot0_adj$eventDate > 0 & !is.na(ret_for_plot0_adj$abnormal_ff3_normalized) & !is.na(ret_for_plot0_adj$matchRet_ff3_normalized)], na.rm = TRUE) / 
              sqrt(sum(ret_for_plot0_adj$eventDate > 0 & !is.na(ret_for_plot0_adj$abnormal_ff3_normalized) & !is.na(ret_for_plot0_adj$matchRet_ff3_normalized))),
  outperform = NA,
  outperform_se = NA
)
overall_summary_ff3$outperform <- overall_summary_ff3$pub_oos - overall_summary_ff3$dm_oos
overall_summary_ff3$outperform_se <- sqrt(overall_summary_ff3$pub_oos_se^2 + overall_summary_ff3$dm_oos_se^2)

# Helper function to get values by group
get_values <- function(summary_df, group_col, group_val, value_col) {
  idx <- which(summary_df[[group_col]] == group_val)
  if(length(idx) > 0) return(summary_df[[value_col]][idx]) else return(NA)
}

# Print formatted table
cat("\nPost-Sample Return            Outperformance vs Data-Mining\n")
cat("                Raw    CAPM    FF3    Raw    CAPM    FF3\n")
cat("Theoretical Foundation\n")

groups_theory <- c("Risk", "Mispricing", "Agnostic") 
for(group in groups_theory) {
  # Post-sample returns
  raw_ret <- round(get_values(raw_summary_theory, "theory_group", group, "pub_oos"))
  capm_ret <- round(get_values(capm_summary_theory, "theory_group", group, "pub_oos"))  
  ff3_ret <- round(get_values(ff3_summary_theory, "theory_group", group, "pub_oos"))
  
  # Outperformance
  raw_out <- round(get_values(raw_summary_theory, "theory_group", group, "outperform"))
  capm_out <- round(get_values(capm_summary_theory, "theory_group", group, "outperform"))
  ff3_out <- round(get_values(ff3_summary_theory, "theory_group", group, "outperform"))
  
  # Standard errors  
  raw_se <- round(get_values(raw_summary_theory, "theory_group", group, "pub_oos_se"))
  capm_se <- round(get_values(capm_summary_theory, "theory_group", group, "pub_oos_se"))
  ff3_se <- round(get_values(ff3_summary_theory, "theory_group", group, "pub_oos_se"))
  
  raw_out_se <- round(get_values(raw_summary_theory, "theory_group", group, "outperform_se"))
  capm_out_se <- round(get_values(capm_summary_theory, "theory_group", group, "outperform_se"))
  ff3_out_se <- round(get_values(ff3_summary_theory, "theory_group", group, "outperform_se"))
  
  # Sample size
  n_sigs <- get_values(raw_summary_theory, "theory_group", group, "n_signals")
  
  cat(sprintf("%-12s %4s   %4s   %4s   %4s   %4s   %4s\n",
              group, raw_ret, capm_ret, ff3_ret, raw_out, capm_out, ff3_out))
  cat(sprintf("%-12s (%2s)   (%2s)   (%2s)   (%2s)   (%2s)   (%2s)\n",
              "", raw_se, capm_se, ff3_se, raw_out_se, capm_out_se, ff3_out_se))
}

cat("Modeling Formalism\n")
groups_model <- c("No Model", "Stylized", "Dynamic or Quantitative")
for(group in groups_model) {
  # Post-sample returns
  raw_ret <- round(get_values(raw_summary_model, "modeltype_grouped", group, "pub_oos"))
  capm_ret <- round(get_values(capm_summary_model, "modeltype_grouped", group, "pub_oos"))  
  ff3_ret <- round(get_values(ff3_summary_model, "modeltype_grouped", group, "pub_oos"))
  
  # Outperformance
  raw_out <- round(get_values(raw_summary_model, "modeltype_grouped", group, "outperform"))
  capm_out <- round(get_values(capm_summary_model, "modeltype_grouped", group, "outperform"))
  ff3_out <- round(get_values(ff3_summary_model, "modeltype_grouped", group, "outperform"))
  
  # Standard errors  
  raw_se <- round(get_values(raw_summary_model, "modeltype_grouped", group, "pub_oos_se"))
  capm_se <- round(get_values(capm_summary_model, "modeltype_grouped", group, "pub_oos_se"))
  ff3_se <- round(get_values(ff3_summary_model, "modeltype_grouped", group, "pub_oos_se"))
  
  raw_out_se <- round(get_values(raw_summary_model, "modeltype_grouped", group, "outperform_se"))
  capm_out_se <- round(get_values(capm_summary_model, "modeltype_grouped", group, "outperform_se"))
  ff3_out_se <- round(get_values(ff3_summary_model, "modeltype_grouped", group, "outperform_se"))
  
  # Sample size  
  n_sigs <- get_values(raw_summary_model, "modeltype_grouped", group, "n_signals")
  
  cat(sprintf("%-12s %4s   %4s   %4s   %4s   %4s   %4s\n",
              group, raw_ret, capm_ret, ff3_ret, raw_out, capm_out, ff3_out))
  cat(sprintf("%-12s (%2s)   (%2s)   (%2s)   (%2s)   (%2s)   (%2s)\n",
              "", raw_se, capm_se, ff3_se, raw_out_se, capm_out_se, ff3_out_se))
}

cat("Overall\n")
cat(sprintf("%-12s %4s   %4s   %4s   %4s   %4s   %4s\n",
            "All", round(overall_summary_raw$pub_oos), round(overall_summary_capm$pub_oos), round(overall_summary_ff3$pub_oos), 
            round(overall_summary_raw$outperform), round(overall_summary_capm$outperform), round(overall_summary_ff3$outperform)))
cat(sprintf("%-12s (%2s)   (%2s)   (%2s)   (%2s)   (%2s)   (%2s)\n",
            "", round(overall_summary_raw$pub_oos_se), round(overall_summary_capm$pub_oos_se), round(overall_summary_ff3$pub_oos_se), 
            round(overall_summary_raw$outperform_se), round(overall_summary_capm$outperform_se), round(overall_summary_ff3$outperform_se)))