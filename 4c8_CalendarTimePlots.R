# Calendar-time plots following Campbell (2024) discussion suggestion
# Instead of event time (months since sample ended), plot returns in calendar time
# Group anomalies by decade of original-sample end date
# Filter to anomalies with >= 5 years of post-sample data

# Setup --------------------------------------------------------

rm(list = ls())
source("0_Environment.R")

# Load data  -------------------------------------------
# Use pre-computed ret_for_plot0 (from 4c1) which already has
# pubname, eventDate, calendarDate, ret, matchRet, theory
# Only need czsum for sampend to assign decade cohorts

ret_for_plot0 <- readRDS("../Data/Processed/ret_for_plot0.RDS")

czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(Keep)

# Assign decade cohort based on sample end year
# sampend is yearmon class; floor(as.numeric()) extracts year
signal_info <- czsum %>%
  transmute(
    signalname,
    sampend_year = floor(as.numeric(sampend)),
    cohort = paste0(floor(sampend_year / 10) * 10, "s")
  )

# Build calendar-time dataset
cal_data <- ret_for_plot0 %>%
  filter(!is.na(matchRet)) %>%
  left_join(signal_info, by = c("pubname" = "signalname"))

# Campbell filter: require >= 5 years of post-sample data
min_oos_months <- 60

signals_with_enough_oos <- cal_data %>%
  filter(eventDate > 0) %>%
  group_by(pubname) %>%
  summarize(max_oos = max(eventDate)) %>%
  filter(max_oos >= min_oos_months) %>%
  pull(pubname)

cal_data <- cal_data %>%
  filter(pubname %in% signals_with_enough_oos)

# Print cohort summary
cat("\n=== Cohort Summary ===\n")
cal_data %>%
  distinct(pubname, cohort) %>%
  count(cohort) %>%
  print()

# Plot Settings -------------------------------------------

fontsizeall = 28
linesizeall = 1.5
rollmonths = 60
ylaball = 'Trailing 5-Year Return (bps pm)'

# Clustered-SE helper (calendar-time analog of the main paper figure) ---------
# Computes rolling 5-yr mean return per group, plus a clustered SE (calMonth,
# pubname) on non-overlapping `rollmonths`-month windows. Mirrors
# ReturnPlotsWithDM_std_errors_indicators in 0_Environment.R but indexed by
# calMonth instead of eventDate.
compute_rolling_clustered_se <- function(dt_long, rollmonths = 60,
                                         group_col = NULL) {
  if (is.null(group_col)) {
    dt_long$.grp <- "all"
    group_col_use <- ".grp"
  } else {
    group_col_use <- group_col
  }

  groups <- unique(dt_long[[group_col_use]])

  out_list <- lapply(groups, function(g) {
    d <- dt_long %>% filter(.data[[group_col_use]] == g)

    pm <- d %>%
      group_by(calMonth) %>%
      summarize(period_mean = mean(return, na.rm = TRUE), .groups = "drop") %>%
      complete(calMonth = seq.Date(min(calMonth), max(calMonth), by = "month")) %>%
      arrange(calMonth) %>%
      mutate(
        month_idx = row_number() - 1L,
        nonoverlap_window = floor(month_idx / rollmonths),
        roll_rbar = zoo::rollapply(period_mean, width = rollmonths, fill = NA,
                                    align = "right",
                                    FUN = function(x) {
                                      if (sum(!is.na(x)) < 0.8 * length(x)) return(NA_real_)
                                      mean(x, na.rm = TRUE)
                                    })
      )

    wr <- pm %>%
      group_by(nonoverlap_window) %>%
      summarize(win_start = min(calMonth),
                win_end   = max(calMonth),
                .groups   = "drop")

    ses <- wr %>%
      rowwise() %>%
      mutate(se = {
        sub <- d %>% filter(!is.na(return),
                            calMonth >= win_start, calMonth <= win_end)
        if (nrow(sub) == 0 ||
            length(unique(sub$calMonth)) < 2 ||
            length(unique(sub$pubname))  < 2) {
          NA_real_
        } else {
          mod <- lm(return ~ 1, data = sub)
          tryCatch(sqrt(sandwich::vcovCL(mod, cluster = ~calMonth + pubname))[1, 1],
                   error = function(e) NA_real_)
        }
      }) %>%
      ungroup() %>%
      select(nonoverlap_window, se)

    pm %>%
      left_join(ses, by = "nonoverlap_window") %>%
      mutate(.group_val = g,
             upper = roll_rbar + se,
             lower = roll_rbar - se)
  })

  out <- bind_rows(out_list)
  names(out)[names(out) == ".group_val"] <- group_col_use
  if (group_col_use == ".grp") out$.grp <- NULL
  out
}


# Calendar-time plotting function -------------------------------------------

CalendarTimePlot <- function(dt, rollmonths = 60, ret_col = "ret",
                             yl = -75, yh = 200,
                             fontsize = 28, linesize = 1.5,
                             yaxislab = 'Trailing 5-Year Return (bps pm)',
                             title = NULL) {

  # Average returns by cohort and calendar month
  # calendarDate is yearmon; convert to Date for plotting
  plot_dt <- dt %>%
    mutate(calMonth = as.Date(calendarDate)) %>%
    group_by(cohort, calMonth) %>%
    summarize(
      rbar = mean(.data[[ret_col]], na.rm = TRUE),
      n_signals = n_distinct(pubname),
      .groups = "drop"
    ) %>%
    # Fill any gaps in monthly grid so rollmean window = true 5 years
    group_by(cohort) %>%
    complete(calMonth = seq.Date(min(calMonth), max(calMonth), by = "month")) %>%
    arrange(cohort, calMonth) %>%
    mutate(
      # Require at least 48 of 60 months (80%) to have data
      roll_rbar = zoo::rollapply(rbar, width = rollmonths, fill = NA, align = 'right',
                                  FUN = function(x) {
                                    if (sum(!is.na(x)) < 0.8 * length(x)) return(NA_real_)
                                    mean(x, na.rm = TRUE)
                                  })
    ) %>%
    ungroup()

  # Filter to cohorts with enough data for rolling mean
  plot_dt <- plot_dt %>% filter(!is.na(roll_rbar))

  # Add signal count to cohort label
  cohort_counts <- dt %>%
    distinct(pubname, cohort) %>%
    count(cohort)

  plot_dt <- plot_dt %>%
    left_join(cohort_counts, by = "cohort") %>%
    mutate(cohort_label = paste0(cohort, " (N=", n, ")"))

  p <- ggplot(plot_dt, aes(x = calMonth, y = roll_rbar,
                            color = cohort_label)) +
    geom_line(size = linesize) +
    scale_color_brewer(palette = "Set1") +
    geom_hline(yintercept = 100, color = 'dimgrey') +
    geom_hline(yintercept = 0) +
    coord_cartesian(ylim = c(yl, yh)) +
    scale_y_continuous(breaks = seq(-200, 200, 25)) +
    scale_x_date(date_breaks = "5 years", date_labels = "%Y") +
    ylab(yaxislab) +
    xlab('Calendar Date') +
    labs(color = 'Sample End Decade') +
    theme_light(base_size = fontsize) +
    theme(
      legend.position = c(0.25, 0.20),
      legend.spacing.y = unit(0.1, units = 'cm'),
      legend.background = element_rect(fill = 'white', color = 'black', size = 0.3),
      legend.key.width = unit(1.5, units = 'cm'),
      legend.text = element_text(size = fontsize * 0.6),
      legend.title = element_text(size = fontsize * 0.7)
    )

  if (!is.null(title)) {
    p <- p + ggtitle(title)
  }

  return(p)
}

# Drop small cohorts (too noisy to be informative)
min_cohort_size <- 10

cohort_sizes <- cal_data %>%
  distinct(pubname, cohort) %>%
  count(cohort) %>%
  filter(n >= min_cohort_size)

cal_data <- cal_data %>%
  filter(cohort %in% cohort_sizes$cohort)

cat("\nCohorts kept (N >=", min_cohort_size, "):\n")
cohort_sizes %>% print()

# Faceted plotting function (one panel per cohort) -------------------

CalendarTimePlotFacet <- function(dt, rollmonths = 60, ret_col = "ret",
                                   yl = -75, yh = 200,
                                   fontsize = 22, linesize = 1.2,
                                   yaxislab = 'Trailing 5-Year Return (bps pm)',
                                   title = NULL) {

  plot_dt <- dt %>%
    mutate(calMonth = as.Date(calendarDate)) %>%
    group_by(cohort, calMonth) %>%
    summarize(
      rbar = mean(.data[[ret_col]], na.rm = TRUE),
      n_signals = n_distinct(pubname),
      .groups = "drop"
    ) %>%
    group_by(cohort) %>%
    complete(calMonth = seq.Date(min(calMonth), max(calMonth), by = "month")) %>%
    arrange(cohort, calMonth) %>%
    mutate(
      roll_rbar = zoo::rollapply(rbar, width = rollmonths, fill = NA, align = 'right',
                                  FUN = function(x) {
                                    if (sum(!is.na(x)) < 0.8 * length(x)) return(NA_real_)
                                    mean(x, na.rm = TRUE)
                                  })
    ) %>%
    ungroup() %>%
    filter(!is.na(roll_rbar))

  cohort_counts <- dt %>% distinct(pubname, cohort) %>% count(cohort)
  plot_dt <- plot_dt %>%
    left_join(cohort_counts, by = "cohort") %>%
    mutate(cohort_label = paste0(cohort, " (N=", n, ")"))

  p <- ggplot(plot_dt, aes(x = calMonth, y = roll_rbar)) +
    geom_line(linewidth = linesize, color = rgb(0, 0.447, 0.741)) +
    geom_hline(yintercept = 100, color = 'dimgrey') +
    geom_hline(yintercept = 0) +
    facet_wrap(~ cohort_label, ncol = 2, scales = "free_x") +
    coord_cartesian(ylim = c(yl, yh)) +
    scale_y_continuous(breaks = seq(-200, 200, 25)) +
    scale_x_date(date_breaks = "10 years", date_labels = "%Y") +
    ylab(yaxislab) +
    xlab('Calendar Date') +
    theme_light(base_size = fontsize) +
    theme(
      strip.text = element_text(size = fontsize * 0.8, face = "bold"),
      panel.spacing = unit(1, "lines")
    )

  if (!is.null(title)) {
    p <- p + ggtitle(title)
  }

  return(p)
}

# Faded cohorts + bold average plotting function -------------------

CalendarTimePlotFaded <- function(dt, rollmonths = 60, ret_col = "ret",
                                   yl = -75, yh = 200,
                                   fontsize = 28, linesize = 1.5,
                                   yaxislab = 'Trailing 5-Year Return (bps pm)',
                                   title = NULL, fade_alpha = 0.25) {

  # Compute rolling mean per cohort
  cohort_dt <- dt %>%
    mutate(calMonth = as.Date(calendarDate)) %>%
    group_by(cohort, calMonth) %>%
    summarize(rbar = mean(.data[[ret_col]], na.rm = TRUE), .groups = "drop") %>%
    group_by(cohort) %>%
    complete(calMonth = seq.Date(min(calMonth), max(calMonth), by = "month")) %>%
    arrange(cohort, calMonth) %>%
    mutate(
      roll_rbar = zoo::rollapply(rbar, width = rollmonths, fill = NA, align = 'right',
                                  FUN = function(x) {
                                    if (sum(!is.na(x)) < 0.8 * length(x)) return(NA_real_)
                                    mean(x, na.rm = TRUE)
                                  })
    ) %>%
    ungroup() %>%
    filter(!is.na(roll_rbar))

  # Compute rolling mean across ALL signals + clustered SE (calMonth x pubname,
  # non-overlapping windows) for the bold average line
  avg_long <- dt %>%
    mutate(calMonth = as.Date(calendarDate)) %>%
    transmute(calMonth, pubname, return = .data[[ret_col]])

  avg_dt <- compute_rolling_clustered_se(avg_long, rollmonths = rollmonths) %>%
    filter(!is.na(roll_rbar))

  # Add cohort labels with N
  cohort_counts <- dt %>% distinct(pubname, cohort) %>% count(cohort)
  cohort_dt <- cohort_dt %>%
    left_join(cohort_counts, by = "cohort") %>%
    mutate(cohort_label = paste0(cohort, " (N=", n, ")"))

  # Build color scale that includes both cohorts and the average line
  cohort_levels <- sort(unique(cohort_dt$cohort_label))
  n_cohorts <- length(cohort_levels)
  brewer_cols <- RColorBrewer::brewer.pal(max(3, n_cohorts), "Set1")[seq_len(n_cohorts)]
  avg_label <- "Average (all cohorts)"
  color_values <- c(setNames(brewer_cols, cohort_levels), setNames("black", avg_label))

  p <- ggplot() +
    # Faded cohort lines
    geom_line(data = cohort_dt,
              aes(x = calMonth, y = roll_rbar, color = cohort_label),
              alpha = fade_alpha, linewidth = 0.8) +
    # +/- 1 SE band on the average line
    geom_ribbon(data = avg_dt %>% filter(!is.na(se)),
                aes(x = calMonth, ymin = lower, ymax = upper),
                fill = "black", alpha = 0.12, color = NA) +
    # Bold average line (mapped to color so it appears in legend)
    geom_line(data = avg_dt %>% mutate(series = avg_label),
              aes(x = calMonth, y = roll_rbar, color = series),
              linewidth = linesize) +
    scale_color_manual(values = color_values,
                       breaks = c(cohort_levels, avg_label)) +
    geom_hline(yintercept = 100, color = 'dimgrey') +
    geom_hline(yintercept = 0) +
    coord_cartesian(ylim = c(yl, yh)) +
    scale_y_continuous(breaks = seq(-200, 200, 25)) +
    scale_x_date(date_breaks = "5 years", date_labels = "%Y") +
    ylab(yaxislab) +
    xlab('Calendar Date') +
    labs(color = 'Cohort') +
    guides(color = guide_legend(override.aes = list(
      alpha = c(rep(1, n_cohorts), 1),
      linewidth = c(rep(0.8, n_cohorts), linesize)
    ))) +
    theme_light(base_size = fontsize) +
    theme(
      legend.position = c(0.20, 0.22),
      legend.spacing.y = unit(0.1, units = 'cm'),
      legend.background = element_rect(fill = 'white', color = 'black', linewidth = 0.3),
      legend.key.width = unit(1.5, units = 'cm'),
      legend.text = element_text(size = fontsize * 0.55),
      legend.title = element_text(size = fontsize * 0.65)
    )

  if (!is.null(title)) {
    p <- p + ggtitle(title)
  }

  return(p)
}

# Plot 1: Published returns — faded cohorts + bold average -------------------

p1 <- CalendarTimePlotFaded(
  dt = cal_data,
  ret_col = "ret",
  title = "Published Returns by Cohort"
)

ggsave("../Results/Fig_CalendarTime_Published_ByCohort.pdf", p1,
       width = 12, height = 8)

# Plot 1b: DM returns — faded cohorts + bold average -------------------

p1b <- CalendarTimePlotFaded(
  dt = cal_data,
  ret_col = "matchRet",
  title = "Data-Mined Returns by Cohort"
)

ggsave("../Results/Fig_CalendarTime_DM_ByCohort.pdf", p1b,
       width = 12, height = 8)

# Plot 3: Published vs DM, all cohorts pooled, in calendar time -------------------

CalendarTimePlotPubVsDM <- function(dt, rollmonths = 60,
                                     yl = -75, yh = 200,
                                     fontsize = 28, linesize = 1.5,
                                     yaxislab = 'Trailing 5-Year Return (bps pm)',
                                     title = NULL) {

  # Long-form signal-level data with SignalType for clustered SE
  long_dt <- dt %>%
    mutate(calMonth = as.Date(calendarDate)) %>%
    select(calMonth, pubname, ret, matchRet) %>%
    gather(key = "SignalType", value = "return", ret, matchRet)

  # Rolling 5-yr mean + clustered SE per SignalType
  plot_dt <- compute_rolling_clustered_se(long_dt, rollmonths = rollmonths,
                                          group_col = "SignalType") %>%
    filter(!is.na(roll_rbar)) %>%
    mutate(SignalType = factor(SignalType,
                               levels = c("ret", "matchRet"),
                               labels = c("Published (Peer Reviewed)",
                                          "Data-Mined for |t|>2.0")))

  p <- ggplot(plot_dt, aes(x = calMonth, y = roll_rbar,
                            color = SignalType, linetype = SignalType)) +
    # +/- 1 SE band only on the Published series, matching the main paper figure
    geom_ribbon(data = plot_dt %>% filter(SignalType == "Published (Peer Reviewed)"),
                aes(x = calMonth, ymin = lower, ymax = upper),
                fill = colors[1], alpha = 0.1, color = NA,
                inherit.aes = FALSE, show.legend = FALSE) +
    geom_line(size = linesize) +
    scale_color_manual(values = colors[1:2]) +
    scale_linetype_manual(values = c('solid', 'longdash')) +
    geom_hline(yintercept = 100, color = 'dimgrey') +
    geom_hline(yintercept = 0) +
    coord_cartesian(ylim = c(yl, yh)) +
    scale_y_continuous(breaks = seq(-200, 200, 25)) +
    scale_x_date(date_breaks = "5 years", date_labels = "%Y") +
    ylab(yaxislab) +
    xlab('Calendar Date') +
    labs(color = '', linetype = '') +
    theme_light(base_size = fontsize) +
    theme(
      legend.position = c(0.35, 0.15),
      legend.spacing.y = unit(0.1, units = 'cm'),
      legend.background = element_rect(fill = 'white', color = 'black', size = 0.3),
      legend.key.width = unit(1.5, units = 'cm')
    )

  if (!is.null(title)) {
    p <- p + ggtitle(title)
  }

  return(p)
}

p3 <- CalendarTimePlotPubVsDM(
  dt = cal_data,
  title = "Published vs Data-Mined Returns"
)

ggsave("../Results/Fig_CalendarTime_PubVsDM.pdf", p3,
       width = 12, height = 8)

# Plot 4: OOS-only versions (post-sample returns only) -------------------
# These exclude in-sample returns which are high by construction

cal_data_oos <- cal_data %>% filter(eventDate >= 60)

p5 <- CalendarTimePlotFaded(
  dt = cal_data_oos,
  ret_col = "ret",
  title = "Published Returns by Cohort (OOS Only)"
)

ggsave("../Results/Fig_CalendarTime_Published_ByCohort_OOS.pdf", p5,
       width = 12, height = 8)

p6 <- CalendarTimePlotPubVsDM(
  dt = cal_data_oos,
  title = "Published vs Data-Mined (OOS Only)"
)

ggsave("../Results/Fig_CalendarTime_PubVsDM_OOS.pdf", p6,
       width = 12, height = 8)

# Plot 4c: DM returns by cohort — OOS only -------------------

p7 <- CalendarTimePlotFaded(
  dt = cal_data_oos,
  ret_col = "matchRet",
  title = "Data-Mined Returns by Cohort (OOS Only)"
)

ggsave("../Results/Fig_CalendarTime_DM_ByCohort_OOS.pdf", p7,
       width = 12, height = 8)

# Plot 5: Published vs DM by theory category -------------------

for (jj in unique(cal_data$theory)) {
  cat("Plotting PubVsDM theory:", jj, "\n")

  # Wider y-axis for theory subsets (noisier)
  theory_yl <- -75
  theory_yh <- 200

  # Full sample (in-sample + post-sample)
  p <- CalendarTimePlotPubVsDM(
    dt = cal_data %>% filter(theory == jj),
    yl = theory_yl, yh = theory_yh,
    title = paste0(str_to_title(jj), ": Published vs Data-Mined")
  )
  ggsave(paste0("../Results/Fig_CalendarTime_PubVsDM_", jj, ".pdf"), p,
         width = 12, height = 8)

  # OOS only
  p_oos <- CalendarTimePlotPubVsDM(
    dt = cal_data_oos %>% filter(theory == jj),
    yl = theory_yl, yh = theory_yh,
    title = paste0(str_to_title(jj), ": Published vs Data-Mined (OOS)")
  )
  ggsave(paste0("../Results/Fig_CalendarTime_PubVsDM_OOS_", jj, ".pdf"), p_oos,
         width = 12, height = 8)
}

# Summary stats -------------------------------------------

cat("\n=== Calendar Time Summary Stats ===\n")

# Post-sample returns by cohort
cat("\nPost-sample mean returns by cohort:\n")
cal_data %>%
  filter(eventDate > 0) %>%
  group_by(cohort) %>%
  summarize(
    n_signals = n_distinct(pubname),
    mean_pub = mean(ret, na.rm = TRUE),
    mean_dm = mean(matchRet, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  print()

cat("\nDone.\n")
