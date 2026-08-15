# Helpers for the reorganized Figure 2 (Spec 3, meeting notes 2026-07-23)
# Used by S2e_Fig2Plots.R and Figure 2 contract tests.
#
# Generalizes ReturnPlotsWithDM_std_errors_indicators (0_Environment.R) to any
# number of overlaid series by splitting it in two: fig2_aggregate_series()
# turns a long panel of individually-normalized returns into per-series rolling
# means with clustered SEs, and fig2_overlay_plot() renders any subset of those
# series with an optional confidence ribbon on one or all of them.

# Compose the four Figure 2 panels from calculation-owned benchmark contracts.
# Signal classifications are presentation-sample choices and therefore enter
# here rather than being stored in a Chapter 3 benchmark artifact.
fig2_assemble_long = function(raw_benchmarks, matched_benchmark,
                              risk_benchmarks, accounting_signals,
                              pre2003_signals) {
  required_raw <- c("published", "accounting_t2", "accounting_top5", "ticker_top5")
  if (!all(required_raw %in% names(raw_benchmarks))) {
    stop("Raw benchmark contract is missing: ",
         paste(setdiff(required_raw, names(raw_benchmarks)), collapse = ", "))
  }
  if (is.null(matched_benchmark$panel)) {
    stop("Matched benchmark contract has no panel.")
  }

  published <- raw_benchmarks$published %>%
    select(pubname, eventDate, calendarDate, published_return = return)

  panel_b_pair <- function(signals, published_label, dm_label) {
    common <- published %>%
      filter(pubname %in% signals) %>%
      inner_join(
        raw_benchmarks$accounting_t2 %>%
          select(pubname, eventDate, dm_return = return) %>%
          filter(!is.na(dm_return)),
        by = c("pubname", "eventDate")
      )
    bind_rows(
      common %>% transmute(
        label = published_label, pubname, eventDate, calendarDate,
        return = published_return
      ),
      common %>% transmute(
        label = dm_label, pubname, eventDate, calendarDate,
        return = dm_return
      )
    )
  }

  panel_b <- bind_rows(
    panel_b_pair(accounting_signals,
                 "Pub, Annual Acct Only", "DM, Annual Acct Pubs"),
    panel_b_pair(pre2003_signals,
                 "Pub, Pre-2003 Only", "DM, Pre-2003 Pubs")
  ) %>% mutate(panel = "b")

  panel_c_wide <- matched_benchmark$panel
  panel_c <- bind_rows(
    panel_c_wide %>% transmute(
      label = "Published", pubname, eventDate, calendarDate,
      return = published_ret_scaled
    ),
    panel_c_wide %>% transmute(
      label = "Matched on t-stat and mean return", pubname, eventDate,
      calendarDate, return = matched_ret_scaled
    ),
    panel_c_wide %>% transmute(
      label = "Matched and excluding correlated", pubname, eventDate,
      calendarDate, return = matched_uncorr_ret_scaled
    )
  ) %>% mutate(panel = "c")

  panel_d_wide <- published %>%
    inner_join(
      raw_benchmarks$accounting_top5 %>%
        select(pubname, eventDate, accounting_return = return) %>%
        filter(!is.na(accounting_return)),
      by = c("pubname", "eventDate")
    ) %>%
    inner_join(
      raw_benchmarks$ticker_top5 %>%
        select(pubname, eventDate, ticker_return = return) %>%
        filter(!is.na(ticker_return)),
      by = c("pubname", "eventDate")
    )
  panel_d <- bind_rows(
    panel_d_wide %>% transmute(
      label = "Published", pubname, eventDate, calendarDate,
      return = published_return
    ),
    panel_d_wide %>% transmute(
      label = "Top 5% |t| Mining Accounting", pubname, eventDate,
      calendarDate, return = accounting_return
    ),
    panel_d_wide %>% transmute(
      label = "Top 5% |t| Mining Tickers", pubname, eventDate,
      calendarDate, return = ticker_return
    )
  ) %>% mutate(panel = "d")

  panel_a_model <- function(model_key, published_label, dm_label) {
    model_panel <- risk_benchmarks[[model_key]]$panel
    bind_rows(
      model_panel %>% transmute(
        label = published_label, pubname, eventDate, calendarDate,
        return = published_return
      ),
      model_panel %>% transmute(
        label = dm_label, pubname, eventDate, calendarDate,
        return = dm_return
      )
    )
  }
  panel_a <- bind_rows(
    panel_a_model("capm", "CAPM, Published", "CAPM, Data-Mined"),
    panel_a_model("ff4", "FF3+Mom, Published", "FF3+Mom, Data-Mined")
  ) %>% mutate(panel = "a")

  bind_rows(panel_b, panel_d, panel_c, panel_a) %>%
    select(label, pubname, eventDate, calendarDate, return, panel)
}

# dt_long columns: label, pubname, eventDate, calendarDate, return.
# Returns one row per (label, eventDate): roll_rbar plus the double-clustered
# (calendarDate + pubname) SE of the enclosing non-overlapping window, as in
# ReturnPlotsWithDM_std_errors_indicators.
fig2_aggregate_series = function(dt_long, rollmonths = 60) {

  get_clustered_se = function(data) {
    if (nrow(data) == 0) return(NA_real_)
    if (length(unique(data$calendarDate)) < 2) return(NA_real_)
    if (length(unique(data$pubname)) < 2) return(NA_real_)
    mod = lm(return ~ 1, data = data)
    se = tryCatch({
      sqrt(sandwich::vcovCL(mod, cluster = ~calendarDate + pubname))[1, 1]
    }, error = function(e) {
      warning('Error in vcovCL: ', e$message)
      NA_real_
    })
    if (is.nan(se)) NA_real_ else se
  }

  dt_long = dt_long %>% filter(!is.na(return))

  period_means = dt_long %>%
    group_by(label, eventDate) %>%
    summarise(period_mean = mean(return, na.rm = TRUE), .groups = 'drop') %>%
    group_by(label) %>%
    arrange(eventDate, .by_group = TRUE) %>%
    mutate(
      roll_rbar = zoo::rollmean(period_mean, k = rollmonths, fill = NA, align = 'right'),
      nonoverlap_window = floor(eventDate / rollmonths)
    ) %>%
    ungroup()

  windows = period_means %>%
    group_by(label, nonoverlap_window) %>%
    summarise(window_end = max(eventDate), .groups = 'drop') %>%
    mutate(window_start = window_end - rollmonths + 1)

  windows$se = mapply(
    function(lab, ws, we) {
      get_clustered_se(dt_long %>%
                         filter(label == lab, eventDate >= ws, eventDate <= we))
    },
    windows$label, windows$window_start, windows$window_end
  )

  period_means %>%
    left_join(windows %>% select(label, nonoverlap_window, se),
              by = c('label', 'nonoverlap_window')) %>%
    mutate(upper = roll_rbar + se, lower = roll_rbar - se) %>%
    select(label, eventDate, roll_rbar, se, upper, lower)
}

# agg: output of fig2_aggregate_series (possibly several panels' worth).
# series_labels: labels to plot, in legend order. ci: which series get ribbons.
fig2_overlay_plot = function(agg, series_labels, colors, linetypes,
                             ci = c('none', 'first', 'all'),
                             xl = -360, xh = 300, yl = -10, yh = 130,
                             fontsize = 28, legendpos = c(30, 15) / 100,
                             yaxislab = 'Trailing 5-Year Return (bps pm)',
                             linesize = 1.5, ribbon_alpha = 0.10) {
  ci = match.arg(ci)

  agg = agg %>%
    filter(label %in% series_labels) %>%
    mutate(label = factor(label, levels = series_labels))

  ribbons = switch(ci,
    none  = agg[0, ],
    first = agg %>% filter(label == series_labels[1]),
    all   = agg
  )

  ggplot(agg, aes(x = eventDate, y = roll_rbar, color = label, linetype = label)) +
    geom_ribbon(data = ribbons, aes(ymin = lower, ymax = upper, fill = label),
                alpha = ribbon_alpha, color = NA) +
    geom_line(linewidth = linesize) +
    scale_color_manual(values = colors) +
    scale_fill_manual(values = colors) +
    scale_linetype_manual(values = linetypes) +
    geom_vline(xintercept = 0) +
    geom_hline(yintercept = c(0, 100), color = c('black', 'dimgrey')) +
    coord_cartesian(xlim = c(xl, xh), ylim = c(yl, yh)) +
    scale_y_continuous(breaks = seq(-200, 180, 25)) +
    scale_x_continuous(breaks = seq(-360, 360, 60)) +
    labs(x = 'Months Since Original Sample Ended', y = yaxislab,
         color = '', linetype = '', fill = '') +
    theme_light(base_size = fontsize) +
    theme(
      legend.position = legendpos,
      legend.spacing.y = unit(0.1, units = 'cm'),
      legend.background = element_rect(fill = 'transparent'),
      legend.key.width = unit(1.5, units = 'cm')
    ) +
    guides(fill = 'none')
}
