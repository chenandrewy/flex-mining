# Helpers for the reorganized Figure 2 (Spec 3, meeting notes 2026-07-23)
# Used by 3d_Fig2Data.R / 4c9_Fig2Plots.R.
#
# Generalizes ReturnPlotsWithDM_std_errors_indicators (0_Environment.R) to any
# number of overlaid series by splitting it in two: fig2_aggregate_series()
# turns a long panel of individually-normalized returns into per-series rolling
# means with clustered SEs, and fig2_overlay_plot() renders any subset of those
# series with an optional confidence ribbon on one or all of them.

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
                         filter(label == lab, eventDate > ws, eventDate <= we))
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
