# Assemble and render the reorganized Section 2 Figure 2 (Spec 3, meeting notes
# 2026-07-23) from calculation-owned benchmark contracts. Main versions have
# no confidence intervals; appendix variants shade one clustered SE.
#
# How to run: normally run through S2_ResearchVsDataMining.R from flex-mining/.
# Inputs:  ../Data/Processed/{raw_dm_benchmarks,
#          risk_adjusted_dm_benchmarks}.RDS plus
#          published-signal metadata
# Outputs (../Results/):
#   Fig2a_FactorAdj.pdf            Fig2a_FactorAdj_CI.pdf
#   Fig2b_PubSampleLimits.pdf      Fig2b_PubSampleLimits_CI.pdf
#   Fig2c_MatchedExclCorr.pdf      Fig2c_MatchedExclCorr_CI.pdf
#   Fig2d_AltMining.pdf            Fig2d_AltMining_CI.pdf

rm(list = ls())
pdf(NULL)
source('0_Environment.R')

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
fig2_assemble_long = function(raw_benchmarks, risk_benchmarks, accounting_signals,
                              pre2003_signals) {
  required_raw <- c(
    "published", "accounting_t2", "accounting_top5", "ticker_top5", "matched"
  )
  if (!all(required_raw %in% names(raw_benchmarks))) {
    stop("Raw benchmark contract is missing: ",
         paste(setdiff(required_raw, names(raw_benchmarks)), collapse = ", "))
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

  panel_c_wide <- raw_benchmarks$matched
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

inclSignals = restrictInclSignals(
  restrictType = globalSettings$restrictType,
  topT = globalSettings$topT
)
czcat = fread('DataInput/SignalsTheoryChecked.csv') %>%
  select(signalname, Year) %>%
  filter(signalname %in% inclSignals)

czacct = readRDS('../Data/Processed/czsum_allpredictors.RDS') %>%
  left_join(
    fread('../Data/Raw/SignalDoc.csv') %>%
      transmute(Acronym, Cat.Data, Cat.Form,
                Def = tolower(`Detailed Definition`)),
    by = c('signalname' = 'Acronym')
  ) %>%
  filter(signalname %in% inclSignals, Cat.Data == 'Accounting') %>%
  mutate(
    drop = FALSE,
    drop = if_else(grepl('quarter', Def), TRUE, drop),
    drop = if_else(
      grepl('analyst|meanest|earningssurprise',
            paste(tolower(signalname), Def)), TRUE, drop
    ),
    drop = if_else(Cat.Form == 'discrete', TRUE, drop),
    drop = if_else(signalname %in% c('ShareIss1Y', 'ShareIss5Y'), TRUE, drop)
  )
accounting_signals = czacct %>% filter(!drop) %>% pull(signalname)
pre2003_signals = czcat %>% filter(Year < 2003) %>% pull(signalname)

raw_benchmarks = readRDS('../Data/Processed/raw_dm_benchmarks.RDS')
risk_benchmarks = readRDS('../Data/Processed/risk_adjusted_dm_benchmarks.RDS')

stopifnot(
  raw_benchmarks$metadata$matched$predictor_count ==
    n_distinct(raw_benchmarks$matched$pubname),
  raw_benchmarks$metadata$matched$panel_observation_count ==
    nrow(raw_benchmarks$matched)
)

fig2_long = fig2_assemble_long(
  raw_benchmarks, risk_benchmarks,
  accounting_signals, pre2003_signals
)
fig2_agg = fig2_long %>%
  group_by(panel) %>%
  group_modify(~ fig2_aggregate_series(.x)) %>%
  ungroup()

# Opt-in debug/equivalence artifacts. Normal rendering keeps display data in
# memory, so styling changes do not invalidate Chapter 3 calculations.
data_outdir = Sys.getenv('FIG2_DATA_OUTPUT_DIR', unset = '')
if (nzchar(data_outdir)) {
  dir.create(data_outdir, recursive = TRUE, showWarnings = FALSE)
  saveRDS(fig2_long, file.path(data_outdir, 'fig2_panel_long.RDS'))
  saveRDS(fig2_agg, file.path(data_outdir, 'fig2_panel_agg.RDS'))
}

# An override permits figure validation without touching ../Results.
outdir = Sys.getenv('FIG2_OUTPUT_DIR', unset = '../Results')
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

# Lines intended for direct comparison share a hue and differ by linetype:
# published is solid and its data-mined benchmark is dotted.
colors_paired = c(colors[1], colors[1], colors[2], colors[2])

fontsizeall = 28
linesizeall = 1.5
ylaball = 'Trailing 5-Year Return (bps pm)'
global_xl = -360
global_xh = 300

panels = list(
  a = list(
    series = c('CAPM, Published', 'CAPM, Data-Mined',
               'FF3+Mom, Published', 'FF3+Mom, Data-Mined'),
    colors = colors_paired,
    # Spec-3 styling: each published/data-mined pair shares a hue.
    linetypes = c('solid', 'dotted', 'solid', 'dotted'),
    yaxislab = 'Trailing 5-Year Alpha (bps pm)',
    yl = 0, yh = 125, yh_ci = 150, legendpos = c(35, 20) / 100,
    file = 'Fig2a_FactorAdj'
  ),
  b = list(
    series = c('Pub, Annual Acct Only', 'DM, Annual Acct Pubs',
               'Pub, Pre-2003 Only', 'DM, Pre-2003 Pubs'),
    colors = colors_paired,
    linetypes = c('solid', 'dotted', 'solid', 'dotted'),
    yaxislab = ylaball,
    yl = 0, yh = 175, yh_ci = 200, legendpos = c(35, 20) / 100,
    file = 'Fig2b_PubSampleLimits'
  ),
  c = list(
    series = c('Published', 'Matched on t-stat and mean return',
               'Matched and excluding correlated'),
    colors = colors,
    linetypes = c('solid', 'longdash', 'dashed'),
    yaxislab = ylaball,
    yl = -50, yh = 170, legendpos = c(40, 22) / 100,
    file = 'Fig2c_MatchedExclCorr'
  ),
  d = list(
    series = c('Published', 'Top 5% |t| Mining Accounting',
               'Top 5% |t| Mining Tickers'),
    colors = colors,
    linetypes = c('solid', 'longdash', 'dashed'),
    yaxislab = ylaball,
    yl = -50, yh = 140, yh_ci = 145, legendpos = c(35, 18) / 100,
    file = 'Fig2d_AltMining'
  )
)

for (pk in names(panels)) {
  p = panels[[pk]]
  agg = fig2_agg %>% filter(panel == pk)

  ci_variants = if (is.null(p$ci_variants)) c('none', 'all') else p$ci_variants
  for (civ in ci_variants) {
    # CI ribbons extend above the lines; give them extra headroom where set
    yh_used = if (civ == 'all' && !is.null(p$yh_ci)) p$yh_ci else p$yh
    plt = fig2_overlay_plot(
      agg, series_labels = p$series, colors = p$colors, linetypes = p$linetypes,
      ci = civ, xl = global_xl, xh = global_xh, yl = p$yl, yh = yh_used,
      fontsize = fontsizeall, legendpos = p$legendpos,
      yaxislab = p$yaxislab, linesize = linesizeall
    )
    fname = paste0(outdir, '/', p$file, if (civ == 'all') '_CI' else '', '.pdf')
    ggsave(fname, plt, width = 10, height = 8)
    print(paste('Saved', fname))
  }
}
