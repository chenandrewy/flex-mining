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
source('0_Environment.R')
source('helpers/fig2_helpers.R')

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
