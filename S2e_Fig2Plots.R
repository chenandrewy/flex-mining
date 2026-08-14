# Render the reorganized Section 2 Figure 2 (Spec 3, meeting notes 2026-07-23) from the
# series built by 3d_Fig2Data.R. Main version has no confidence intervals;
# the appendix variant shades one clustered SE around every series.
#
# How to run: normally run through S2_ResearchVsDataMining.R from flex-mining/.
# Inputs:  ../Data/Processed/fig2_panel_agg.RDS
# Outputs (../Results/Fig2/):
#   Fig2a_FactorAdj.pdf            Fig2a_FactorAdj_CI.pdf
#   Fig2b_PubSampleLimits.pdf      Fig2b_PubSampleLimits_CI.pdf
#   Fig2c_MatchedExclCorr.pdf      Fig2c_MatchedExclCorr_CI.pdf
#   Fig2d_AltMining.pdf            Fig2d_AltMining_CI.pdf

rm(list = ls())
source('0_Environment.R')
source('helpers/fig2_helpers.R')

fig2_agg = readRDS('../Data/Processed/fig2_panel_agg.RDS')

outdir = '../Results/Fig2'
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

  for (civ in c('none', 'all')) {
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
