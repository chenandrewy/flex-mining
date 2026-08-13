# Grayscale-safe variant of Figure 2 panels (a) and (b): four distinct
# linetypes so the two pub (and two DM) lines stay distinguishable without
# hue. The primary 4c20b versions use Andrew's Spec-3 styling (pub solid,
# DM dashed, pairs separated by hue; docs/journal/260717a,exhibit-reorg.md
# in the writing repo), adopted 2026-08-12. Panels (c)/(d) have one linetype
# per series either way, so only (a)/(b) get variants.
#
# Reuses fig2_panel_agg.RDS, so 4c20a must have run first.
#
# Outputs: ../Results/Fig2/Fig2{a,b}_*_Linetype4{,_CI}.pdf

rm(list = ls())
source('0_Environment.R')
source('helpers/fig2_helpers.R')

fig2_agg = readRDS('../Data/Processed/fig2_panel_agg.RDS')

outdir = '../Results/Fig2'
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

MATPURPLE = rgb(0.4940, 0.1840, 0.5560)
colors4 = c(colors[1], colors[2], MATPURPLE, colors[3])
lines4 = c('solid', 'longdash', 'dotdash', 'dotted')

fontsizeall = 28
linesizeall = 1.5
global_xl = -360
global_xh = 300

panels = list(
  a = list(
    series = c('CAPM, Published', 'CAPM, Data-Mined',
               'FF3+Mom, Published', 'FF3+Mom, Data-Mined'),
    yaxislab = 'Trailing 5-Year Alpha (bps pm)',
    yl = 0, yh = 125, yh_ci = 150, legendpos = c(35, 20) / 100,
    file = 'Fig2a_FactorAdj_Linetype4'
  ),
  b = list(
    series = c('Pub, Annual Acct Only', 'DM, Annual Acct Pubs',
               'Pub, Pre-2003 Only', 'DM, Pre-2003 Pubs'),
    yaxislab = 'Trailing 5-Year Return (bps pm)',
    yl = 0, yh = 175, yh_ci = 200, legendpos = c(35, 20) / 100,
    file = 'Fig2b_PubSampleLimits_Linetype4'
  )
)

for (pk in names(panels)) {
  p = panels[[pk]]
  agg = fig2_agg %>% filter(panel == pk)

  for (civ in c('none', 'all')) {
    yh_used = if (civ == 'all' && !is.null(p$yh_ci)) p$yh_ci else p$yh
    plt = fig2_overlay_plot(
      agg, series_labels = p$series, colors = colors4, linetypes = lines4,
      ci = civ, xl = global_xl, xh = global_xh, yl = p$yl, yh = yh_used,
      fontsize = fontsizeall, legendpos = p$legendpos,
      yaxislab = p$yaxislab, linesize = linesizeall
    )
    fname = paste0(outdir, '/', p$file, if (civ == 'all') '_CI' else '', '.pdf')
    ggsave(fname, plt, width = 10, height = 8)
    print(paste('Saved', fname))
  }
}
