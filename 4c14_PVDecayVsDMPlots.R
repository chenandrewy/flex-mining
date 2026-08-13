# Main-spec decay figure (Published vs Matched Data-Mined) restricted to
# PRESENT-VALUE (PV) vs non-PV published signals.
#
# This reuses ReturnPlotsWithDM() from 0_Environment.R -- the exact helper that
# draws the paper's main Figure 1 (published vs data-mined) -- but applied
# separately to the PV and non-PV subsets, under both specs (pv_narrow, pv_broad).
# It answers Campbell's question in the paper's own format: do PV published
# signals beat their data-mined benchmark differently than non-PV ones?
#
# PV flags come from DataInput/SignalsTheoryChecked_withPV.csv (non-destructive
# merge; original SignalsTheoryChecked.csv untouched). Aesthetic settings mirror
# 4c2_ResearchVsDMPlots.R's main "t_min_2" figure.

# Setup -------------------------------------------------------------------
rm(list = ls())
source('0_Environment.R')

inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType,
                                  topT = globalSettings$topT)

# PV classification (NEW merged file; original untouched)
czpv = fread('DataInput/SignalsTheoryChecked_withPV.csv') %>%
  transmute(signalname,
            pv_narrow = toupper(trimws(as.character(pv_narrow))) == 'TRUE',
            pv_broad  = toupper(trimws(as.character(pv_broad)))  == 'TRUE') %>%
  filter(signalname %in% inclSignals)

# Main-spec input, with PV flags joined on pubname (= signalname)
ret_for_plot0 = readRDS('../Data/Processed/ret_for_plot0.RDS') %>%
  left_join(czpv %>% rename(pubname = signalname), by = 'pubname') %>%
  filter(!is.na(matchRet), !is.na(pv_narrow), !is.na(pv_broad))

# Aesthetics copied from 4c2_ResearchVsDMPlots.R main figure ("t_min_2")
fontsizeall = 28
ylaball     = 'Trailing 5-Year Return (bps pm)'
linesizeall = 1.5
global_xl   = -360
global_xh   = 300
leglabels   = c("Published (and Peer Reviewed)",
                "Data-Mined for |t|>2.0 in Original Sample",
                "N/A")

# Wrapper: main-spec published-vs-DM figure for one PV subset, with a title -----
pv_dm_figure <- function(dt_sub, outfile, title) {
  tmp_base <- '../Results/temp_PVDM'
  tmp_suffix <- gsub('[^A-Za-z0-9]', '', title)

  p <- ReturnPlotsWithDM(
    dt = dt_sub,
    basepath = tmp_base,
    suffix = tmp_suffix,
    rollmonths = 60,
    colors = colors,
    labelmatch = FALSE,
    yl = 0, yh = 125,
    xl = global_xl, xh = global_xh,
    legendlabels = leglabels,
    legendpos = c(35, 20)/100,
    fontsize = fontsizeall,
    yaxislab = ylaball,
    linesize = linesizeall
  )

  # add a panel title and save the final figure
  (p + labs(title = title) +
      theme(legend.background = element_rect(fill = "white", color = "black", size = 0.3),
            legend.margin = margin(-1.0, 0.5, 0.5, 0.5, "cm"),
            legend.position = c(44, 18)/100,
            legend.spacing.y = unit(0.2, "cm"),
            plot.title = element_text(size = fontsizeall * 0.7)) +
      guides(color = guide_legend(byrow = TRUE))) %>%
    ggsave(filename = outfile, width = 10, height = 8)

  file.remove(paste0(tmp_base, '_', tmp_suffix, '.pdf'))
  invisible(NULL)
}

# Figures: 2 specs x {PV, non-PV} -----------------------------------------
specs <- list(
  list(flag = 'pv_narrow', label = 'Narrow'),
  list(flag = 'pv_broad',  label = 'Broad')
)

for (sp in specs) {
  pv_sub    <- ret_for_plot0 %>% filter(.data[[sp$flag]])
  nonpv_sub <- ret_for_plot0 %>% filter(!.data[[sp$flag]])
  n_pv    <- n_distinct(pv_sub$pubname)
  n_nonpv <- n_distinct(nonpv_sub$pubname)

  cat(sprintf('[%s] PV signals: %d | non-PV signals: %d\n', sp$label, n_pv, n_nonpv))

  pv_dm_figure(pv_sub,
               sprintf('../Results/Fig_PVDecayDM_%sPV.pdf', sp$label),
               sprintf('Present-value signals (%s spec, %d signals)', sp$label, n_pv))
  pv_dm_figure(nonpv_sub,
               sprintf('../Results/Fig_PVDecayDM_%sNonPV.pdf', sp$label),
               sprintf('Non-present-value signals (%s spec, %d signals)', sp$label, n_nonpv))
}

cat('\nDone. Wrote Fig_PVDecayDM_{Narrow,Broad}{PV,NonPV}.pdf to ../Results/\n')
