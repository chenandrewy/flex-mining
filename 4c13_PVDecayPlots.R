# Event-time decay figure split by PRESENT-VALUE (PV) vs non-PV published signals.
#
# This is the figure analog of 3a_RiskVsMispricingPlots.R (which splits the
# trailing-return decay curve by theory category). Here we split by whether the
# predictor uses present-value logic, following Campbell's (NBER AP 2024)
# distinction, under two specs (pv_narrow, pv_broad).
#
# PV flags come from DataInput/SignalsTheoryChecked_withPV.csv (non-destructive
# merge; original SignalsTheoryChecked.csv untouched).
#
# The plot helper below is a generalized copy of ReturnPlotsNoDM() from
# 0_Environment.R: same trailing-5-year rolling mean, same axes/labels, but with
# an arbitrary 2-level catID instead of the hardcoded risk/mispricing/agnostic.

# Setup -------------------------------------------------------------------
rm(list = ls())
source('0_Environment.R')

# Generalized event-time decay plot (any set of catID levels) -------------
ReturnPlotsNoDM_cat <- function(dt, basepath, suffix = '', rollmonths = 60,
                                filetype = '.pdf', xl = -360, xh = 240,
                                yl = -90, yh = 180, fontsize = 18,
                                legpos = c(78, 88)/100, cat_levels = NULL) {

  #' @param dt Table with columns (signalname, ret, eventDate, catID)

  prepLegend <- dt %>%
    group_by(catID) %>%
    summarise(nSignals = n_distinct(signalname), .groups = 'drop')

  if (is.null(cat_levels)) cat_levels <- sort(unique(dt$catID))
  lab_vec <- vapply(cat_levels, function(cc)
    paste0(cc, ' (', prepLegend$nSignals[prepLegend$catID == cc], ' signals)'),
    character(1))

  # mean return by (catID, eventDate); trailing rolling mean WITHIN catID
  plotme <- dt %>%
    group_by(catID, eventDate) %>%
    summarise(rbar = mean(ret), .groups = 'drop_last') %>%   # stays grouped by catID
    arrange(catID, eventDate) %>%
    mutate(roll_rbar = zoo::rollmean(rbar, k = rollmonths, fill = NA, align = 'right')) %>%
    ungroup() %>%
    mutate(catID = factor(catID, levels = cat_levels, labels = lab_vec))

  catfac <- levels(plotme$catID)

  p <- ggplot(plotme, aes(x = eventDate, y = roll_rbar, color = catID, linetype = catID)) +
    geom_line(size = 1.1) +
    scale_color_manual(values = colors, breaks = catfac) +
    scale_linetype_manual(values = c('solid', 'longdash', 'dashed')[seq_along(catfac)],
                          breaks = catfac) +
    geom_vline(xintercept = 0) +
    coord_cartesian(xlim = c(xl, xh), ylim = c(yl, yh)) +
    scale_y_continuous(breaks = seq(-200, 180, 25)) +
    scale_x_continuous(breaks = seq(-360, 360, 60)) +
    geom_hline(yintercept = 100, color = 'dimgrey') +
    geom_hline(yintercept = 0) +
    ylab('Trailing 5-Year Return (bps p.m.)') +
    xlab('Months Since Original Sample Ended') +
    labs(color = '', linetype = '') +
    theme_light(base_size = fontsize) +
    theme(legend.position = legpos,
          legend.spacing.y = unit(0, units = 'cm'),
          legend.background = element_rect(fill = 'transparent'))

  ggsave(paste0(basepath, '_', suffix, filetype), p, width = 10, height = 8)
  invisible(p)
}

# Load and prep data ------------------------------------------------------
inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType,
                                  topT = globalSettings$topT)

# PV classification (NEW merged file; original untouched). fread types the
# pv_* columns as logical; the coercion below is robust either way.
czpv = fread('DataInput/SignalsTheoryChecked_withPV.csv') %>%
  transmute(signalname,
            pv_narrow = toupper(trimws(as.character(pv_narrow))) == 'TRUE',
            pv_broad  = toupper(trimws(as.character(pv_broad)))  == 'TRUE') %>%
  filter(signalname %in% inclSignals)

czret = readRDS('../Data/Processed/czret_keeponly.RDS') %>%
  left_join(czpv, by = 'signalname') %>%
  mutate(ret = ret / rbar * 100) %>%
  filter(signalname %in% inclSignals) %>%
  filter(!is.na(pv_narrow) & !is.na(pv_broad))

# PV group labels
czret <- czret %>%
  mutate(catNarrow = ifelse(pv_narrow, 'PV', 'non-PV'),
         catBroad  = ifelse(pv_broad,  'PV', 'non-PV'))

cat('Signals: narrow PV =', n_distinct(czret$signalname[czret$pv_narrow]),
    '| broad PV =', n_distinct(czret$signalname[czret$pv_broad]),
    '| total =', n_distinct(czret$signalname), '\n')

# Figures -----------------------------------------------------------------
# NARROW spec: price-scaled valuation ratios vs the rest
ReturnPlotsNoDM_cat(
  dt = czret %>% transmute(eventDate, signalname, ret, catID = catNarrow),
  basepath = '../Results/Fig_PVDecay',
  suffix = 'Narrow',
  cat_levels = c('PV', 'non-PV')
)

# BROAD spec: narrow + QMJ-style profitability/quality vs the rest
ReturnPlotsNoDM_cat(
  dt = czret %>% transmute(eventDate, signalname, ret, catID = catBroad),
  basepath = '../Results/Fig_PVDecay',
  suffix = 'Broad',
  cat_levels = c('PV', 'non-PV')
)

cat('\nDone. Wrote Fig_PVDecay_Narrow.pdf and Fig_PVDecay_Broad.pdf to ../Results/\n')
