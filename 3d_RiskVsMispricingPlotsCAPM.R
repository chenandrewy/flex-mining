# Setup -------------------------------------------------------------------
rm(list = ls())
source('0_Environment.R')
library(roll)     # Model fitting
extract_beta <- function(x, y) {
  model <- lm(y ~ x)
  bet <- coef(model)[2]
  return(bet)
}

czsum = readRDS('../Data/Processed/czsum_allpredictors.RDS')

czcat = fread('DataInput/SignalsTheoryChecked.csv') %>% 
  select(signalname, Year, theory)

czret = readRDS('../Data/Processed/czret_keeponly.RDS') %>%
  left_join(czcat, by = 'signalname') %>%
  mutate(
    retOrig = ret
    , ret = ret/rbar*100
  )

FamaFrenchFactors <- readRDS('../Data/Raw/FamaFrenchFactors.RData') %>%
  rename(date = yearm)

czret <- czret %>% left_join(FamaFrenchFactors, by  = c('date'))

czret[, exr_bp := ret - rf*100]

czret[, beta_all :=  extract_beta(exr_bp, mktrf*100), by = signalname]

czret[, abnormal_all := exr_bp - beta_all*mktrf*100]

czret[, beta_roll :=  coefficients(roll_lm(exr_bp, mktrf*100, width = 60))[, 2], by = signalname]

czret[, abnormal_roll := exr_bp - beta_roll*mktrf*100]


# Create a plot by category without data-mining benchmark
ReturnPlotsNoDMAlpha = function(dt, suffix = '', rollmonths = 60, filetype = '.pdf',
                           xl = -360, xh = 240, yl = -10, yh = 130, 
                           basepath = NA_character_) {
  
  #' @param dt Table with four columns (signalname, ret, eventDate, catID)
  #' @param suffix String to attach to saved pdf figure 
  #' @param rollmonths Number of months over which moving average is computed
  
  # Prep legend
  prepLegend = dt %>% 
    group_by(catID) %>% 
    summarise(nSignals = n_distinct(signalname))
  
  # Plot    
  plotme = dt %>%
    group_by(catID, eventDate) %>% 
    summarise(abar = mean(alpha, na.rm = TRUE)) %>% 
    arrange(catID, eventDate) %>% 
    mutate(
      roll_abar = zoo::rollmean(abar, k = rollmonths, fill = NA, align = 'right')
    ) %>% 
    mutate(catID = factor(catID, levels = c('risk', 'mispricing', 'agnostic'), 
                          labels = c(paste0('Risk (', prepLegend$nSignals[prepLegend$catID == 'risk'], ' signals)'),
                                     paste0('Mispricing (', prepLegend$nSignals[prepLegend$catID == 'mispricing'], ' signals)'), 
                                     paste0('Agnostic (', prepLegend$nSignals[prepLegend$catID == 'agnostic'], ' signals)')))) 
  
  catfac = plotme$catID %>% unique() %>% sort()
  
  print( plotme %>% 
           ggplot(aes(x = eventDate, y = roll_abar, color = catID, linetype = catID)) +
           geom_line(size = 1.1) +
           # scale_color_brewer(palette = 'Dark2') + 
           scale_color_manual(values = colors, breaks = catfac) +
           scale_linetype_manual(values = c('solid','longdash','dashed'), breaks = catfac) +
           geom_vline(xintercept = 0) +
           coord_cartesian(
             xlim = c(xl, xh), ylim = c(yl, yh)
           ) +
           scale_y_continuous(breaks = seq(-200,180,25)) +
           scale_x_continuous(breaks = seq(-360,360,60)) +  
           geom_hline(yintercept = 100, color = 'dimgrey') +
           # annotate(geom="text",
           #          label='In-Sample Mean', x=16, y=95, vjust=-1,
           #          family = "Palatino Linotype", color = 'dimgrey'
           # )  +
           geom_hline(yintercept = 0) +
           ylab('Trailing 5-Year Abnormal Return (bps p.m.)') +
           xlab('Months Since Original Sample Ended') +
           labs(color = '', linetype = '') +
           theme_light(base_size = 18) +
           theme(
             legend.position = c(85,85)/100
             , legend.spacing.y = unit(0, units = 'cm')
             #    , legend.box.background = element_rect(fill='transparent')
             ,legend.background = element_rect(fill='transparent')
           ) 
  )
  
  ggsave(paste0(basepath, '_', suffix, filetype), width = 10, height = 8)
  
}



# Main Figure  ----------------------------------

# All Signals
ReturnPlotsNoDMAlpha(dt = czret %>% 
                  transmute(eventDate,
                            signalname,
                            alpha = abnormal_all,
                            catID = theory),
                basepath = '../Results/Fig_PublicationsOverTime',
                suffix = 'AllSignalsAlphaFullSample',
                yl = -120, yh = 200
)

# All Signals
ReturnPlotsNoDMAlpha(dt = czret %>% 
                       transmute(eventDate,
                                 signalname,
                                 alpha = abnormal_roll,
                                 catID = theory),
                     basepath = '../Results/Fig_PublicationsOverTime',
                     suffix = 'AllSignalsAlphaRollSample',
                     yl = -120, yh = 200
)
