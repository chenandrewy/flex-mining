# Setup -------------------------------------------------------------------
rm(list = ls())
source('0_Environment.R')

czsum = readRDS('../Data/Processed/czsum_allpredictors.RDS')

czcat = fread('DataInput/SignalsTheoryChecked.csv') %>% 
  select(signalname, Year, theory)

czret = readRDS('../Data/Processed/czret_keeponly.RDS') %>% 
  left_join(czcat, by = 'signalname') %>% 
  mutate(
    retOrig = ret
    , ret = ret/rbar*100
  )

# Main Figure  ----------------------------------

# All Signals
ReturnPlotsNoDM(dt = czret %>% 
                  transmute(eventDate,
                            signalname,
                            ret,
                            catID = theory),
                basepath = '../Results/Fig_PublicationsOverTime',
                suffix = 'AllSignals',
                yl = -90, yh = 180
)

# Post-2000 samp ends only ------------------------------------------------

temp = czret %>% 
  filter(Year > 2004) %>% 
  transmute(eventDate,
            signalname,
            ret,
            catID = theory)  

temp %>% distinct(signalname)

# All Signals
ReturnPlotsNoDM(dt = temp,
                basepath = '../Results/Fig_PublicationsOverTime',
                suffix = 'PubPost2004',
                yl = -120, yh = 200
)

# Animations for Slides ---------------------------------------------------
# also: larger font size
fontsizeall = 28
legposall = c(30,20)/100

ReturnPlotsNoDM(dt = czret %>% 
                  mutate(
                    ret = NA_real_
                  ) %>% 
                  transmute(eventDate,
                            signalname,
                            ret,
                            catID = theory),
                basepath = '../Results/Extra/Anim-Pub-1',
                suffix = 'AllSignals',
                filetype = '.png',
                yl = -90, yh = 180,
                fontsize = fontsizeall,
                legpos = legposall
)

ReturnPlotsNoDM(dt = czret %>% 
                  mutate(
                    ret = if_else(theory != 'mispricing', NA_real_, ret)
                  ) %>% 
                  transmute(eventDate,
                            signalname,
                            ret,
                            catID = theory),
                basepath = '../Results/Extra/Anim-Pub-2',
                suffix = 'AllSignals',
                filetype = '.png',
                yl = -90, yh = 180,
                fontsize = fontsizeall,
                legpos = legposall
)

ReturnPlotsNoDM(dt = czret %>% 
                  mutate(
                    ret = if_else(theory == 'risk', NA_real_, ret)
                  ) %>% 
                  transmute(eventDate,
                            signalname,
                            ret,
                            catID = theory),
                basepath = '../Results/Extra/Anim-Pub-3',
                suffix = 'AllSignals',
                filetype = '.png',
                yl = -90, yh = 180,
                fontsize = fontsizeall,
                legpos = legposall
)

ReturnPlotsNoDM(dt = czret %>% 
                  transmute(eventDate,
                            signalname,
                            ret,
                            catID = theory),
                basepath = '../Results/Extra/Anim-Pub-4',
                suffix = 'AllSignals',
                filetype = '.png',
                yl = -90, yh = 180,
                fontsize = fontsizeall,
                legpos = legposall
)



# CAPM versions of plots --------------------------------------------------


## Load FF factors and join ------------------------------------------------
FamaFrenchFactors <- readRDS('../Data/Raw/FamaFrenchFactors.RData') %>%
  rename(date = yearm)

czret <- czret %>% 
  left_join(FamaFrenchFactors, by  = c('date'))


## Full-sample betas -------------------------------------------------------
czret[, beta_all :=  extract_beta(ret, mktrf*100), by = signalname]

czret[, abnormal_all := ret - beta_all*mktrf*100]

czret[samptype == 'insamp', abar_all := mean(abnormal_all, na.rm = TRUE), by = signalname]

czret[, abar_all := nafill(abar_all, "locf"), by = .(signalname)]

czret[, abnormal_all_normalized := 100*abnormal_all/abar_all]

## Rolling betas -----------------------------------------------------------
czret[, beta_roll :=  coefficients(roll_lm(ret, mktrf*100, width = 60))[, 2], by = signalname]

czret[, abnormal_roll := ret - beta_roll*mktrf*100]

czret[samptype == 'insamp', abar_roll := mean(abnormal_roll, na.rm = TRUE), by = signalname]

czret[, abar_roll := nafill(abar_roll, "locf"), by = .(signalname)]

czret[, abnormal_roll_normalized := 100*abnormal_roll/abar_roll]


## OP in-sample version ----------------------------------------------------

czret[, insamp := (samptype == 'insamp')]

czret[, beta_all_not_norm :=  extract_beta(retOrig, mktrf), by = .(signalname, insamp)]

czret[, abnormal_all_not_norm := retOrig - beta_all_not_norm*mktrf]

czret[samptype == 'insamp', abar_all_not_norm := mean(abnormal_all_not_norm, na.rm = TRUE), by = signalname]

czret[samptype == 'insamp', abar_all_not_norm_t := t.test(abnormal_all_not_norm, na.rm = TRUE)$statistic, by = signalname]

czret[, abar_all_not_norm := nafill(abar_all_not_norm, "locf"), by = .(signalname)]

czret[, abar_all_not_norm_t := nafill(abar_all_not_norm_t, "locf"), by = .(signalname)]

czret[, abnormal_all_normalized_v2 := 100*abnormal_all_not_norm/abar_all_not_norm]

czret[, mean(abnormal_all, na.rm = TRUE), by = samptype]
czret[, mean(abnormal_roll, na.rm = TRUE), by = samptype]
czret[, mean(abnormal_all_normalized, na.rm = TRUE), by = samptype]
czret[, mean(abnormal_roll_normalized, na.rm = TRUE), by = samptype]
czret[, mean(abnormal_all_normalized_v2, na.rm = TRUE), by = samptype]
czret[, mean(abar_all_not_norm_t, na.rm = TRUE), by = samptype]


## Plots -------------------------------------------------------------------

# All Signals
ReturnPlotsNoDMAlpha(dt = czret %>% 
                       transmute(eventDate,
                                 signalname,
                                 alpha = abnormal_all,
                                 catID = theory),
                     basepath = '../Results/Fig_PublicationsOverTime',
                     suffix = 'AllSignalsAlphaFullSampleNormalizedRet',
                     yl = -120, yh = 200
)

# All Signals
ReturnPlotsNoDMAlpha(dt = czret %>% 
                       transmute(eventDate,
                                 signalname,
                                 alpha = abnormal_all_normalized,
                                 catID = theory),
                     basepath = '../Results/Fig_PublicationsOverTime',
                     suffix = 'AllSignalsAlphaFullSampleNormalizedRetThenAbnormal',
                     yl = -120, yh = 200
)

# All Signals
ReturnPlotsNoDMAlpha(dt = czret %>% 
                       transmute(eventDate,
                                 signalname,
                                 alpha = abnormal_roll,
                                 catID = theory),
                     basepath = '../Results/Fig_PublicationsOverTime',
                     suffix = 'AllSignalsAlphaRollNormalizedRetThenAbnormal',
                     yl = -120, yh = 200
)

# All Signals
ReturnPlotsNoDMAlpha(dt = czret %>% 
                       transmute(eventDate,
                                 signalname,
                                 alpha = abnormal_roll,
                                 catID = theory),
                     basepath = '../Results/Fig_PublicationsOverTime',
                     suffix = 'AllSignalsAlphaRollNormalizedRetThenAbnormal',
                     yl = -120, yh = 200
)



# All Signals
ReturnPlotsNoDMAlpha(dt = czret[abar_all_not_norm_t >= 1, ] %>%
                       transmute(eventDate,
                                 signalname,
                                 alpha = abnormal_all_normalized_v2,
                                 catID = theory),
                     basepath = '../Results/Fig_PublicationsOverTime',
                     suffix = 'AllSignalsAlphaFullSampleNormalizedAbnormalTge1',
                     yl = -120, yh = 300
)

# All Signals
ReturnPlotsNoDMAlpha(dt = czret[abar_all_not_norm_t >= 2, ] %>%
                       transmute(eventDate,
                                 signalname,
                                 alpha = abnormal_all_normalized_v2,
                                 catID = theory),
                     basepath = '../Results/Fig_PublicationsOverTime',
                     suffix = 'AllSignalsAlphaFullSampleNormalizedAbnormalTge2',
                     yl = -120, yh = 300
)

# All Signals
ReturnPlotsNoDMAlpha(dt = czret[abar_all_not_norm_t >= 3, ] %>%
                       transmute(eventDate,
                                 signalname,
                                 alpha = abnormal_all_normalized_v2,
                                 catID = theory),
                     basepath = '../Results/Fig_PublicationsOverTime',
                     suffix = 'AllSignalsAlphaFullSampleNormalizedAbnormalTge3',
                     yl = -120, yh = 300
)


# Risk using factor models ------------------------------------------------


## Load and process data ---------------------------------------------------

czcat = fread('DataInput/SignalsTheoryChecked.csv') %>% 
  select(signalname, Year, theory)

czret = readRDS('../Data/Processed/czret_keeponly.RDS') %>% 
  left_join(czcat, by = 'signalname') %>% 
  mutate(
    retOrig = ret
    , ret = ret/rbar*100
  )

czret <- czret %>% left_join(FamaFrenchFactors, by  = c('date'))

## Compute factor adjustments ----------------------------------------------
czret %>% setDT()

czret[, risk_via_mkt :=  mkt_implied_category(.SD), by = signalname]
czret[, risk_via_ff3 :=  ff3_implied_category(.SD), by = signalname]
czret[, risk_via_ff5 :=  ff5_implied_category(.SD), by = signalname]

add_catID(czret, "risk_via_mkt", 3)
add_catID(czret, "risk_via_ff3", 3)
add_catID(czret, "risk_via_ff5", 3)

average_by_ntile <- czret[eventDate >0 ,
                          .(ret = mean(ret)/100,
                            catID_risk_via_mkt = first(catID_risk_via_mkt),
                            catID_risk_via_ff3 = first(catID_risk_via_ff3),
                            catID_risk_via_ff5 = first(catID_risk_via_ff5),
                            risk_via_mkt = first(risk_via_mkt),
                            risk_via_ff3 = first(risk_via_ff3),
                            risk_via_ff5 = first(risk_via_ff5)
                          ), by = signalname]


## Plot results ------------------------------------------------------------
ylab <- "[Post-Sample]/[In-Sample]"
xlab <- "[Predicted by Risk Model]/[In-Sample]"

# Create the individual plots
xax <- c(-.5, 1)
repelsize = 6
repelcolor = 'royalblue4'

# CAPM
reg_capm = lm(ret ~ risk_via_mkt, average_by_ntile) %>% 
  summary()

p1 <- ggplot(average_by_ntile, aes(x=risk_via_mkt, y=ret)) +
  geom_hline(yintercept = 0, color = 'gray', size =2) +
  geom_vline(xintercept =  0, color = 'gray', size = 1) +
  geom_hline(yintercept = 1, color = 'gray', size = 2) +
  geom_point(size = 2.5) +
  theme_light(base_size = 26) +
  theme(
    legend.position = c(80,85)/100
    , legend.spacing.y = unit(0, units = 'cm')
    , legend.background = element_rect(fill='transparent')) +
  labs(
    x=xlab,
    y=ylab) +
  xlim(xax) + ylim(c(-1, 2))    +
  geom_abline(
    aes(intercept = reg_capm$coefficients[1], slope = reg_capm$coefficients[2])
    , color = colors[1], size = 2
  ) 

p1

ggsave('../Results/Fig_Risk_via_CAPM.pdf', p1, width = 10, height = 8)


# FF3
reg_ff3 = lm(ret ~ risk_via_ff3, average_by_ntile) %>% 
  summary()

p2 <- ggplot(average_by_ntile, aes(x=risk_via_ff3, y=ret)) +
  geom_hline(yintercept = 0, color = 'gray', size = 2) +
  geom_vline(xintercept =  0, color = 'gray', size = 1) +
  geom_hline(yintercept = 1, color = 'gray', size = 2) +
  geom_point(size = 2.5) +
  theme_light(base_size = 26) +
  theme(
    legend.position = c(80,85)/100
    , legend.spacing.y = unit(0, units = 'cm')
    , legend.background = element_rect(fill='transparent')) +
  labs(
    x=xlab,
    y=ylab) +
  xlim(xax) + ylim(c(-1, 2))    +
  geom_abline(
    aes(intercept = reg_ff3$coefficients[1], slope = reg_ff3$coefficients[2])
    , color = colors[1], size = 2
  ) 

p2

ggsave('../Results/Fig_Risk_via_FF3.pdf', p2, width = 10, height = 8)

# FF5
reg_ff5 = lm(ret ~ risk_via_ff5, average_by_ntile) %>% 
  summary()

p3 <- ggplot(average_by_ntile, aes(x=risk_via_ff5, y=ret)) +
  geom_hline(yintercept = 0, color = 'gray', size = 2) +
  geom_vline(xintercept =  0, color = 'gray', size = 1) +
  geom_hline(yintercept = 1, color = 'gray', size = 2) +
  geom_point(size = 2.5) +
  theme_light(base_size = 26) +
  theme(
    legend.position = c(80,85)/100
    , legend.spacing.y = unit(0, units = 'cm')
    , legend.background = element_rect(fill='transparent')) +
  labs(
    x=xlab,
    y=ylab) +
  xlim(xax) + ylim(c(-1, 2))    +
  geom_abline(
    aes(intercept = reg_ff5$coefficients[1], slope = reg_ff5$coefficients[2])
    , color = colors[1], size = 2
  ) 

p3

ggsave('../Results/Fig_Risk_via_FF5.pdf', p3, width = 10, height = 8)

