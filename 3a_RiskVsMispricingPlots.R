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
                yl = -90, yh = 180
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
                yl = -90, yh = 180
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
                yl = -90, yh = 180
)

ReturnPlotsNoDM(dt = czret %>% 
                  transmute(eventDate,
                            signalname,
                            ret,
                            catID = theory),
                basepath = '../Results/Extra/Anim-Pub-4',
                suffix = 'AllSignals',
                filetype = '.png',
                yl = -90, yh = 180
)



