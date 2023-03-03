# Setup -------------------------------------------------------------------
rm(list = ls())
source('0_Environment.R')

# this really only needs PredictorPortsFull.csv and the text TextClassification.csv
# and not this huge matched data.
# clean up eventually please
tmp = readRDS('../Data/Processed/CZ-style-v4 MatchPub.RData')
czret = tmp$czret
czsum = tmp$czsum
rm(temp)

czdoc = fread('../Data/Raw/SignalDoc.csv') %>% select(1:20) %>% 
  rename(signalname = Acronym)


# Main Figure  ----------------------------------


# All Signals
ReturnPlotsNoDM(dt = czret %>% 
                  transmute(eventDate,
                            signalname,
                            ret,
                            catID = theory1),
                basepath = '../Results/Fig_PublicationsOverTime',
                suffix = 'AllSignals'
)



# Post-2000 samp ends only ------------------------------------------------

temp = czret %>% 
  transmute(eventDate,
            signalname,
            ret,
            catID = theory1) %>% 
  inner_join(
    czdoc %>% filter(Year > 2004) %>% select(signalname)
  )

temp %>% distinct(signalname)

# All Signals
ReturnPlotsNoDM(dt = temp,
                basepath = '../Results/Fig_PublicationsOverTime',
                suffix = 'PubPost2004'
)




# Animations for Slides ---------------------------------------------------




ReturnPlotsNoDM(dt = czret %>% 
                  mutate(
                    ret = NA_real_
                  ) %>% 
                  transmute(eventDate,
                            signalname,
                            ret,
                            catID = theory1),
                basepath = '../Results/Anim-Pub-1',
                suffix = 'AllSignals',
                filetype = '.png'
)


ReturnPlotsNoDM(dt = czret %>% 
                  mutate(
                    ret = if_else(theory1 == 'risk', NA_real_, ret)
                  ) %>% 
                  transmute(eventDate,
                            signalname,
                            ret,
                            catID = theory1),
                basepath = '../Results/Anim-Pub-2',
                suffix = 'AllSignals',
                filetype = '.png'
)


ReturnPlotsNoDM(dt = czret %>% 
                  transmute(eventDate,
                            signalname,
                            ret,
                            catID = theory1),
                basepath = '../Results/Anim-Pub-3',
                suffix = 'AllSignals',
                filetype = '.png'
)




# test: plot decay vs risk to misprice -----------------------------------------------------------------

wordcount = fread('DataIntermediate/TextClassification.csv') %>% 
  select(signalname, theory1, misprice_risk_ratio)

# check oos def
czret %>% 
  group_by(signalname, samptype) %>% 
  summarize(nmonth = n()) %>% 
  group_by(samptype) %>% 
  summarize(mean(nmonth))

# calc decay
czdecay = czret %>% 
  mutate(
    samptype2 = samptype
    , samptype2 = if_else(samptype %in% c('oos','postpub'), 'postsamp', samptype2)
  ) %>% 
  group_by(signalname, samptype2) %>% 
  summarize(rbar = mean(ret)) %>% 
  pivot_wider(names_from = samptype2, values_from = rbar)

# data to plot
plotme = czdecay  %>% 
  left_join(wordcount)  %>% 
  mutate(
    decay_pct = (insamp-postsamp)/insamp * 100
    , log_risk_misprice = -log(misprice_risk_ratio)
  ) 

# regression 
reg = lm(decay_pct ~ log_risk_misprice, plotme) %>% 
  summary()

regstr = paste0(
  'y = ', round(reg$coefficients[1], 2)
  , ' + ', format(round(reg$coefficients[2], 2), nsmall = 2)
  , ' x'
)

sestr = paste0(
  '       (', round(reg$coefficients[1,2], 2), ')'
  , '  (', format(round(reg$coefficients[2,2], 2), nsmall = 2), ')'
  , '  '
)

xbase = 0.25
ybase = 200

ggplot(aes(log_risk_misprice, decay_pct), data = plotme) +
  geom_hline(yintercept = 0, color = 'gray', size = 2) +
  geom_abline(
    aes(intercept = reg$coefficients[1], slope = reg$coefficients[2])
    , color = colors[1], size = 2
  ) +
  geom_point(size = 2.5) +
  theme_light(base_size = 26) +
  theme(
    legend.position = c(80,85)/100
    , legend.spacing.y = unit(0, units = 'cm')
    , legend.background = element_rect(fill='transparent')
  ) +
  labs(y = 'Post-Sample Decay (%)', x = 'Log ([Risk Words]/[Mispricing Words])') +
  annotate('text',x=xbase, y=ybase, label = regstr, size = 8, color = colors[1]) +
  annotate('text',x=xbase, y=ybase - 30, label = sestr, size = 8, color = colors[1]) +
  coord_cartesian(ylim = c(-200,200))

ggsave('../Results/Fig_DecayVsWords.pdf', width = 10, height = 8)

