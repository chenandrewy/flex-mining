# Setup -------------------------------------------------------------------
rm(list = ls())
source('0_Environment.R')

# this really only needs PredictorPortsFull.csv and the text TextClassification.csv
# and not this huge matched data.
# clean up eventually please
tmp = readRDS('../Data/Processed/CZ-style-v4 MatchPub.RData')
czret = tmp$czret
czsum = tmp$czsum
rm(tmp)

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



# Plot oos return vs risk to misprice -----------------------------------------------------------------

wordcount = fread('DataIntermediate/TextClassification.csv') %>% 
  select(signalname, theory1, misprice_risk_ratio)

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
    diff_ret = (postsamp)
    , log_risk_misprice = -log(misprice_risk_ratio)
  ) %>% 
  arrange(log_risk_misprice) %>% 
  ungroup() %>% 
  mutate(rank = row_number())

# regression 
reg = lm(diff_ret ~ log_risk_misprice, plotme) %>% 
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

xbase = -0
ybase = -50

ggplot(aes(log_risk_misprice, diff_ret), data = plotme) +
  geom_hline(yintercept = 0, color = 'gray', size = 2) +
  geom_hline(yintercept = 100, color = 'gray', size = 2) +  
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
  labs(y = 'Post-Sample Return (bps)', x = 'Log ([Risk Words]/[Mispricing Words])') +
  annotate('text',x=xbase, y=ybase, label = regstr, size = 8, color = colors[1]) +
  annotate('text',x=xbase, y=ybase - 30, label = sestr, size = 8, color = colors[1])  +
  scale_y_continuous(breaks = seq(-500,500,100)) 
  # ggrepel::geom_text_repel(
  #   aes(label=signalname), max.overlaps = Inf, box.padding = 1.5
  #   , data = plotme %>% filter(rank <= 10), color = 'magenta'
  # ) 
  

ggsave('../Results/Fig_DecayVsWords.pdf', width = 10, height = 8)



# Plot oos return vs risk to misprice -----------------------------------------------------------------

wordcount = fread('DataIntermediate/TextClassification.csv') %>% 
  select(signalname, theory1, misprice_risk_ratio)

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
    diff_ret = (postsamp)
    , log_risk_misprice = -log(misprice_risk_ratio)
  ) %>% 
  arrange(log_risk_misprice) %>% 
  ungroup() %>% 
  mutate(rank = row_number())

# regression 
reg = lm(diff_ret ~ log_risk_misprice, plotme) %>% 
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

xbase = 1
ybase = 300
repelsize = 6
repelcolor = 'royalblue4'

ggplot(aes(log_risk_misprice, diff_ret), data = plotme) +
  geom_hline(yintercept = 0, color = 'gray', size = 2) +
  geom_hline(yintercept = 100, color = 'gray', size = 2) +  
  geom_abline(
    aes(intercept = reg$coefficients[1], slope = reg$coefficients[2])
    , color = colors[2], size = 2
  ) +
  geom_point(size = 2.5) +
  theme_light(base_size = 26) +
  theme(
    legend.position = c(80,85)/100
    , legend.spacing.y = unit(0, units = 'cm')
    , legend.background = element_rect(fill='transparent')
  ) +
  labs(y = 'Post-Sample Return (bps)', x = 'Log ([Risk Words]/[Mispricing Words])') +
  # annotate('text',x=xbase, y=ybase, label = regstr, size = 8, color = colors[1]) +
  # annotate('text',x=xbase, y=ybase - 30, label = sestr, size = 8, color = colors[1])  +
  scale_y_continuous(breaks = seq(-500,500,100))  +
  ggrepel::geom_text_repel(
    aes(label=signalname), max.overlaps = Inf, box.padding = 1.5
    , data = plotme %>% filter(rank <= 12), color = repelcolor, size = repelsize
    , xlim = c(-3, -2.1)
  ) +
  ggrepel::geom_text_repel(
    aes(label=signalname), max.overlaps = Inf, box.padding = 0.5
    , data = plotme %>% filter(rank >= 202-15), color = repelcolor, size = repelsize
  )

ggsave('../Results/Fig_DecayVsWords_Names.pdf', width = 10, height = 8)



# Plot oos return vs risk to misprice -v2 ----------------------------------------------------------------

wordcount = fread('DataIntermediate/TextClassification.csv') %>% 
  select(signalname, theory1, misprice_risk_ratio)

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
    diff_ret = (postsamp)/insamp
    , log_risk_misprice = -log(misprice_risk_ratio)
  ) %>% 
  arrange(log_risk_misprice) %>% 
  ungroup() %>% 
  mutate(rank = row_number())

# regression 
reg = lm(diff_ret ~ log_risk_misprice, plotme) %>% 
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

xbase = 1
ybase = 300
repelsize = 6
repelcolor = 'royalblue4'
  
ggplot(aes(log_risk_misprice, diff_ret), data = plotme) +
  geom_hline(yintercept = 0, color = 'gray', size = 1) +
  geom_hline(yintercept = 1, color = 'gray', size = 1) +  
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
    # , plot.margin = margin(1, 1, 1, 1, "cm")    
  ) +
  labs(y = expression('[Post-Sample] / [In-Sample]')
       , x = 'Log ([Risk Words]/[Mispricing Words])') +
  # annotate('text',x=xbase, y=ybase, label = regstr, size = 8, color = colors[1]) +
  # annotate('text',x=xbase, y=ybase - 30, label = sestr, size = 8, color = colors[1])  +
  scale_y_continuous(breaks = seq(-5,5,1))  +
  scale_x_continuous(breaks = seq(-4,6,2))  +
  ggrepel::geom_text_repel(
    aes(label=signalname), max.overlaps = Inf
    , box.padding = 1.5
    , data = plotme %>% filter(rank <= 15), color = repelcolor, size = repelsize
    , xlim = c(-4, -1.5)
  ) +
  ggrepel::geom_text_repel(
    aes(label=signalname), max.overlaps = Inf, box.padding = 0.5
    , data = plotme %>% filter(rank >= 202-15), color = repelcolor, size = repelsize
  ) +
  coord_cartesian(xlim = c(-4,4))

ggsave('../Results/Fig_DecayVsWords_Names2.pdf', width = 10, height = 8)


