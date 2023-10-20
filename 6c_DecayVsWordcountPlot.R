
# Plot oos return vs risk to misprice -----------------------------------------------------------------

wordcount = fread('DataIntermediate/TextClassification.csv') %>% 
  select(signalname, theory, misprice_risk_ratio)

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


# Plot oos vs words w/ signal names ----------------------------------------------------------------

wordcount = fread('DataIntermediate/TextClassification.csv') %>% 
  select(signalname, theory, misprice_risk_ratio)

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
    , data = plotme %>% filter(rank >= nrow(plotme)-15), color = repelcolor, size = repelsize
  ) +
  coord_cartesian(xlim = c(-4,4))

ggsave('../Results/Fig_DecayVsWords_Names2.pdf', width = 10, height = 8)


