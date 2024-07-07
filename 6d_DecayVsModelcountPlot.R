# Setup -------------------------------------------------------------------
source('0_Environment.R')

czcat = fread('DataInput/SignalsTheoryChecked.csv') %>% 
  select(signalname, Year, theory, NoModel, Stylized, Dynamic, Quantitative)

czcat[, modeltype := case_when(NoModel == 1 ~ 'No Model',
                               Stylized == 1 ~ 'Stylized',
                               Dynamic == 1 ~ 'Dynamic',
                               Quantitative == 1 ~ 'Quantitative')]
czcat[, modeltype := factor(modeltype, levels = c('No Model', 'Stylized', 'Dynamic', 'Quantitative'))]

czret = readRDS('../Data/Processed/czret_keeponly.RDS') %>% 
  left_join(czcat, by = 'signalname') 

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
  left_join(czcat[, .(signalname, modeltype)])  %>% 
  mutate(
    diff_ret = (postsamp)/insamp
  ) %>% 
  group_by(modeltype) %>% 
  mutate(rank = row_number()) %>%
  ungroup()

# Plot with regression line ------------------------------------

# regression 
reg = lm(diff_ret ~ modeltype, plotme) %>% 
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

repelsize = 5
repelcolor = 'royalblue4'
# Use ggplot to create the plot
pos <- position_jitter(width = 0.2, seed = 1)
ggplot(plotme, aes(x = modeltype, y = diff_ret))+  
  geom_abline(
    aes(intercept = reg$coefficients[1], slope = reg$coefficients[2])
    , color = colors[1], size = 2, alpha = 1
  ) +
  geom_hline(yintercept = 0, color = 'gray', size = 1) +
  geom_hline(yintercept = 1, color = 'gray', size = 1) +  
  geom_jitter(size = 2.5,alpha = 0.5,
              position = pos) +
  labs(y = expression('[Post-Sample] / [In-Sample]')
       , x = '') +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))+
  theme_light(base_size = 26) +
  theme(
    legend.position = c(80,85)/100
    , legend.spacing.y = unit(0, units = 'cm')
    , legend.background = element_rect(fill='transparent')) +
  scale_y_continuous(breaks = seq(-5,5,1)) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(signalname %in% c('BMdec', 'Mom12m', 'Size', 'AssetGrowth', 'OperProf') 
                     & modeltype == 'No Model', signalname, '')),
    position = pos,
    max.overlaps = Inf
    , box.padding = 1.5
, color = repelcolor, size = repelsize
    , xlim = c(0, 1.2)
  ) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(rank <= 10 & modeltype == 'Stylized', signalname, '')),
    position = pos,
    max.overlaps = Inf
    , box.padding = 1.5, color = repelcolor, size = repelsize
    , xlim = c(1.2, 2.2)
  ) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(rank <= 5 & modeltype == 'Dynamic', signalname, '')),
    position = pos,
    max.overlaps = Inf,
    box.padding = 1.5,
    color = repelcolor, size = repelsize
    , xlim = c(2.2, 3.2)
  ) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(rank <= 10 & modeltype == 'Quantitative', signalname, '')),
    position = pos,
    max.overlaps = Inf,
    box.padding = 1.5,
    color = repelcolor, size = repelsize
    , xlim = c(3.2, 4.2)
  ) + coord_cartesian(xlim = c(0.5,4))

ggsave('../Results/Fig_DecayVsModel_Names.pdf', width = 10, height = 8)

# Plot with group means ------------------------------------

# Calculate means
means <- plotme %>%
  group_by(modeltype) %>%
  summarise(mean_diff_ret = mean(diff_ret, na.rm = TRUE))

######################
# Use ggplot to create the plot
pos <- position_jitter(width = 0.2, seed = 1)
plt = ggplot(plotme, aes(x = modeltype, y = diff_ret)) + 
  geom_jitter(size = 2.5,alpha = 0.5,
              position = pos) +
  stat_summary(fun = mean, geom = "point", aes(group = 1),
               color = "blue", size = repelsize, shape = 18)+
  geom_line(data = means, aes(x = modeltype, y = mean_diff_ret, group = 1),
            color = colors[1], size = 1) +  
  geom_hline(yintercept = 0, color = 'gray', size = 1) +
  geom_hline(yintercept = 1, color = 'gray', size = 1) +  

  labs(y = expression('[Post-Sample] / [In-Sample]')
       , x = '') +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))+
  theme_light(base_size = 26) +
  theme(
    legend.position = c(80,85)/100
    , legend.spacing.y = unit(0, units = 'cm')
    , legend.background = element_rect(fill='transparent')) +
  scale_y_continuous(breaks = seq(-5,5,1)) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(signalname %in% c('BMdec', 'Mom12m', 'Size', 'AssetGrowth', 'OperProf') 
                     & modeltype == 'No Model', signalname, '')),
    position = pos,
    max.overlaps = Inf
    , box.padding = 1.5
    , color = repelcolor, size = repelsize
    , xlim = c(0, 1.2)
  ) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(rank <= 10 & modeltype == 'Stylized', signalname, '')),
    position = pos,
    max.overlaps = Inf
    , box.padding = 1.5, color = repelcolor, size = repelsize
    , xlim = c(1.2, 2.2)
  ) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(rank <= 5 & modeltype == 'Dynamic', signalname, '')),
    position = pos,
    max.overlaps = Inf,
    box.padding = 1.5,
    color = repelcolor, size = repelsize
    , xlim = c(2.2, 3.2)
  ) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(rank <= 10 & modeltype == 'Quantitative', signalname, '')),
    position = pos,
    max.overlaps = Inf,
    box.padding = 1.5,
    color = repelcolor, size = repelsize
    , xlim = c(3.2, 4.2)
  ) + 
  coord_cartesian(xlim = c(0.5,4), ylim = c(-1.75,+2.6))

ggsave('../Results/Fig_DecayVsModel_NamesMeans.pdf', width = 10, height = 8)

# save png for ppt
ggsave('../Results/Fig_DecayVsModel_NamesMeans.png', width = 10, height = 8)

# Plot blank axis for slides ------------------------------------

# Calculate means
means <- plotme %>%
  group_by(modeltype) %>%
  summarise(mean_diff_ret = mean(diff_ret, na.rm = TRUE))

######################
# Use ggplot to create the plot
pos <- position_jitter(width = 0.2, seed = 1)
plt = ggplot(plotme %>% mutate(diff_ret = 0.1), aes(x = modeltype, y = diff_ret)) + 
  geom_jitter(size = 0.01,alpha = 0.5,position = pos, color='white') +
  geom_hline(yintercept = 0, color = 'gray', size = 1) +
  geom_hline(yintercept = 1, color = 'gray', size = 1) +  
  labs(y = expression('[Post-Sample] / [In-Sample]')
       , x = '') +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))+
  theme_light(base_size = 26) +
  theme(
    legend.position = c(80,85)/100
    , legend.spacing.y = unit(0, units = 'cm')
    , legend.background = element_rect(fill='transparent')) +
  scale_y_continuous(breaks = seq(-5,5,1)) + 
  coord_cartesian(xlim = c(0.5,4), ylim = c(-1.75,+2.6))

ggsave('../Results/Fig_DecayVsModel_NamesMeans_0.pdf', width = 10, height = 8)

# save png for ppt
ggsave('../Results/Fig_DecayVsModel_NamesMeans_0.png', width = 10, height = 8)

# Plot without signalnames for slides ------------------------------------

# Calculate means
means <- plotme %>%
  group_by(modeltype) %>%
  summarise(mean_diff_ret = mean(diff_ret, na.rm = TRUE)) %>% 
  mutate(type = 'Model Type Mean')

######################
# Use ggplot to create the plot
pos <- position_jitter(width = 0.2, seed = 1)
plt0 = ggplot(plotme, aes(x = modeltype, y = diff_ret)) + 
  # jitter
  geom_jitter(size = 2.5,alpha = 0.0,
              position = pos) +
  # make nice
  geom_hline(yintercept = 0, color = 'gray', size = 1) +
  geom_hline(yintercept = 1, color = 'gray', size = 1) +  
  labs(y = expression('[Post-Sample Return] / [In-Sample]')
       , x = '') +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))+
  theme_light(base_size = 26) +
  theme(
    legend.position = c(68,15)/100
    , legend.title = element_blank()
    , legend.spacing.y = unit(0, units = 'cm')
    , legend.background = element_rect(fill='transparent')) +
  scale_y_continuous(breaks = seq(-5,5,1)) +  
  coord_cartesian(xlim = c(1,3.8), ylim = c(-1.75,+2.6)) 

# save png for ppt
ggsave('../Results/Extra/Fig_DecayVsModel_NoNames0.png', width = 10, height = 8)

plt1 = plt +
  # jitter
  geom_jitter(size = 2.5,alpha = 0.5,
              position = pos) +
  # add means
  geom_point(data = means, aes(x = modeltype,y = mean_diff_ret, 
              color=type, shape=type),
              size = 4.5) +
  geom_line(data = means, aes(x = modeltype, y = mean_diff_ret, group = 1),
            color = colors[1], size = 1) +  
  scale_color_manual(values = c('Model Type Mean' = colors[1]))  +
  scale_shape_manual(values = c('Model Type Mean' = 17)) +  
  coord_cartesian(xlim = c(1,3.8), ylim = c(-1.75,+2.6)) 

# save png for ppt
ggsave('../Results/Extra/Fig_DecayVsModel_NoNames1.png', width = 10, height = 8)
