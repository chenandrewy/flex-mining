# Setup -------------------------------------------------------------------
source('0_Environment.R')

czcat = fread('DataInput/SignalsTheoryChecked.csv') %>% 
  select(signalname, Year, Journal, theory, NoModel, Stylized, Dynamic, Quantitative)

czcat %>% select(Journal) %>% distinct()

# Define top journals
top_finance = c('JF', 'JFE', 'RFS')
top_econ = c('QJE', 'JPE')  # Note: AER, Econometrica, REStud not in your data

# Add journal type classifications
czcat[, journaltype := case_when(
  Journal %in% top_finance ~ 'Top 3 Finance',
  Journal %in% top_econ ~ 'Top Econ',
  TRUE ~ 'Other'
)]
czcat[, journaltype := factor(journaltype, levels = c('Top Econ', 'Top 3 Finance', 'Other'))]

# Add combined elite classification
czcat[, elitejournal := case_when(
  Journal %in% c(top_finance, top_econ) ~ 'Elite Journal',
  TRUE ~ 'Other'
)]
czcat[, elitejournal := factor(elitejournal, levels = c('Elite Journal', 'Other'))]

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

# Reorder journal type factor
czcat[, journaltype := factor(journaltype, levels = c('Top Econ', 'Top 3 Finance', 'Other'))]

# data to plot - modified for journal analysis
plotme = czdecay  %>% 
  left_join(czcat[, .(signalname, journaltype, elitejournal)])  %>% 
  mutate(
    diff_ret = (postsamp)/insamp
    # Ensure the factor levels are preserved in plotme
    , journaltype = factor(journaltype, levels = c('Top Econ', 'Top 3 Finance', 'Other'))
  ) %>% 
  group_by(journaltype) %>% 
  mutate(rank = row_number()) %>%
  ungroup()

# Create alternative version using elite journal classification
plotme_elite = czdecay  %>% 
  left_join(czcat[, .(signalname, journaltype, elitejournal)])  %>% 
  mutate(
    diff_ret = (postsamp)/insamp
  ) %>% 
  group_by(elitejournal) %>% 
  mutate(rank = row_number()) %>%
  ungroup()

# Calculate means for elite plot
means_elite <- plotme_elite %>%
  group_by(elitejournal) %>%
  summarise(mean_diff_ret = mean(diff_ret, na.rm = TRUE))

# Use ggplot to create the plot
pos <- position_jitter(width = 0.2, seed = 1)
ggplot(plotme_elite, aes(x = elitejournal, y = diff_ret))+  
  geom_jitter(size = 2.5, alpha = 0.5,
              position = pos) +
  stat_summary(fun = mean, geom = "point", aes(group = 1),
               color = "blue", size = repelsize, shape = 18)+
  geom_line(data = means_elite, aes(x = elitejournal, y = mean_diff_ret, group = 1),
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
  scale_x_discrete(labels = c("Top 3 + Top 5", "Other")) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(signalname %in% c('BMdec', 'Mom12m', 'Size', 'AssetGrowth', 'OperProf') 
                     & elitejournal == 'Elite Journal', signalname, '')),
    position = pos,
    max.overlaps = Inf,
    box.padding = 1.5,
    color = repelcolor, size = repelsize,
    xlim = c(0, 1.2)
  ) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(rank <= 15 & elitejournal == 'Elite Journal', signalname, '')),
    position = pos,
    max.overlaps = Inf,
    box.padding = 1.5,
    color = repelcolor, size = repelsize,
    xlim = c(1.2, 2.2)
  ) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(rank <= 15 & elitejournal == 'Other', signalname, '')),
    position = pos,
    max.overlaps = Inf,
    box.padding = 1.5,
    color = repelcolor, size = repelsize,
    xlim = c(2.2, 3.2)
  ) + 
  coord_cartesian(xlim = c(0.5,3))

ggsave('../Results/Fig_DecayVsJournal_Names_Elite.pdf', width = 10, height = 8)

# Plot with group means ------------------------------------

# Calculate means
means <- plotme %>%
  group_by(journaltype) %>%
  summarise(mean_diff_ret = mean(diff_ret, na.rm = TRUE))

######################
# Use ggplot to create the plot
pos <- position_jitter(width = 0.2, seed = 1)
plt = ggplot(plotme, aes(x = journaltype, y = diff_ret)) + 
  geom_jitter(size = 2.5,alpha = 0.5,
              position = pos) +
  stat_summary(fun = mean, geom = "point", aes(group = 1),
               color = "blue", size = repelsize, shape = 18)+
  geom_line(data = means, aes(x = journaltype, y = mean_diff_ret, group = 1),
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
                     & journaltype == 'Top Econ', signalname, '')),
    position = pos,
    max.overlaps = Inf,
    box.padding = 1.5,
    color = repelcolor, size = repelsize,
    xlim = c(0, 1.2)
  ) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(rank <= 5 & journaltype == 'Top Econ', signalname, '')),
    position = pos,
    max.overlaps = Inf,
    box.padding = 1.5,
    color = repelcolor, size = repelsize,
    xlim = c(1.2, 2.2)
  ) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(rank <= 10 & journaltype == 'Top 3 Finance', signalname, '')),
    position = pos,
    max.overlaps = Inf,
    box.padding = 1.5,
    color = repelcolor, size = repelsize,
    xlim = c(2.2, 3.2)
  ) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(rank <= 10 & journaltype == 'Other', signalname, '')),
    position = pos,
    max.overlaps = Inf,
    box.padding = 1.5,
    color = repelcolor, size = repelsize,
    xlim = c(3.2, 4.2)
  ) + 
  coord_cartesian(xlim = c(0.5,4), ylim = c(-1.75,+2.6))
plt
ggsave('../Results/Fig_DecayVsJournal_NamesMeans.pdf', width = 10, height = 8)
