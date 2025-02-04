# Setup -------------------------------------------------------------------
source('0_Environment.R')

inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType, 
                                  topT = globalSettings$topT)

czcat = fread('DataInput/SignalsTheoryChecked.csv') %>% 
  select(signalname, Year, Journal, theory, NoModel, Stylized, Dynamic, Quantitative) 

czcat %>% select(Journal) %>% distinct()

# Define top journals
top_finance = c('JF', 'JFE', 'RFS')
top_econ = c('QJE', 'JPE')  # Note: AER, Econometrica, REStud not in data
top_accounting = c('JAR', 'JAE', 'AR')  # Top 3 Acct journals

# Add journal type classifications
czcat[, journaltype := case_when(
  Journal %in% top_finance ~ 'Top 3 Fin',
  Journal %in% top_econ ~ 'Top 5 Econ',  # Named "Top 5" even though we only have 2
  Journal %in% top_accounting ~ 'Top 3 Acct',
  TRUE ~ 'Other'
)]
czcat[, journaltype := factor(journaltype, 
                             levels = c('Top 5 Econ', 'Top 3 Fin', 'Top 3 Acct', 'Other'))]

czret = readRDS('../Data/Processed/czret_keeponly.RDS') %>% 
  left_join(czcat, by = 'signalname') %>% 
  filter(signalname %in% inclSignals) 

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
czcat[, journaltype := factor(journaltype, levels = c('Top 5 Econ', 'Top 3 Fin', 'Top 3 Acct', 'Other'))]

# data to plot - modified for journal analysis
plotme = czdecay  %>% 
  left_join(czcat[, .(signalname, journaltype)])  %>% 
  mutate(
    diff_ret = (postsamp)/insamp
    # Ensure the factor levels are preserved in plotme
    , journaltype = factor(journaltype, levels = c('Top 5 Econ', 'Top 3 Fin', 'Top 3 Acct', 'Other'))
  ) %>% 
  group_by(journaltype) %>% 
  mutate(rank = row_number()) %>%
  ungroup()

repelsize = 5
repelcolor = 'royalblue4'
# Plot with group means ------------------------------------

# Calculate means
means <- plotme %>%
  group_by(journaltype) %>%
  summarise(mean_diff_ret = mean(diff_ret, na.rm = TRUE))

######################
# Use ggplot to create the plot
pos <- position_jitter(width = 0.2, seed = 1)
plt = ggplot(plotme, aes(x = journaltype, y = diff_ret)) + 
  # First: reference lines
  geom_hline(yintercept = 0, color = 'gray', size = 1) +
  geom_hline(yintercept = 1, color = 'gray', size = 1) +  
  # Second: mean line and points
  geom_line(data = means, aes(x = journaltype, y = mean_diff_ret, group = 1),
            color = colors[1], size = 1, alpha = 0.5) +  
  stat_summary(fun = mean, geom = "point", aes(group = 1),
               color = "blue", size = repelsize, shape = 18)+
  # Third: scatter points
  geom_jitter(size = 2.5, alpha = 0.5,
              position = pos) +
  # Fourth: all text labels on top
  ggrepel::geom_text_repel(
    aes(label=ifelse(signalname %in% c('BMdec', 'Mom12m', 'Size', 'AssetGrowth', 'OperProf') 
                     & journaltype == 'Top 5 Econ', signalname, '')),
    position = pos,
    max.overlaps = Inf,
    box.padding = 1.5,
    color = repelcolor, size = repelsize,
    xlim = c(0, 1.2)
  ) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(rank <= 5 & journaltype == 'Top 5 Econ', signalname, '')),
    position = pos,
    max.overlaps = Inf,
    box.padding = 1.5,
    color = repelcolor, size = repelsize,
    xlim = c(1.2, 2.2)
  ) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(rank <= 10 & journaltype == 'Top 3 Fin', signalname, '')),
    position = pos,
    max.overlaps = Inf,
    box.padding = 1.5,
    color = repelcolor, size = repelsize,
    xlim = c(2.2, 3.2)
  ) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(rank <= 10 & journaltype == 'Top 3 Acct', signalname, '')),
    position = pos,
    max.overlaps = Inf,
    box.padding = 1.5,
    color = repelcolor, size = repelsize,
    xlim = c(3.2, 4.2)
  ) +
  ggrepel::geom_text_repel(
    aes(label=ifelse(rank <= 10 & journaltype == 'Other', signalname, '')),
    position = pos,
    max.overlaps = Inf,
    box.padding = 1.5,
    color = repelcolor, size = repelsize,
    xlim = c(4.2, 5.2)
  ) + 
  # Theme and scales at the end
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
  coord_cartesian(xlim = c(1,4.5), ylim = c(-1.75,+2.6))
plt


ggsave('../Results/Fig_DecayVsJournal_Means.pdf', width = 10, height = 8)  # Increased width
