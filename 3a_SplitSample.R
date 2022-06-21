# Strategy returns based on sample splits

# Setup ====
rm(list =ls())
source('0_Environment.R')
source('0_functions.R')

# training sample
train_start = 1963
train_end   = 1993

# test sample
test_start = 1994
test_end = 1994 + 2

useData = list(
  dataset = 'yz_data'  # all_data
  , signal_form = 'ratio'
  , portnum = 10
  , weights = 'ew'
  , longshort_form = 'ls_extremes'
)


# Prepare data ------------------------------------------------------------
# Load data


if (useData$dataset == 'all_data') {
  
  tmp = readRDS(
    paste0('../Data/LongShortPortfolios/stratdat_',
           useData$signal_form, '_', 
           useData$longshort_form,
           useData$portnum, 
           useData$weights, '.RData')
  )
  
  rets = tmp$ret %>% 
    transmute(signalname, 
              date,
              ret)
  
  plotString = paste0(useData$dataset, '_',  # For saving plots
           useData$signal_form, '_', 
           useData$longshort_form,
           useData$portnum, 
           useData$weights)
  
  # yz_data
} else if (useData$dataset == 'yz_data') {
  
  # number of strategies to sample
  nstrat = 1000
  
  temp = read_sas('../Data Yan-Zheng/Yan_Zheng_RFS_Data.sas7bdat')
  
  # clean, select ew or vw
  temp1 = temp %>%
    mutate(
      signalname = paste(transformation, fsvariable, sep = '.')
    ) %>%
    transmute(
      signalname, date = DATE, ret = 100*ddiff_ew
    )
  
  # sample strats (aka signals)
  stratlist = temp1$signalname %>% unique()
  stratselect = tibble(
    signalname = sample(stratlist, nstrat, replace = F)
    , strati = 1:nstrat
  )
  
  
  rets = temp1 %>%
    inner_join(
      stratselect, by = 'signalname'
    ) %>%
    select(signalname, date, ret)

  plotString = paste0(useData$dataset, '_',
                      useData$signal_form, '_', 
                      useData$longshort_form,
                      useData$portnum, 
                      useData$weights)
  
  # clean up
  rm(list = ls(pattern = 'temp'))
  gc()
  
} else {
  
  message(paste('dataset must be one of yz_data or all_data'))
  
}



# alphas and shrinkage calc ====
stratSum = calculate_alpha_shrinkage(dt = rets,
                                     train_start = train_start,
                                     train_end = train_end,
                                     test_start = test_start,
                                     test_end = test_end)

# quick plot to console
stratSum %>% 
  mutate(stats = ifelse(stattype == 'classical', 'classical', 'JS shrinkage')) %>% 
  ggplot(aes(x=tstat)) +
  geom_histogram(
    aes(fill = stats), alpha = 0.6, position = 'identity') +
  labs(x = 't-stat', y = 'Count', fill = '') +
  theme_bw(base_size = 14)


ggsave(paste0('../Results/Hist_', plotString, '.png'), width = 12, height = 10)

# oos vs shrinkage ====

nbin = 50

# Summarize alphas by bins of training data
binshrink = stratSum %>% 
  filter(stattype == 'stein_ez', samp == 'train') %>% 
  mutate(
    bin = ntile(alpha, nbin)
  ) %>% 
  group_by(bin) %>% 
  summarize(
    alpha = mean(alpha), tstat= mean(tstat)
  )

# merge all data together
plotme = stratSum %>% 
  filter(stattype == 'stein_ez', samp == 'train') %>% 
  mutate(
    bin = ntile(alpha, nbin)
  ) %>%  
  select(signalname, bin) %>% 
  left_join(
    binshrink %>% rename(alpha_shrink = alpha, tstat_shrink = tstat)
    , by = 'bin'
  ) %>% 
  left_join(
    stratSum %>% 
      filter(stattype == 'classical', samp == 'test') %>% 
      rename(alpha_test = alpha, tstat_test = tstat)
    , by = 'signalname'
  )


# plot
outliersU = quantile(plotme$alpha_test, .995, na.rm = TRUE)
outliersD = quantile(plotme$alpha_test, .005, na.rm = TRUE)
plotme %>% 
  ggplot(aes(x=bin)) +
  geom_boxplot(aes(y=alpha_test, group = bin), outlier.shape = '')  +
  geom_point(
    aes(y=alpha_shrink ), color = 'blue', size = 3
  ) +
  coord_cartesian(ylim = c(outliersD -.1, outliersU + .1)) +
  labs(x = 'Training data alpha bin', y = 'Test data alpha') +
  theme_bw(base_size = 14)

ggsave(paste0('../Results/TrainVsTest_', plotString, '.png'), width = 12, height = 10)
