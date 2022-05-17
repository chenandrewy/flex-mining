# Created 2022 05 Andrew: for replicating some old charts 
# old charts do a simple sample split rather than rolling windows.
# perhaps this is best to keep things simple in the first part of the paper


# Setup ====
source('0_Environment.R')

# training sample
train_start = 1963
train_end   = 1993

# test sample
test_start = 1994
test_end = 1994 + 2

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


yz_ret = temp1 %>% 
  inner_join(
    stratselect, by = 'signalname'
  ) %>% 
  select(strati, date, ret)

# clean up
rm(list = ls(pattern = 'temp'))
gc()


# alphas and shrinkage calc ====

# add sample indicators
retdat = yz_ret %>% 
  mutate(
    samp = case_when(
      year(date) >= train_start & year(date) <= train_end ~ 'train'
      , year(date) >= test_start & year(date) <= test_end ~ 'test'
    )
  ) %>% 
  filter(!is.na(ret), !is.na(samp))  %>% 
  setDT()


# find alphas
stratsum = retdat[
  , list(
    alpha = summary(lm(ret ~ 1))$coefficients['(Intercept)' , 'Estimate']
    , tstat = summary(lm(ret ~ 1))$coefficients['(Intercept)' , 't value']
  )
  , by = .(strati, samp)
] 

# find shrinkage
sampsum = stratsum %>% 
  group_by(samp) %>% 
  summarize(
    shrinkage = var(tstat)^-1
  )

# shrink strat summary
stratshrink = stratsum %>% 
  left_join(
    sampsum, by = 'samp'
  ) %>% 
  mutate(
    alpha = alpha*(1-shrinkage)
    , tstat = tstat*(1-shrinkage)
  ) %>% 
  select(-shrinkage)

# t-stat hist ====


# clean up
plotme = stratsum %>% 
  mutate(stattype = 'classical') %>% 
  rbind(
    stratshrink %>% mutate(stattype = 'stein_ez') 
  )
  
# quick plot to console (improve meee)
ggplot(plotme, aes(x=tstat)) +
  geom_histogram(
    aes(fill = stattype), alpha = 0.6, position = 'identity'
  ) +
  theme_minimal()


# oos vs shrinkage ====

nbin = 50

# assign strats to bins using training data
stratshrink2 = stratshrink %>% 
  filter(samp == 'train') %>% 
  mutate(
    bin = ntile(alpha, nbin)
  ) 

# summarize by bin
binshrink = stratshrink2 %>% 
  group_by(bin) %>% 
  summarize(
    alpha = mean(alpha), tstat= mean(tstat)
  )

# merge all data together
plotme = stratshrink2 %>% 
  select(strati, bin) %>% 
  left_join(
    binshrink %>% rename(alpha_shrink = alpha, tstat_shrink = tstat)
    , by = 'bin'
  ) %>% 
  left_join(
    stratsum %>% filter(samp == 'test') %>% 
      rename(alpha_test = alpha, tstat_test = tstat)
    , by = 'strati'
  )


# plot (make me better pls)
ggplot(plotme, aes(x=bin)) +
  geom_boxplot(aes(y=alpha_test, group = bin))  +
  geom_point(
    aes(y=alpha_shrink ), color = 'blue', size = 3
  ) +
  coord_cartesian(ylim = c(-2.5, 2.5))