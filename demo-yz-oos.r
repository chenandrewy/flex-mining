# demo of "out of sample" (rolling window) strategies
# Created 2022 05 Andrew Chen

# SETUP ====

library(tidyverse)
library(lubridate)
library(haven) # for read_sas

# number of strategies to sample 
nstrat = 1000

temp = read_sas('../Data Yan-Zheng/Yan_Zheng_RFS_Data.sas7bdat')

# clean, select ew or vw
temp1 = temp %>% 
  mutate(
    signalname = paste(transformation, fsvariable, sep = '.')
  ) %>% 
  transmute(
    signalname, date = DATE, ret = 100*ddiff_vw
  )


# sample strats (aka signals)
stratlist = temp1$signalname %>% unique()
stratselect = sample(stratlist, nstrat, replace = F)

yz_ret = temp1 %>% 
  filter(
    signalname %in% stratselect
  )

# clean up
rm(list = ls(pattern = 'temp'))
gc()

# Evaluate strats each june ====

datelist = yz_ret %>% 
  distinct(date) %>% 
  filter(month(date) == 6) %>% 
  arrange(date) %>% 
  pull


paststat = tibble()
for (i in 1:length(datelist)){
  
  
  currdate = datelist[i]
  
  print(currdate)
  
  samp = yz_ret %>% filter(date <= currdate, date >= currdate %m-% months(240) )
  
  sampsum = samp %>% 
    group_by(signalname) %>% 
    summarize(
      rbar = mean(ret), vol = sd(ret), n = sum(!is.na(ret)), tstat = rbar/vol*sqrt(n)
    ) 
  
  sampsum$tradedate = currdate # date when this info can be used to trade
  
  paststat = rbind(paststat, sampsum)
  
} # for i
 
paststat = 
  paststat %>% 
  rename(
    past_rbar = rbar, past_tstat = tstat, past_vol = vol, past_n = n
  )


# Sign yz strategies based on past returns and summarize oos  ====

# add past stats to yz rets, careful with timing
temp = yz_ret %>% 
  left_join(
    paststat %>% mutate(date = tradedate %m+% months(1)) # stats from tradedate used for returns next month
    , by = c('signalname', 'date')
  )  %>% 
  arrange(
    signalname, date
  )

# fill paststats (for non-july ret months) and sign returns
ret_signed = temp %>% 
  group_by(signalname) %>% 
  fill(past_tstat, past_rbar) %>% 
  mutate(
    ret = ret*sign(past_rbar)
  ) %>% 
  filter(!is.na(ret))

# summarize
signalsum = ret_signed %>% 
  group_by(signalname) %>% 
  summarize(
    past_tabs = abs(mean(past_tstat)), rbar = mean(ret), vol = sd(ret)
    , ndate = n(), tstat = rbar/vol*sqrt(ndate)
  )


# Check ====

# should see a positive slope
signalsum %>% 
  ggplot(aes(x=past_tabs, y = rbar)) +
  geom_point()

# should see more in tails thatn standard normal
signalsum %>% 
  ggplot(aes(x=tstat)) +
  geom_histogram() +
  xlab('out of sample long-short t-stat') +
  scale_x_continuous(breaks = seq(-3,5,1))

# more precisely
F_t_oos = ecdf(signalsum$past_tabs)

tlist = c(2,2.5,3)

fdrmax = (1-pnorm(tlist))/(1-F_t_oos(tlist))
  
print('fdr max estimates')
print(fdrmax)



# simple FDR ====
# super simple Storey 2002 algo
pcut = 0.9

pval = paststat %>% 
  mutate(
    pval = 2*(1-pnorm(abs(past_tstat)))
  ) %>% 
  arrange(pval) %>% 
  pull(pval)

Fp = ecdf(pval)

pif = (1-Fp(pcut))/(1-pcut)

pif  

fdr_bh = 0.05/Fp(0.05)

fdr_bh

fdr_bh*pif




