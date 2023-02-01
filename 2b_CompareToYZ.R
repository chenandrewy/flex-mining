# load stuff -----------------------------------------------------------------
rm(list = ls())
source('0_Environment.R')
library(janitor)

## US, compustat ----------------------------------------------------------------------
# stratdat = readRDS('../Data/LongShortPortfolios/stratdat deleteme.RData')
stratdat = readRDS('../Data/LongShortPortfolios/stratdat CZ-style.RData')

us = stratdat$ret %>% 
  left_join(stratdat$port_list %>% select(portid, sweight)) %>% 
  transmute(
    source = 'us', sweight, signalid, yearm, ret, nstock
  ) %>% 
  left_join(stratdat$signal_list)

rm(stratdat)

# computstat
comp0 = readRDS('../Data/Intermediate/CompustatAnnual.RData') %>% 
  filter(!is.na(at))

# cz 
czdoc = fread('../Data/CZ/SignalDoc.csv') %>% 
  clean_names() 
czdoc = czdoc[ , 1:20]

## YZ ----------------------------------------------------------------------

stratdat = readRDS('../Data/LongShortPortfolios/yz_reorg_all.RData')
yz = stratdat$ret %>% 
  left_join(stratdat$signal_list) %>% 
  mutate(
    source = 'yz'
  )

rm(stratdat)


## Combine -----------------------------------------------------------------

both = us %>% mutate(transformation = NA_character_) %>% 
  rbind(
    yz %>% transmute(source, sweight, signalid, yearm, ret, nstock
                     , signal_form, v1, v2, transformation)
  ) %>% 
  filter(yearm >= min(yz$yearm), yearm <= max(yz$yearm))   
 


# Overviews -------------------------------------------------------


## Signal Counts ------------------------------------------------------------------

# by sweight
both %>% 
  group_by(source, sweight) %>% 
  summarize(nsignal = n_distinct(signalid))

# by sweight, signal_form
both %>% 
  group_by(source, sweight, signal_form) %>% 
  summarize(nsignal = n_distinct(signalid)) %>% 
  arrange(sweight, source, nsignal) %>% 
  print(n=40)


## sweight level stats -----------------------------------------------------------------


sumsignal = both %>% 
  group_by(source, sweight, signalid, v1, signal_form, v2) %>% 
  summarize(rbar = mean(ret), nmonth = n(), tstat = mean(ret)/sd(ret)*sqrt(n()))


q = c(1, 5, 10, 25, 50)/100
q = c(q, 1-q) %>% sort() %>% unique()

# compare order stats: 
sumsignal %>%
  filter(nmonth >= 2) %>%   
  group_by(sweight, source, ) %>% 
  summarize(
    q = q, rbar_q = quantile(rbar, q), nmonth_q = quantile(nmonth, q), tstat_q = quantile(tstat, q)
  ) %>% 
  pivot_wider(
    names_from = 'source', values_from = ends_with('_q')
  )


# v1 level stats -------------------------------------------------------------------------


v1mean = sumsignal %>% 
  group_by(source, sweight, v1) %>% 
  summarise(rbarbar = mean(rbar), nmonthbar = mean(nmonth), tstatbar = mean(tstat)) %>% 
  pivot_wider(
    names_from = 'source', values_from = ends_with('bar')
  ) %>% 
  print(n=200)

lm(rbarbar_us ~ rbarbar_yz, v1mean) %>% summary
lm(nmonthbar_us ~ nmonthbar_yz, v1mean) %>% summary
lm(tstatbar_us ~ tstatbar_yz, v1mean) %>% summary



# Signal-level detailed ---------------------------------------------------

# select a sample to focus on
startyear = 0
endyear = startyear + 4000

sumsignal = both %>% 
  filter(yearm > startyear, yearm < endyear) %>% 
  group_by(source, sweight, v1, signal_form, v2) %>% 
  summarize(
    rbar = mean(ret), nmonth = n(), tstat = mean(ret)/sd(ret)*sqrt(n())
    , nstock = mean(nstock)
  )

sumwide = sumsignal %>% 
  ungroup() %>% 
  filter(sweight == 'ew') %>% 
  transmute(v1, signal_form, v2, source, rbar, nmonth) %>% 
  pivot_wider(
    names_from = 'source', values_from = c(rbar, nmonth)
  ) %>% 
  left_join(
    sumsignal %>% filter(source == 'us') %>% select(v1, signal_form, v2, nstock)
  )

# regress us on yz
lm(rbar_us ~ rbar_yz, sumwide) %>% summary


# nobs discrepencies -------------------------------------------------------------------------


setDT(both)

diff_samp = both[ 
  , .(start = min(yearm), end = max(yearm))  
  , by = c('source','sweight','v1','signal_form','v2')  
  ] %>% 
  pivot_wider(
    names_from = 'source', values_from = c(start,end)
  ) %>% 
  mutate(
    dstart = start_us - start_yz
    , dend = end_us - end_yz
  ) %>% 
  filter(dstart != 0 | dend != 0) %>% 
  arrange(-abs(dstart)) %>% 
  print(n=20)

View(diff_samp)

diff_samp %>% filter(dstart <= -1) %>% 
  distinct(v1, .keep_all = T) %>% 
  print(n=40)


# -------------------------------------------------------------------------


%>% 
  mutate(
    diff = us - yz
  ) %>% 
  arrange(-abs(diff))

temp %>% 
  print(n=50)

temp2 = temp %>% group_by(signal_form) %>% 
  summarise(us = mean(us), yz = mean(yz), diff = floor(100*mean(diff))) %>% 
  arrange(-abs(diff)) 
  
head(temp2)  
tail(temp2)

temp %>% filter(signal_form == 'diff(v1/v2)', is.na(yz)) %>% 
  inner_join(us, by = c('v1','signal_form','v2')) %>% 
  select(v1,signal_form,v2, yearm, ret) %>% 
  filter(yearm >= 1997)




both %>% 
  filter(sweight == 'ew') %>% 
  group_by()


# -------------------------------------------------------------------------


temp %>% 
  group_by(signal_form) %>% 
  summarize(mean(abs(diff)), mean(diff), mean(us), mean(yz))



temp %>% filter(is.na(us) | is.na(yz)) %>% 
  print(n=Inf)


# OOS tests ---------------------------------------------------------------

quantile(czdoc$sample_end_year)

# hist(czdoc$sample_end_year)

sampstart = 1963
sampend = 1998
target = list()
target$rbar = 0.7
target$drbar = 0.1


insamp = both %>% 
  select(-c(signal_form, v1, v2, transformation)) %>% 
  filter(sweight == 'ew') %>% 
  filter(yearm >= sampstart, yearm <= sampend) %>% 
  group_by(source, sweight, signalid) %>% 
  summarize(
    rbar = mean(ret)
  )


pubinsamp = insamp %>% 
  mutate(sign = sign(rbar), rbar = rbar*sign) %>% 
  filter(
    rbar > target$rbar - target$drbar
    , rbar < target$rbar + target$drbar
  )


oostest = both %>%
  inner_join(pubinsamp) %>% 
  mutate(ret = ret*sign) %>% 
  group_by(source, yearm) %>% 
  summarize(
    ret = mean(ret)
  ) %>% 
  mutate(
    rollrbar = rollmean(ret, k=5*12, fill = NA, align = 'right')
  )


ggplot(oostest, aes(x=yearm, y=rollrbar)) +
         geom_line( aes(color = source), size = 1) +
        geom_vline( xintercept = sampend) +
  geom_hline(yintercept = 0) +
  geom_hline(yintercept = target$rbar)



# Large returns over time ---------------------------------------------------------

groupsize = 10

temp = both %>% 
  filter(sweight == 'ew') %>% 
  mutate(
    group = floor(yearm/groupsize)*groupsize
  ) %>% 
  group_by(source,signalid,group) %>% 
  summarize(
    year = mean(yearm), rbar = mean(ret)
  )


# -------------------------------------------------------------------------



qlist = c(0.99)
qlist = c(1-qlist, qlist) %>% sort()
plotme = temp %>% 
  group_by(source, group) %>% 
  summarize(
    q = as.character(qlist), rbarq = floor(100*quantile(rbar, qlist))
  ) 

ggplot(plotme, aes(x=group, y=rbarq)) +
  geom_line(aes(color = q, linetype = source), size = 1.5)



plotme %>% 
  pivot_wider(
    id_cols = c('source','group')
    ,names_from = 'q', values_from = 'rbarq', names_prefix = 'q'
  )

plotme %>% 
  pivot_wider(
    id_cols = c('group','q')
    , names_from = 'source', values_from = 'rbarq'
  ) %>% 
  mutate(diff = (us - yz)*sign(us)) %>% 
  print(n=100)

temp %>% 
  group_by(source,group) %>% 
  summarize(
    sd_rbar = sd(rbar)
  ) %>% 
  pivot_wider(
    names_from = 'source', values_from = 'sd_rbar'
  ) %>% 
  mutate(diff = us - yz)


# nstocks? ----------------------------------------------------------------


us = us %>% filter(sweight == 'ew')
yz = yz %>% filter(sweight == 'ew')


badlist = us %>% group_by(v1,signal_form,v2) %>% 
  summarize(
    nstock = mean(nstock)
  ) %>% 
  arrange(nstock) %>% 
  head(30) 
  

both %>% 
  inner_join(badlist, by = c('v1','signal_form', 'v2')) %>% 
  group_by(source, v1, signal_form, v2) %>% 
  summarize(vol = sd(ret)) %>% 
  pivot_wider(names_from = 'source', values_from = 'vol') %>% 
  mutate(diff = us - yz) %>% 
  print(n=40)

