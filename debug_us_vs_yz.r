# reconcile yz signal counts

# Find YZ exceptions -------------------------------------------------------------------------

rm(list = ls())

source('0_Environment.R')

yzraw = read_sas('../Data Yan-Zheng/Yan_Zheng_RFS_Data.sas7bdat')



signals = yzraw %>% transmute(fun = transformation, x = fsvariable) %>% distinct(fun, x)


xsum = signals  %>% 
  group_by(x) %>% 
  summarize(
    n = n()
  ) %>% 
  arrange(n)

# print out altered ones
#   there are 15 altered ones
#   2 are strange: rdip and txndbr
#   13 are from the denominators, but omits emp and mktcap (since they're not in the numerator)
altered = xsum %>% filter(n < 76)
altered
compnames$yz.denom[!compnames$yz.denom %in% altered$x]


# compare selected x 
#   the 13 altered from the denominators seem to drop x == y, leading to a decrease of 13 * 5 = 65 signals
#   the 2 altered strange ones drop the pd_var_ transformations,
#     and there are 31 pd_var transformations leading to a decrease of 2 * 31 = 62 signals
#   this explains the 240*76 = 18,240 - 18,113 = 127

signals %>% filter(x == 'aco') %>% 
  full_join(
    signals %>% filter(x == 'act'), by = c('fun')
  ) %>% 
  full_join(
    signals %>% filter(x == 'at'), by = c('fun')
  ) %>%   
  full_join(
    signals %>% filter(x == 'rdip'), by = c('fun')
  ) %>%     
  full_join(
    signals %>% filter(x == 'txndbr'), by = c('fun')
  ) %>%       
  print(n=Inf)


# Compare US to YZ -------------------------------------------------------------------------


## load stuff -----------------------------------------------------------------

# yz
yzraw = read_sas('../Data Yan-Zheng/Yan_Zheng_RFS_Data.sas7bdat')

yz_signal_list = yzraw %>% distinct(transformation,fsvariable) %>% 
  arrange(transformation, fsvariable) %>% 
  mutate(signalid = row_number()) 

yz = yzraw %>% 
  left_join(yz_signal_list) %>% 
  mutate(yearm = as.yearmon(DATE), source = 'yz') %>% 
  pivot_longer(
    cols = starts_with('ddiff') ,names_to = 'sweight', names_prefix = 'ddiff_', values_to = 'ret'
  ) %>% 
  select(source, sweight, signalid, yearm, ret) %>% 
  mutate(ret = ret*100)

# a run of mine
# stratdat = readRDS('../Data/LongShortPortfolios/stratdat_yzrep_2022_12_27.RData')
# stratdat = readRDS('../Data/LongShortPortfolios/stratdat 2023-01-11 16.RData')
stratdat = readRDS('../Data/LongShortPortfolios/stratdat 2023-01-11 23h30m.RData')

us = stratdat$ret %>% 
  left_join(stratdat$port_list %>% select(portid, sweight)) %>% 
  transmute(
    source = 'us', sweight, signalid, yearm, ret
  ) %>% 
  filter(yearm >= 1963.6, yearm <= 2014) 


# computstat
comp0 = readRDS('../Data/Intermediate/CompustatAnnual.RData') %>% 
  filter(!is.na(at))

sum1 = rbind(us,yz) %>% 
  group_by(source, sweight, signalid) %>% 
  summarize(rbar = mean(ret), nmonth = n(), tstat = mean(ret)/sd(ret)*sqrt(n())) 


## compare us and yz -------------------------------------------------------


# compare number of signals with nmonths >= 2
sum1 %>% 
  group_by(source,sweight) %>% 
  filter(nmonth >= 2) %>% 
  summarize(nsignal = n())

# compare order stats: ew
#   yz has at minimum 126 months for each strat
#   5% of our strats have so few obs

q = c(1, 5, 10, 25, 50)/100
q = c(q, 1-q) %>% sort() %>% unique()

sum1 %>%
  filter(nmonth >= 2) %>% 
  filter(sweight == 'ew') %>% 
  group_by(source) %>% 
  summarize(
    q = q, rbar_q = quantile(rbar, q), nmonth_q = quantile(nmonth, q), tstat_q = quantile(tstat, q)
  ) %>% 
  pivot_wider(
    names_from = 'source', values_from = ends_with('_q')
  )

# compare order stats: 
sum1 %>%
  filter(nmonth >= 2) %>%   
  filter(sweight == 'vw') %>% 
  group_by(source) %>% 
  summarize(
    q = q, rbar_q = quantile(rbar, q), nmonth_q = quantile(nmonth, q), tstat_q = quantile(tstat, q)
  ) %>% 
  pivot_wider(
    names_from = 'source', values_from = ends_with('_q')
  )



## make list of bad signals -----------------------------------------------


bad_signals = stratdat$signal_list %>% 
  select(signalid, signal_form, v1, v2) %>% 
  left_join(
    sum1 %>% filter(source == 'us', sweight == 'ew') %>% select(signalid, nmonth)
  ) %>% 
  mutate(nmonth = if_else(is.na(nmonth), as.integer(0) , nmonth)) %>% 
  filter( nmonth <= 120) %>% 
  arrange(nmonth, v1, signal_form, v2)

writexl::write_xlsx(bad_signals, '../Data/bad_signals.xlsx')


compare_compvar = sum1 %>% 
  filter(source == 'us', sweight == 'ew') %>% 
  left_join(stratdat$signal_list) %>% 
  ungroup() %>% 
  select(source, v1,nmonth) %>% rbind(
    sum1 %>% 
      filter(source == 'yz', sweight == 'ew') %>% 
      left_join(yz_signal_list) %>% 
      rename(v1 = fsvariable) %>% 
      ungroup() %>% 
      select(source, v1, nmonth)    
  ) %>% 
  group_by(source, v1) %>% 
  summarize(
    nstrat = n(), nmonth_min = min(nmonth)
  ) %>% 
  pivot_wider(names_from = source
              , values_from = c(nstrat,nmonth_min)) %>% 
  mutate(nstrat_diff = nstrat_yz - nstrat_us) %>% 
  arrange(-nstrat_diff)

compare_compvar

writexl::write_xlsx(compare_compvar, '../Data/compare_compvar.xlsx')


## zoom in on tails --------------------------------------------------------

badvar = bad_signals$v1 %>% unique()

p = c(0.1, 1, 5, 10, 25, 50)
p = c(p, 100-p) %>% unique() %>% sort()

comp0 %>% select(gvkey, datadate, all_of(badvar)) %>% 
  pivot_longer(cols = -c(gvkey,datadate), names_to = 'name', values_to = 'value') %>% 
  group_by(name) %>% 
  summarize(
    p, value_p = quantile(value, p/100, na.rm = T)
  ) %>% 
  pivot_wider(names_from = 'p', names_prefix = 'p', values_from = 'value_p') %>% 
  print(n=50)

# distributions excluding 0
temp = comp0 %>% select(gvkey, datadate, all_of(badvar)) %>% 
  pivot_longer(cols = -c(gvkey,datadate), names_to = 'name', values_to = 'value') %>% 
  filter(value != 0) %>% 
  group_by(name) %>% 
  summarize(
    p, value_p = quantile(value, p/100, na.rm = T)
  ) %>% 
  pivot_wider(names_from = 'p', names_prefix = 'p', values_from = 'value_p') 
temp %>% print(n=50)


# observations by year excluding zero
#   seems like there's barely enough to have a long short portfolio using all nonzero
comp0 %>% 
  mutate(year = year(datadate)) %>% 
  select(gvkey, year, all_of(badvar)) %>% 
  pivot_longer(cols = -c(gvkey,year), names_to = 'name', values_to = 'value') %>% 
  filter(!is.na(value)) %>% 
  group_by(name,year) %>% 
  summarize(n_nonzero = sum(value != 0), n_zero = sum(value == 0)) %>% 
  group_by(name) %>% 
  summarize(
    across(starts_with('n_'), list(mean = mean, med = median))
  ) %>% 
  print(n=50)



## debug one -------------------------------------------------------------------------


# select one 
bad_signals %>% 
  filter(v1 == 'dudd') %>% 
  print(n= Inf)

# signal_list0 = signal_list

signal_list = signal_list0 %>% filter(
  v1 == 'dudd', signal_form == 'ratio', v2 == 'at'
) 

debugSource('0_Environment.R')

num_cores = 1

signali = 1
ls_dat = make_many_ls()

ls_dat %>% print(n=200)

# check quantiles
qlist = c(10, 1, 0.1)
qlist = c(qlist, 100-qlist)/100 %>% sort()

dt %>% group_by(ret_yearm) %>% 
  summarize(
    q = qlist, signal = quantile(signal, qlist)
  )  %>% 
  pivot_wider(names_from = q, values_from = signal, names_prefix = 'q') %>% 
  as.data.table() %>% 
  print(topn=10) 


dt %>% group_by(ret_yearm) %>% 
  summarize(
    n_neg = sum(signal < 0), n_zero = sum(signal == 0), n_pos = sum(signal > 0)
    , n = n()
  )  %>% 
  as.data.table() %>% 
  print(topn = 10)

  

breakdat = dt %>% 
  group_by(ret_yearm) %>% 
  summarize(
    ntot = n()
    , qlo_alt = quantile(signal, 20/pmax(ntot,20))
    , qlo = quantile(signal, 1/portnum)
    , qhi = quantile(signal, 1-1/portnum)
    , qhi_alt = quantile(signal, 1-20/pmax(ntot,20))    
  ) %>% 
  # backup: replace simple quantile with the alts if not enough stocks
  mutate(
    qlo = pmax(qlo, qlo_alt), qhi = pmin(qhi, qhi_alt)
  ) %>%
  # backup 2: if qlo == qhi, adjust
  mutate(
    qlo = if_else(qlo == qhi, pmin(qlo, qlo_alt), qlo)
    , qhi = if_else(qlo == qhi, pmax(qhi, qhi_alt), qhi)
  )


dt %>% 
  inner_join(breakdat, by = 'ret_yearm') %>% 
  mutate(
    short = signal <= qlo
    , long = signal >= qhi
  ) %>% 
  mutate(
    port = case_when(
      short & !long ~ 'short'
      , !short & long ~ 'long'
    )
  )