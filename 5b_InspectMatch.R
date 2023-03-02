
# Setup -------------------------------------------------------------------------
rm(list = ls())


source('0_Environment.R')

library(stringr)
library(writexl)

# read matched strategies and get their documentation from stratdat

# I think these files match
# But we really should make it more clear by saving documentation with MatchedData*.RDS
matchdat = readRDS('../Data/Processed/LastMatchedData.RDS')
setDT(matchdat)

stratdat = readRDS('../Data/LongShortPortfolios/stratdat CZ-style-v2.RData')


# load pub stuff
czdat = readRDS('../Data/Processed/czdata.RDS') 

czdat$czsum = czdat$czsum %>% left_join(
  fread('../Data/CZ/SignalDoc.csv') %>% 
    select(1:20) %>% 
    transmute(signalname = Acronym, Authors, Year, Journal, LongDescription, sign = Sign)  
) %>% 
  setDT()

czdat$czret = czdat$czret %>% 
  filter(Keep == 1) %>% 
  left_join(
    czdat$czsum %>% select(signalname,sign) 
  ) %>% 
  setDT()



# Merge dm and pub --------------------------------------------------------

# set up for merge
czdat$czret[
  , ':=' (
    samptype = if_else(samptype == 'postpub','oos',samptype)
    , source = '1_pub'
    , candSignalname = NA
  )
]
matchdat[ , source := '2_dm']

# merge
allret = matchdat %>% rbind(
  czdat$czret %>% transmute(
    candSignalname, eventDate, ret = retOrig, samptype, source, actSignal = signalname, sign
  )
)

# Compare all -------------------------------------------------------------

# not used right now but may be useful for appendix

candsum = allret[
  !is.na(samptype) & !is.na(ret)
  , .(rbar = mean(ret), nmonth = .N, t = mean(ret)/sd(ret)*sqrt(.N))
  , by = c('source','actSignal','candSignalname','samptype')
]



pubsum = candsum[
  , .(rbar = mean(rbar), nmonth = mean(nmonth), t = mean(t), nstrat = .N)
  , by = c('actSignal','source','samptype')
] %>% 
  arrange(actSignal, source, samptype) %>% 
  select(source,actSignal, samptype, rbar, t, nstrat) %>% 
  pivot_wider(
    id_cols = c('actSignal', 'source')
    , names_from = samptype
    , values_from = c(rbar, t, nstrat)
  ) %>% 
  setDT()

pubsum2 = pubsum[ source == '1_pub'] %>% select(actSignal, starts_with('rbar_'), t_insamp) %>% 
  left_join(
    pubsum[ source == '2_dm'] %>% transmute(
      actSignal, rbar_insamp_dm = rbar_insamp, t_insamp_dm = t_insamp, rbar_oos_dm = rbar_oos, nmatch = nstrat_insamp
    )
  ) %>% 
  mutate(diff_rbar_oos = rbar_oos - rbar_oos_dm)

# check at console
#   pub outperforms if t-stat is low
#   seems like matching is too generous when t-stat is low
pubsum2

lm(diff_rbar_oos ~ nmatch, pubsum2) %>% summary()
lm(diff_rbar_oos ~ t_insamp, pubsum2) %>% summary()
lm(diff_rbar_oos ~ rbar_insamp, pubsum2) %>% summary()
lm(t_insamp ~ nmatch, pubsum2) %>% summary()
lm(diff_rbar_oos ~ nmatch + t_insamp + rbar_insamp, pubsum2) %>% summary()

lm(diff_rbar_oos ~ x
 , pubsum2 %>% mutate(x = t_insamp - t_insamp_dm)
 ) %>% 
  summary()

# Inspect select predictors ------------------------------------------------------------------


# read compvars doc
compdoc = readxl::read_xlsx('Yan-Zheng-Compustat-Vars.xlsx') %>% 
  transmute(
    acronym = tolower(acronym)
    , longname 
    , shortername 
  ) 


# create function for outputting tables
inspect_one_pub = function(name){
  
  # make small dat with doc for dm signals
  smallsum = allret[
    actSignal == name & !is.na(samptype) & !is.na(ret)
    , .(rbar = mean(ret), n = .N, t = mean(ret)/sd(ret)*sqrt(.N), sign = mean(sign))
    , by = c('source','actSignal','candSignalname','samptype')
  ] %>% 
    pivot_wider(names_from = samptype, values_from = c(rbar,n,t)) %>% 
    left_join(
      stratdat$signal_list %>% rename(candSignalname = signalid)
      , by = 'candSignalname'    
    ) %>% 
    arrange(desc(source)) %>% 
    select(actSignal, source, v1, v2, signal_form, everything()) %>% 
    select(-c(candSignalname, t_oos)) %>% 
    setDT() 
  
  # add mean
  smallsum = smallsum %>% 
    bind_rows(
      smallsum %>% 
        filter(source == '2_dm') %>% 
        summarize(across(where(is.numeric), mean)) %>% 
        mutate(source = '3_dm_mean')
    )
  
  # plug in and format
  smallsum2 = smallsum %>%
    # change format of formulas
    mutate(
      signal_form = if_else(signal_form == 'v1/v2','(v1)/(v2)', signal_form)
      , signal_form = str_replace_all(signal_form, '\\(', '\\[')
      , signal_form = str_replace_all(signal_form, '\\)', '\\]')    
      , signal_form = str_replace_all(signal_form, 'pdiff', '%$\\\\Delta$')    
      , signal_form = str_replace_all(signal_form, 'diff', '$\\\\Delta$')    
    ) %>% 
    left_join(
      compdoc %>% transmute(v1 = acronym, v1long = substr(shortername,1,23))
    ) %>% 
    left_join(
      compdoc %>% transmute(v2 = acronym, v2long = substr(shortername,1,20))
    ) %>%   
    mutate(
      signal = str_replace(signal_form, 'v1', v1long)
      , signal = str_replace(signal, 'v2', v2long)
    ) %>% 
    select(-c(actSignal, ends_with('long'))) %>% 
    select(source, signal, everything()) 
  
  # clean up for output
  #   compute sample periods
  tempsamp = paste(
    year(czdat$czsum[signalname == name, ]$sampstart) 
    , year(czdat$czsum[signalname == name, ]$sampend)
    , sep = '-'
  )
  tempoos = paste(
    year(czdat$czsum[signalname == name, ]$sampend) +1
    , min(as.numeric(floor(max(stratdat$ret$yearm)))
          , max(year(czdat$czret[signalname == name]$date)))
    , sep = '-'
  )  
  
  # make table
  tabout  = smallsum2 %>% 
    as_tibble() %>% 
    mutate(dist = abs(rbar_insamp - smallsum2[source == '1_pub']$rbar_insamp)) %>% 
    arrange(source, dist)  %>% 
    mutate(id = row_number() - 1) %>% 
    select(
      source,id,signal,sign,starts_with('rbar_')
      , dist, v1, v2, signal_form
      , t_insamp
    ) %>% 
    mutate(across(where(is.numeric), round, 2)) %>% 
    rename(setNames('rbar_oos', tempoos)) %>% 
    rename(setNames('rbar_insamp', tempsamp)) 
  
} # end inspect_one_pub



# make tables
namelist = c('Size','BMdec','Mom12m')

tabout = lapply(namelist, inspect_one_pub)
names(tabout) = namelist

tabout$allsignals = pubsum2 

# save to disk
write_xlsx(tabout, '../Results/InspectMatch.xlsx')
