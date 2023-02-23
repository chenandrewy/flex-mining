
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
  , .(rbar = mean(ret), n = .N, t = mean(ret)/sd(ret)*sqrt(.N))
  , by = c('source','actSignal','candSignalname','samptype')
]


pubsum = candsum[
  , .(rbar = mean(rbar), n = mean(n), t = mean(t))
  , by = c('source','actSignal','samptype')
] %>% 
  select(source,actSignal, samptype, rbar) %>% 
  pivot_wider(
    id_cols = c('source','actSignal'), names_from = samptype, values_from = rbar
  ) %>% 
  arrange(actSignal,desc(source)) %>%
  left_join(
    czdat$czsum %>% transmute(actSignal = signalname, theory1, Authors, Year)
  ) %>% 
  arrange(theory1, actSignal, desc(source)) %>% 
  mutate(across(where(is.numeric), round, 2))


pubsum %>% print(n = 200)


# Inspect select predictors ------------------------------------------------------------------


# read compvars doc
compdoc = readxl::read_xlsx('compustat-definitions.xlsx') %>% 
  janitor::clean_names() %>% 
  transmute(
    shortname = tolower(ccm_item_name)
    , longname = shorter_description
  ) %>% 
  distinct(shortname, .keep_all = T)

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
      , signal_form = str_replace(signal_form, 'pdiff', '%$\\\\Delta$')    
      , signal_form = str_replace(signal_form, 'diff', '$\\\\Delta$')    
    ) %>% 
    left_join(
      compdoc %>% transmute(v1 = shortname, v1long = substr(longname,1,23))
    ) %>% 
    left_join(
      compdoc %>% transmute(v2 = shortname, v2long = substr(longname,1,20))
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

# save to disk
write_xlsx(tabout, '../Results/InspectMatch.xlsx')
