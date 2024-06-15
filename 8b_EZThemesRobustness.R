# Created 2024 06: super ez themes
# Setup --------------------------------------------------------

rm(list = ls())
set.seed(123) 

source("0_Environment.R")
library(doParallel)

## User Settings ------------------------------------------------

# define predictor
pred_min_tabs = 2 # min abs(tstat)
pred_top_n = Inf # min t-stat rank

# number of cores
ncores = 4

# min data requirements
nstock_min = 10
nmonth_min = 120

# sample periods
# Stattman publishes B/M in 1980, seems like a good place to start
insamp = tibble(
  start = as.yearmon('Jul 1963')
  , end = as.yearmon('Dec 1990')
)

# splitting in 2004 is informative
oos1 = tibble(
  start = as.yearmon('Jan 1991')
  , end = as.yearmon('Dec 2004')
)

oos2 = tibble(
  start = as.yearmon('Jan 2005')
  , end = as.yearmon('Dec 2022')
)

# list of anomalies for measuring spanning
pubselect = c('BMdec','Size','Mom12m', 'AssetGrowth', 'GP')
# pubselect = c('BMdec','Size','Mom12m')

# name of compustat LS file
dmcomp <- list()
dmcomp$name <- paste0('../Data/Processed/'
  , globalSettings$dataVersion, ' LongShort.RData')

# Data load -----------------------------------------------------

tic0 = Sys.time()

## Load CZ ----------------------------------------------------

# published
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
    filter(Keep) %>%
    setDT()

czcat <- fread("DataInput/SignalsTheoryChecked.csv") %>%
    select(signalname, Year, theory)

czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  left_join(czcat, by = "signalname") %>%
  mutate(ret_scaled = ret / rbar * 100)

## Load DM --------------------------------------------------
# read in DM strats (only used in this section)
DMname <- paste0(
    "../Data/Processed/",
    globalSettings$dataVersion,
    " LongShort.RData"
)
dm_rets <- readRDS(DMname)$ret
dm_info <- readRDS(DMname)$port_list

dm_rets <- dm_rets %>%
    left_join(
        dm_info %>% select(portid, sweight),
        by = c("portid")
    ) %>%
    transmute(
        sweight,
        dmname = signalid, yearm, ret, nstock_long, nstock_short
    ) %>%
    setDT()

# tighten up for leaner computation
dm_rets[, id := paste0(sweight, '|', dmname)][
  , ':=' (sweight = NULL, dmname = NULL)]
setcolorder(dm_rets, c('id', 'yearm', 'ret'))  

## Load signal docs --------------------------------------------

# wrap in function for easy editing of xlsx
import_docs = function(){
  # read compustat acronyms
  dmdoc = readRDS(dmcomp$name)$signal_list %>%  setDT() 
  yzdoc = readxl::read_xlsx('DataInput/Updated_Yan-Zheng-Compustat-Vars.xlsx') %>% 
    transmute(acronym = tolower(acronym), shortername ) %>% 
    setDT() 

  # merge
  dmdoc = dmdoc[ 
    , signal_form := if_else(signal_form == 'diff(v1)/lag(v2)', 'd_', '')] %>% 
    merge(yzdoc[,.(acronym,shortername)], by.x = 'v1', by.y = 'acronym') %>%
    rename(v1long = shortername) %>%
    merge(yzdoc[,.(acronym,shortername)], by.x = 'v2', by.y = 'acronym') %>%
    rename(v2long = shortername) 

  # create link table
  dm_linktable = expand_grid(sweight = c('ew','vw'), dmname =  dmdoc$signalid) %>% 
    mutate(dmcode = paste0(sweight, '|', dmname))  %>% 
    left_join(dmdoc, by = c('dmname' = 'signalid')) %>%
    mutate(shortdesc = paste0(substr(dmcode,1,3), signal_form, v1, '/', v2)
      , desc = if_else(signal_form=='d_'
        , paste0('d_[', v1long, ']/lag[', v2long, ']')
        , paste0('[', v1long, ']/[', v2long, ']')
    )) %>% 
    setDT()

  return(dm_linktable)

} # end import_docs
dm_linktable = import_docs()

# Create dmpred: data for data mined predictors ------------------------
dmpred = list()

# select predictors, summarize in-samp, sign
dmpred$sum = dm_rets[yearm <= insamp$end & yearm >= insamp$start &
                       nstock_long>=nstock_min & nstock_short>=nstock_min, ] %>% 
  .[, .(rbar=mean(ret), tstat=mean(ret)/sd(ret)*sqrt(.N)
    , nmonth=.N), by=id] %>% 
    mutate(sign = sign(rbar)) %>% 
    transmute(id, sign, rbar=sign*rbar, tstat=sign*tstat, nmonth) %>% 
    filter(nmonth >= nmonth_min) 

# get signed returns
dmpred$ret = merge(dm_rets, dmpred$sum, by=c('id')) %>% 
  filter(nstock_long>=nstock_min & nstock_short>=nstock_min) %>%
  mutate(ret_signed = sign*ret) %>% 
  select(id, yearm, ret_signed, starts_with('nstock')) 

# add OOS ret
dmpred$sum = dmpred$sum %>% 
  merge(dmpred$ret[yearm >= oos1$start & yearm <= oos1$end
      , .(rbaroos = mean(ret_signed)), by = 'id']
    , by = 'id') 

dmpred$sum = dmpred$sum %>% 
  merge(dmpred$ret[yearm >= oos2$start & yearm <= oos2$end
      , .(rbaroos2 = mean(ret_signed)), by = 'id']
    , by = 'id')     

# add spanning by lit
temppub = czret %>% 
    filter(signalname %in% pubselect) %>%
    transmute(pubname=signalname,yearm=date,pubret=ret) %>% 
    dcast(yearm ~ pubname, value.var = 'pubret') 

modelname = paste0('ret_signed ~ ', paste(pubselect, collapse = ' + ')) 
temprsq = dmpred$ret %>% 
  merge(temppub, by='yearm') %>% 
  .[yearm >= insamp$start & yearm <= insamp$end
  , .(rsq_lit = summary(lm(as.formula(modelname), data=.SD))$r.squared)
  , by = 'id']

dmpred$sum = dmpred$sum %>% merge(temprsq, by = 'id')

# Define themes ---------------------------------------------------

# for easy editing of xlsx
dm_info = import_docs()

stratsum = dmpred$sum %>% 
  merge(dm_info %>% 
    transmute(id=dmcode
    , v1
    , numer = paste0(signal_form, v1long)
    , denom = v2long)    
  , by = 'id') %>% 
  mutate(sweight=substr(id,1,2)
   , numer = str_replace_all(numer, 'd_','$\\\\Delta$')
   , numer = str_replace_all(numer, '&','\\\\&'))

# stats by sweight-numer group
groupsum = stratsum %>% 
  group_by(sweight, v1, numer) %>%
  summarize(
    nstrat = n()
    , pctshort = round(mean(sign==-1)*100, 0)
    , pctsignif = round(mean(tstat>1.96)*100, 0)
    , tstat = round(mean(tstat), 1)
    , rbar = mean(rbar)
    , rbaroos = mean(rbaroos)
    , rbaroos2 = mean(rbaroos2)
    , rbaroos_rbar = mean(rbaroos)/mean(rbar)
    , rsqlit = round(mean(rsq_lit)*100, 0)
    , .groups = 'drop'
  ) %>% 
  arrange(-tstat) %>% 
  mutate(rank = row_number()) 

# save groupsum to csv for manual categorization (if needed)
# (this gets copy-pasted into DataInput/DM-Numerator-LitCat.xlsx)
if (FALSE) {
  groupsum %>% distinct(v1, numer) %>%  write_csv('../results/numer_list.csv')
}

# themes are then the top 20 groups by t-stat
themesum = groupsum %>% 
  arrange(-tstat) %>% 
  head(20) 

# check to console ------------------------------------------------
themesum %>% select(numer, pctshort, tstat, rbar)

# tbc: output to tables for appendix