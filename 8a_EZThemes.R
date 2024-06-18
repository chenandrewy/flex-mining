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
  , end = as.yearmon('Dec 1980')
)

# splitting in 2004 is informative
oos1 = tibble(
  start = as.yearmon('Jan 1981')
  , end = as.yearmon('Dec 2004')
)

oos2 = tibble(
  start = as.yearmon('Jan 2005')
  , end = as.yearmon('Dec 2022')
)

# list of anomalies for measuring spanning
pubselect = c('BM', 'Beta', 'DivYieldST', 'EP', 'Price') # stuff published before 1980
# pubselect = c('BMdec','Size','Mom12m', 'AssetGrowth', 'GP')
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
    , signal_form
    , numer = paste0(signal_form, v1long)
    , denom = v2long)    
  , by = 'id') %>% 
  mutate(sweight=substr(id,1,2)
   , numer = str_replace_all(numer, 'd_','$\\\\Delta$')
   , numer = str_replace_all(numer, '&','\\\\&'))

# stats by sweight-numer group
groupsum = stratsum %>% 
  group_by(sweight, signal_form, v1, numer) %>%
  summarize(
    nstrat = n()
    , pctshort = mean(sign==-1)*100
    , pctsignif = mean(tstat>1.96)*100
    , tstat = mean(tstat)
    , rbar = mean(rbar)
    , rbaroos = mean(rbaroos)
    , rbaroos2 = mean(rbaroos2)
    , rbaroos_rbar = mean(rbaroos)/mean(rbar)
    , rsqlit = mean(rsq_lit)*100
    , .groups = 'drop'
  ) %>% 
  arrange(-tstat) %>%     
  mutate(rank = row_number())  

# save groupsum to csv for manual categorization (if needed)
# (this gets copy-pasted into DataInput/DM-Numerator-LitCat.xlsx)
if (FALSE) {
  groupsum %>% distinct(signal_form, v1, numer) %>%  write_csv('../results/numer_list.csv')
}

# Make table ----------------------------------------

library(kableExtra)

# import literature categories
litinfo = tibble(
  litcat0=c('investment', 'diff investment', 'external financing', 'accruals'
    , 'diff profitability', 'debt structure')
) %>% mutate(order = row_number()) %>% 
  mutate(litcat = paste(order, litcat0, sep='|')) %>% 
  select(-order)

# merge diff investment (cleaner table)
litinfo = litinfo %>% 
  mutate(litcat = if_else(litcat=='2|diff investment', '1|investment', litcat))

groupsumcat = readxl::read_xlsx('DataInput/DM-Numerator-LitCat.xlsx') %>% 
  transmute(signal_form, v1, numer, litcat0 = LitCat) %>% 
  left_join(litinfo, by = 'litcat0') 

# sketch table
tab1 = groupsum %>% 
  arrange(-tstat) %>%
  head(20) %>% 
  mutate(blank1 = '') %>% 
  left_join(groupsumcat %>% select(signal_form, v1, litcat)
    , by = c('signal_form', 'v1')) %>% 
  transmute(litcat
    , group = paste0(numer, ' (', sweight, ')')
    , pctshort = round(pctshort, 0)
    , tstat = round(tstat, 1)
    , rbar = round(rbar, 2)
    , blank1
    , decay1=round(1*((rbaroos)/rbar), 2)
    , decay2=round(1*((rbaroos2)/rbar), 2)) %>% 
  arrange(litcat, -tstat) 

# add blank rows for litcats
tab2 = tab1 %>% 
  bind_rows(
    tab1 %>% distinct(litcat) %>% mutate(tstat = Inf, group = litcat)
  ) %>% 
  arrange(litcat, -tstat) %>% 
  select(-litcat) %>% 
  print()

# export to temp.tex
tab2 %>% 
  kable('latex', booktabs = T, linesep='', escape=F, digits=2) %>% 
  cat(file='../results/temp.tex')

# Make it beautiful ----------------------------------------

# setup
tex = readLines('../results/temp.tex')
mcol = function(x) paste0('\\multicolumn{1}{c}{', x, '}')
strsamp = paste0(year(insamp$start), '-', year(insamp$end))
stroos1 = paste0(year(oos1$start), '-', year(oos1$end))
stroos2 = paste0(year(oos2$start), '-', year(oos2$end))
lhead = function(x) paste0('\\multicolumn{', ncol(tab2), '}{l}{', x, '} \\\\ \\hline')

# expand the header
tex = tex %>% append('', after=4) %>% append('', after=5)

tex[4] = paste(
  ''
  , paste0('\\multicolumn{3}{c}{', strsamp, ' (IS)}')
  , '' # blank
  , mcol(stroos1)
  , paste0(mcol(stroos2), ' \\\\ \\cmidrule{2-4} \\cmidrule{6-7}')
  , sep=' & ')

tex[5] = paste(
  'Numerator (Stock Weight)'
  , mcol('Pct'), '\\multirow{2}{*}{t-stat}', mcol('Mean')
  , '' # blank  
  , '\\multicolumn{2}{c}{Mean Return} \\\\  '
  , sep = ' & '
)

tex[6] = paste(
  ''
  , mcol('Short'), '', mcol('Return')
  , '' # blank  
  , '\\multicolumn{2}{c}{OOS / IS} \\\\ '
  , sep = ' & '
)

# add litcat subheaders
subheadstr = list(
  'Investment / Investment Growth (Titman, Wei, Xie 2004; Cooper, Gulen, Schill 2008)'
  , 'External Financing (Spiess and Affleck-Graves 1999; Pontiff and Woodgate 2008)'
  , 'Accruals / Inventory Growth (Sloan 1996; Thomas and Zhang 2002; Belo and Lin 2012)'
  , 'Earnings Surprise (Foster, Olsen, Shevlin 1984; Chan, Jegadeesh, Lakonishok 1996)'
  , 'Debt Structure (Valta 2016)'
)

# find subhead rows
tex1 = tex
subheadrow = which(grepl('NA & Inf', tex1))
for (i in 1:length(subheadrow)){
  tex1[subheadrow[i]] = lhead(subheadstr[i])

  # extra spacing
  if (i > 1){
    j = subheadrow[i]-1
    n = nchar(tex1[j])
    tex1[j] = paste0(substr(tex1[j], 1, (n-2)), ' \\bigstrut[b] \\\\ ')
  }
}


writeLines(tex1, '../results/theme_ez_decay.tex')

# copy to overleaf (if on Andrew's machine)
if (any(grepl('ayc', Sys.info()))) {
  writeLines(tex1, 'D:/Dropbox/Apps/Overleaf/PeerReviewedTheory_Paper/exhibits/theme_ez_decay.tex')
}

# # Make table w/ rsq ----------------------------------------

# library(kableExtra)

# # import literature categories
# litinfo = tibble(
#   litcat0=c('investment', 'diff investment', 'external financing', 'accruals'
#     , 'diff profitability', 'debt structure')
# ) %>% mutate(order = row_number()) %>% 
#   mutate(litcat = paste(order, litcat0, sep='|')) %>% 
#   select(-order)

# # merge diff investment (cleaner table)
# litinfo = litinfo %>% 
#   mutate(litcat = if_else(litcat=='2|diff investment', '1|investment', litcat))

# groupsumcat = readxl::read_xlsx('DataInput/DM-Numerator-LitCat.xlsx') %>% 
#   transmute(numer, litcat0 = LitCat) %>% 
#   left_join(litinfo, by = 'litcat0') 

# # sketch table
# tab1 = themesum %>% 
#   mutate(blank1 = '') %>% 
#   left_join(groupsumcat %>% select(numer, litcat)
#     , by = c('numer')) %>% 
#   transmute(litcat, numer, pctshort, tstat,  rbar, rsqlit
#     , blank1
#     , decay1=round(1*((rbaroos)/rbar), 2)
#     , decay2=round(1*((rbaroos2)/rbar), 2)) %>% 
#   arrange(litcat, -tstat) %>% 
#   print()

# # add blank rows for litcats
# tab2 = tab1 %>% 
#   bind_rows(
#     litinfo %>% filter(!is.na(order)) %>% distinct(litcat) %>% mutate(tstat = Inf, numer = litcat)
#   ) %>% 
#   arrange(litcat, -tstat) %>% 
#   select(-litcat) %>% 
#   print()

# # export to temp.tex
# tab2 %>% 
#   kable('latex', booktabs = T, linesep='', escape=F, digits=2) %>% 
#   cat(file='../results/temp.tex')

# # Make rsq table beautiful ----------------------------------------

# # setup
# tex = readLines('../results/temp.tex')
# mcol = function(x) paste0('\\multicolumn{1}{c}{', x, '}')
# strsamp = paste0(year(insamp$start), '-', year(insamp$end))
# stroos1 = paste0(year(oos1$start), '-', year(oos1$end))
# stroos2 = paste0(year(oos2$start), '-', year(oos2$end))
# lhead = function(x) paste0('\\multicolumn{', ncol(tab2), '}{l}{', x, '} \\\\ \\hline')

# # expand the header
# tex = tex %>% append('', after=4) %>% append('', after=5)

# tex[4] = paste(
#   ''
#   , paste0('\\multicolumn{4}{c}{', strsamp, ' (IS)}')
#   , '' # blank
#   , mcol(stroos1)
#   , paste0(mcol(stroos2), ' \\\\ \\cmidrule{2-5} \\cmidrule{7-8}')
#   , sep=' & ')

# tex[5] = paste(
#   'Numerator of Ratio'
#   , mcol('Pct'), '\\multirow{2}{*}{t-stat}', mcol('Mean'), mcol('Prev Lit')
#   , '' # blank  
#   , '\\multicolumn{2}{c}{Mean Return} \\\\  '
#   , sep = ' & '
# )

# tex[6] = paste(
#   ''
#   , mcol('Short'), '', mcol('Return'), mcol('$R^2$')
#   , '' # blank  
#   , '\\multicolumn{2}{c}{OOS / IS} \\\\ '
#   , sep = ' & '
# )

# # add litcat subheaders
# tex[8] = lhead('Investment / Investment Growth (Titman, Wei, Xie 2004; Cooper, Gulen, Schill 2008)') 
# tex[15] = lhead('External Financing (Spiess and Affleck-Graves 1999; Pontiff and Woodgate 2008)')
# tex[20] = lhead('Accruals / Inventory Growth (Sloan 1996; Thomas and Zhang 2002; Belo and Lin 2012)')
# tex[26] = lhead('Earnings Surprise (Foster, Olsen, Shevlin 1984; Chan, Jegadeesh, Lakonishok 1996)')
# tex[31] = lhead('Debt Structure (Valta 2016)')

# for (i in c(15, 20, 26, 31)) {
#   j = i-1
#   n = nchar(tex[j])
#   tex[j] = paste0(substr(tex[j], 1, (n-2)), ' \\bigstrut[b] \\\\ ')
# }

# writeLines(tex, '../results/theme_ez_decay_rsq.tex')

# # copy to overleaf (if on Andrew's machine)
# if (any(grepl('ayc', Sys.info()))) {
#   writeLines(tex, 'D:/Dropbox/Apps/Overleaf/PeerReviewedTheory_Paper/exhibits/theme_ez_decay_rsq.tex')
# }





# # Check some stuff --------------------------------------------

# groupsum %>% transmute(sweight, numer, rank, tstat
#   , oosf1 = rbaroos/rbar, oosf2 = rbaroos2/rbar) %>% 
#   filter(tstat > 1.9) %>% 
#   print(n=Inf)

# # where are valuations?
# groupsum %>% select(sweight, v1, numer, pctshort, tstat, rank) %>% 
#   filter(grepl('Market', numer)) %>% print()

# # profitability?
# groupsum %>% select(sweight, v1, numer, pctshort, tstat, rank) %>% 
#   filter(grepl('prof', numer)) %>% print()

# # post-2004 decay
# themesum %>% 
#   mutate(oosfrac = rbaroos2/rbar) %>% 
#   summarize(mean(oosfrac))

# themesum$rsqlit %>% mean()





