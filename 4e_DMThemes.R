# Created 2024 05. Finds spanned and unspanned dm strats, makes 
# themes based off hierarchical clustering using correlation matrix
# tbc

# Setup --------------------------------------------------------

rm(list = ls())

source("0_Environment.R")

library("dplyr")
library("tidyverse")
library("magrittr")
library(doParallel)

# hierarchical clustering
library(ggdendro)
library(dendextend)

## User Settings ------------------------------------------------

# in serial, 300 strats in 6 sec, 30000 in 17 hours
# with 4 cores, 1500 strats in 20 sec, 30000 strats in 2.5 hours
# for 6000 strats, minutes  = 6000^2/1500^2 * 20/60 = 5

# for now, limit to n_dm_for_cor strats sorted by number of sample pub matches
# (drops signals that are t>2 only transiently)
n_dm_for_cor = 300 

# number of cores
ncores = 4

# name of compustat LS file
dmcomp <- list()
dmcomp$name <- paste0('../Data/Processed/',
                      globalSettings$dataVersion, 
                      ' LongShort.RData')

# maximum correlation (signed)
# For ref, cor(ret BMdec, ret diff(at)/lag(at)) = -0.64
maxcor = 0.5

# Settings for correlation computation
# in serial, 300 strats in 6 sec, 30000 in 17 hours
# with 4 cores, 1500 strats in 20 sec, 30000 strats in 2.5 hours
# for 6000 strats, minutes  = 6000^2/1500^2 * 20/60 = 5
# for now, limit to n_dm_for_cor strats sorted by in-samp abs(tstat)
n_dm_for_cor = 6000 

# Plot settings
plotdat <- list()

plotdat$name <- "t_min_2"
plotdat$legprefix = "|t|>2.0"
plotdat$npubmax = Inf
plotdat$use_sign_info = TRUE

plotdat$matchset <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf,
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 1*2, # Default = 0, minimum screened t-stat
  t_max = Inf, # maximum screened t-stat
  t_rankpct_min = 100, # top x% of data mined t-stats, 100% for off
  minNumStocks = globalSettings$minNumStocks
)

# aesthetic settings
fontsizeall = 28
legposall = c(30,15)/100
ylaball = 'Trailing 5-Year Return (bps pm)'
linesizeall = 1.5

## Load data ----------------------------------------------------

# published
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
    filter(Keep) %>%
    setDT()

czcat <- fread("DataInput/SignalsTheoryChecked.csv") %>%
    select(signalname, Year, theory)

czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  left_join(czcat, by = "signalname") %>%
  mutate(ret_scaled = ret / rbar * 100)

## dm signal descriptions ---------------------------------------

# read compustat acronyms
dmdoc = readRDS(dmcomp$name)$signal_list %>%  setDT() 
yzdoc = readxl::read_xlsx('DataInput/Yan-Zheng-Compustat-Vars.xlsx') %>% 
  transmute(acronym = tolower(acronym), longname , shortername ) %>% 
  setDT() 

# merge
dmdoc = dmdoc[ , signal_form := if_else(signal_form == 'diff(v1)/lag(v2)', 'd_', '')] %>% 
  merge(yzdoc[,.(acronym,shortername)], by.x = 'v1', by.y = 'acronym') %>%
  rename(v1long = shortername) %>%
  merge(yzdoc[,.(acronym,shortername)], by.x = 'v2', by.y = 'acronym') %>%
  rename(v2long = shortername) 

# create link table
dm_linktable = expand_grid(sweight = c('ew','vw'), dmname =  dmdoc$signalid) %>% 
  mutate(dmcode = paste0(sweight, '|', dmname))  %>% 
  left_join(dmdoc, by = c('dmname' = 'signalid')) %>%
  mutate(desc = paste0(substr(dmcode,1,3), signal_form, v1long, '/', v2long)
    , shortdesc = paste0(substr(dmcode,1,3), signal_form, v1, '/', v2)) %>% 
  setDT()

rm('dmdoc', 'yzdoc')

# Generate Compustat DM sumstats --------------------------------

print("creating Compustat mining in-sample sumstats")
print("Takes about 4 minutes using 4 cores")
start_time <- Sys.time()
dmcomp$insampsum <- sumstats_for_DM_Strats(
  DMname = dmcomp$name,
  nsampmax = Inf
)
print("finished")
stop_time <- Sys.time()
stop_time - start_time


save.image(file = "first_part.RData")
# Create dmpred: matched dm strats and spanning cat -----------------------

## Select strats -----------------------

# notation follows make_ret_for_plotting function
# not sure it's optimal here
dmpred = list()
dmpred$matched <- SelectDMStrats(dmcomp$insampsum, plotdat$matchset)
dmpred$unique_match = dmpred$matched %>% 
  .[,.(mean_tabs = mean(abs(tstat)), npubmatch = .N), by = c('sweight', 'dmname')]

## Loop -----------------------
# make list of samples
samplist <- czsum %>%
    distinct(sampstart, sampend) %>%
    arrange(sampstart, sampend)

# mark dm strats that are spanned by current pub or previous pub
sampendlist = samplist$sampend %>% sort() %>% unique()
for (i in 1:length(sampendlist)){

  # initialize
  if (i==1) {
    spanned = data.table(); dmpred$matched$spanned_ever = FALSE
  }
  
  # find sweight, dmnames that are spanned now
  spanned_now = dmpred$matched[sampend == sampendlist[i]] %>% 
    filter(sign(rbar)*cor > maxcor) %>% 
    mutate(sampend = sampendlist[i]) %>%
    select(sampend, sweight, dmname) 

  # combine with previous
  spanned = rbind(spanned, spanned_now)

  # mark ew dm strats that have ever been spanned
  badlist_ew = spanned[sampend <= sampendlist[i] & sweight == 'ew']$dmname
  dmpred$matched[sampend == sampendlist[i] & sweight == 'ew'
    , spanned_ever := dmname %in% badlist_ew]

  # mark vw dm strats that have ever been spanned
  badlist_vw = spanned[sampend <= sampendlist[i] & sweight == 'vw']$dmname
  dmpred$matched[sampend == sampendlist[i] & sweight == 'vw'
    , spanned_ever := dmname %in% badlist_vw]
  
} # end for i in 1:length sampendlist

# Compute correlations ---------------------------------------
# used only for the clustering, not for the decay chart

## Data setup --------------------------------------------------
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

# tighten up for leaner correlation computation
dm_rets[, id := paste0(sweight, '|', dmname)][
  , ':=' (sweight = NULL, dmname = NULL)]
setcolorder(dm_rets, c('id', 'yearm', 'ret'))

# filter based on mean(abs(tstat)) for now
# tbc: compute all correlations, perhaps in a different file
stratlist = dmcomp$insampsum[
  min_nstock_long >= 10 & min_nstock_short >= 10 & abs(tstat) > 2
  , .(mean_tabs = mean(abs(tstat)), npubmatch = .N)
  , by = c('sweight', 'dmname')
] %>% 
  mutate(id = paste0(sweight, '|', dmname)) %>%
  select(id, mean_tabs, npubmatch) %>%
  arrange(-mean_tabs) %>% 
  head(n_dm_for_cor) %>% 
  arrange(id)
dm_rets = dm_rets[id %in% stratlist$id]

## Loop --------------------------------------------------

tic = Sys.time()

# set up cluster
cl <- makePSOCKcluster(ncores)
registerDoParallel(cl)
setkey(dm_rets,  id, yearm) # sort
dmcor = foreach(i = 1:(nrow(stratlist)-1), .combine = rbind
  , .packages = c("data.table", "tidyverse", "zoo")
  ) %dopar% {

  if (i %% 100 == 0) {
    print(paste0("correlation for ", i, " of ", nrow(stratlist)))  
  }

  stratcur <- stratlist[i, ]$id

  # add *signed* current return to dm_rets
  # currently sign is based on mean across all samples, potential look ahead bias
  # but it should be minor
  retcur <- dm_rets[id == stratcur, .(yearm, ret)]  
  retcur$retsigned = sign(mean(retcur$ret)) * retcur$ret
  dm_rets[retcur, retcur := i.retsigned, on = .(yearm)]

  # find list of strats that has not been examined yet
  stratneeded = stratlist[(i+1):nrow(stratlist)]

  # compute correlation
  cor_cur = dm_rets[
    id %in% stratneeded$id
    , .(cor = cor(ret, retcur, use = "pairwise"))
    , by = c('id')
  ] %>% 
    mutate(id2 = stratcur) %>%
    transmute(id1 = id, id2, cor)

  # clean up
  dm_rets[ , retcur := NULL]

  return(cor_cur)
} # end dmcor = foreach 
stopCluster(cl)
difftime(Sys.time(), tic, units = 'secs')

## Make matrix form --------------------------------------

# add symmetrical correlations
symcor = dmcor[ , .(id2 = id1, id1= id2, cor)]
dmcor = rbind(dmcor, symcor)

# add self correlations
templist = dmcor$id1 %>% unique()
selfcor = data.table(id1 = templist, id2 = templist, cor = 1)
dmcor = rbind(dmcor, selfcor)

# reshape  into matrix
cmat = dmcor %>% dcast(id1 ~ id2, value.var = 'cor')  
temprowname = cmat$id1
cmat = cmat[ , id1 := NULL]
cmat = as.matrix(cmat)
rownames(cmat) = temprowname

# make alternative cmat with more descriptive names
cmat2 = cmat
temp = data.table(i = 1:nrow(cmat), name = rownames(cmat)) %>% 
    left_join(dm_linktable[,.(dmcode, desc)], by = c('name' = 'dmcode')) %>% 
    arrange(i)
rownames(cmat2) = temp$desc
colnames(cmat2) = temp$desc

# Convenience save ----------------------------------------------
save.image('../Data/tmp_4e_DMThemes.RData')

# Convenience load ----------------------------------------------

load('../Data/tmp_4e_DMThemes.RData')
source('0_Environment.R')

# Plot decay --------------------------------------------------

## make event time returns -----------------------
print("Making spanned accounting event time returns")
print("Can take a few minutes...")
start_time <- Sys.time()
dmpred$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = dmpred$matched[spanned_ever==TRUE]
  , npubmax = plotdat$npubmax, 
  czsum = czsum, use_sign_info = plotdat$use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

plotdat$comp_matched <- dmpred$matched[spanned_ever==TRUE]
plotdat$comp_event_time <- dmpred$event_time

print("Making unspanned accounting event time returns")
print("Can take a few minutes...")
start_time <- Sys.time()
dmpred$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = dmpred$matched[spanned_ever==FALSE]
  , npubmax = plotdat$npubmax, 
  czsum = czsum, use_sign_info = plotdat$use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

plotdat$unspan_matched <- dmpred$matched[spanned_ever==FALSE]
plotdat$unspan_event_time <- dmpred$event_time

# join and reformat for plotting function
ret_for_plotting <- czret %>%
    transmute(pubname = signalname, eventDate, ret = ret_scaled) %>%
    left_join(
      plotdat$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
    ) %>%
    left_join(
      plotdat$unspan_event_time %>% transmute(pubname, eventDate, matchRetAlt = dm_mean)
    ) %>%
    select(eventDate, ret, matchRet, matchRetAlt, pubname) %>%
    # keep only rows where both matchrets are observed
    filter(!is.na(matchRet) & !is.na(matchRetAlt))

## actually plot ----------------------------------------------
printme = ReturnPlotsWithDM(
  dt = ret_for_plotting, 
  basepath = "../Results/Fig_DM",
  suffix = 'unspan',
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  legendlabels =
    c(
      paste0("Published"),
      paste0("Cor > 0.5 w/ a current or previous pub"),
      paste0("Cor < 0.5 w/ all current and previous pubs")
    ),
  legendpos = c(45,20)/100,
  fontsize = fontsizeall,
  yaxislab = ylaball,
  linesize = linesizeall
)

# Describe spanning over time -----------------------------------

tab_span = dmpred$matched %>% 
  distinct(sampend, sweight, dmname, spanned_ever) %>%
  .[ , .(n_dm_tg2 = .N, n_span = sum(spanned_ever)
          , n_unspan = sum(!spanned_ever)
          , pct_unspan = 100*mean(!spanned_ever))
    , by = c('sampend')] %>% 
  arrange(sampend) 

# tbc: make a nice table pls
tab_span %>% as_tibble() %>% print(n = 100)

# Describe spanned in sampend = 1990 (FF1993) ----------------------
ycur = 1990

# pull list of spanned dm strats
dmcodelist = dmpred$matched %>% 
  .[floor(sampend) == ycur & spanned_ever == TRUE
    , .(sweight, dmname)] %>% 
  distinct(sweight, dmname) %>% 
  merge(dm_linktable, by = c('sweight', 'dmname')) %>% 
  pull(dmcode)

# extract correlation matrix
lookup = data.table(i = 1:nrow(cmat), name = rownames(cmat))
rowlist = lookup %>% filter(name %in% dmcodelist) %>% pull(i)  
cmatcur = cmat[rowlist, rowlist]

## make hierarchical cluster hc --------------------------
# careful, scales O(n^3)

tic = Sys.time()

# JKP use dist = 1-cor and Ward 1963 linkage citing Murtagh Legendre 2014
# but the default complete method seems similar
hc = dist(1-cmatcur) %>% hclust(method = 'ward.D')  
dend = as.dendrogram(hc)
toc = Sys.time()
difftime(toc, tic, units = 'secs')

## Create table of clusters ------------------------------
nclustermax = 20

# find height that gives < nclustermax
heights = floor(seq(1, floor(max(hc$height)/2), length.out = 500)) %>% 
    unique() 
ngrouplist = sapply(heights, function(h) length(unique(cutree(hc, h = h))))
hstar = heights[ngrouplist <= nclustermax][1]

# cut tree 
sub_grp = cutree(dend, h=hstar)

# create name_cluster data table
name_cluster = data.table(dmcode = names(sub_grp), cluster = sub_grp) %>% 
  left_join(dm_linktable[,.(dmcode, shortdesc, desc)], by='dmcode') %>%
  left_join(
    dmpred$matched[floor(sampend) == ycur]  %>% 
    mutate(dmcode = paste0(sweight, '|', dmname)) %>% 
    group_by(dmcode) %>% summarize(mean_tstat = mean(tstat)) %>% ungroup() %>% 
    select(dmcode, mean_tstat)
    , by='dmcode') %>% 
  arrange(cluster, dmcode) 

# tbc: make a nice table describing strats, including examples
name_cluster %>% group_by(cluster) %>% summarize(nstrat = n()) %>% 
  left_join(
    name_cluster %>% group_by(cluster) %>% arrange(cluster, -abs(mean_tstat)) %>%
      slice(1) 
    , by = 'cluster'
  ) %>% 
  print(n = 20) %>% 
  transmute(cluster, nstrat, example_dm = dmcode, example_desc = desc, mean_tstat)

# Describe unspanned in sampend = 1990 (FF1993) ----------------------
ycur = 1990

# pull list of spanned dm strats
dmcodelist = dmpred$matched %>% 
  .[floor(sampend) == ycur & spanned_ever == FALSE
    , .(sweight, dmname)] %>% 
  distinct(sweight, dmname) %>% 
  merge(dm_linktable, by = c('sweight', 'dmname')) %>% 
  pull(dmcode)

# extract correlation matrix
lookup = data.table(i = 1:nrow(cmat), name = rownames(cmat))
rowlist = lookup %>% filter(name %in% dmcodelist) %>% pull(i)  
cmatcur = cmat[rowlist, rowlist]

## make hierarchical cluster hc --------------------------
# careful, scales O(n^3)
nrow(cmatcur)

tic = Sys.time()
# JKP use dist = 1-cor and Ward 1963 linkage citing Murtagh Legendre 2014
# but the default complete method seems similar
hc = dist(1-cmatcur) %>% hclust(method = 'ward.D')  
dend = as.dendrogram(hc)
toc = Sys.time()
difftime(toc, tic, units = 'secs')

## Create table of clusters ------------------------------
nclustermax = 20

# find height that gives < nclustermax
heights = floor(seq(1, floor(max(hc$height)/2), length.out = 500)) %>% 
    unique() 
ngrouplist = sapply(heights, function(h) length(unique(cutree(hc, h = h))))
hstar = heights[ngrouplist <= nclustermax][1]

# cut tree 
sub_grp = cutree(dend, h=hstar)

# create name_cluster data table
name_cluster = data.table(dmcode = names(sub_grp), cluster = sub_grp) %>% 
  left_join(dm_linktable[,.(dmcode, shortdesc, desc)], by='dmcode') %>%
  left_join(
    dmpred$matched[floor(sampend) == ycur]  %>% 
    mutate(dmcode = paste0(sweight, '|', dmname)) %>% 
    group_by(dmcode) %>% summarize(mean_tstat = mean(tstat)) %>% ungroup() %>% 
    select(dmcode, mean_tstat)
    , by='dmcode') %>% 
  arrange(cluster, dmcode) 

# tbc: make a nice table describing strats, including examples
name_cluster %>% group_by(cluster) %>% summarize(nstrat = n()) %>% 
  left_join(
    name_cluster %>% group_by(cluster) %>% arrange(cluster, -abs(mean_tstat)) %>%
      slice(1) 
    , by = 'cluster'
  ) %>% 
  print(n = 20) %>% 
  transmute(cluster, nstrat, example_dm = dmcode, example_desc = desc, mean_tstat)

