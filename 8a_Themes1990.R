# Created 2024 06: trying to simplify the themes by focusing
# on 1990 sample end
# takes about 3 min
# Setup --------------------------------------------------------

rm(list = ls())

source("0_Environment.R")
library(doParallel)

## User Settings ------------------------------------------------

# keep n_dm_total strats by largest tstats
# 29000*2 * 0.05 = 2,900 strats matches Fig 2
n_dm_total = 3000

# number of cores
ncores = 4

# name of compustat LS file
dmcomp <- list()
dmcomp$name <- paste0('../Data/Processed/'
  , globalSettings$dataVersion, ' LongShort.RData')

# min stock in each leg
nstock_min = 10

sampcur = tibble(
  sampstart = as.yearmon('Jul 1963')
  , sampend = as.yearmon('Dec 1990')
)

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
  yzdoc = readxl::read_xlsx('DataInput/Yan-Zheng-Compustat-Vars.xlsx') %>% 
    transmute(acronym = tolower(acronym), longname , shortername ) %>% 
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

# Filter dm to predictors, sign, add OOS ret ------------------------
dmpred = list()

dmpred$sum = dm_rets[yearm <= sampcur$sampend & nstock_long>=nstock_min & nstock_short>=nstock_min, ] %>% 
  .[, .(rbar=mean(ret), tstat=mean(ret)/sd(ret)*sqrt(.N)
    , nmonth=.N), by=id] %>% 
    arrange(-abs(tstat)) %>%
    filter(abs(tstat)> 2, nmonth >= 120) %>% 
    slice(1:n_dm_total)

dmpred$ret = merge(dm_rets, dmpred$sum, by=c('id')) %>% 
  filter(nstock_long>=nstock_min & nstock_short>=nstock_min) %>%
  mutate(ret_signed = sign(rbar)*ret) %>% 
  select(id, yearm, ret_signed) 

# add OOS ret
dmpred$sum = dmpred$sum %>% 
  merge(dmpred$ret[yearm >  sampcur$sampend, .(rbaroos_sign = mean(ret_signed)), by = 'id']
    , by = 'id') %>% 
  mutate(rbaroos = rbaroos_sign * sign(rbar)) %>%
  select(-rbaroos_sign)

# Functions for clustering ---------------------------------------

make_cmat = function(stratlist, sampstart, sampend){
  # stratlist is a dt with columns (id ~ sweight|dmname)
  # crops and signs dm_rets using sampstart, sampend

  # copy predictor returns
  dmpred_ret = copy(dmpred$ret)  

  # set up cluster
  cl <- makePSOCKcluster(ncores)
  registerDoParallel(cl)
  setkey(dmpred_ret,  id, yearm) # sort
  ## Loop --------------------------------------------------
  print(paste0('Computing correlations for nstrat = ', nrow(stratlist)))
  tic = Sys.time()
  dmcor = foreach(i = 1:(nrow(stratlist)-1), .combine = rbind
    , .packages = c("data.table", "tidyverse", "zoo")
    ) %dopar% {

    if (i %% 100 == 0) {
      print(paste0("correlation for ", i, " of ", nrow(stratlist)))  
    }

    stratcur <- stratlist[i, ]$id

    # add current signed return to dmpred_ret
    retcur <- dmpred_ret[id == stratcur, .(yearm, ret_signed)]  
    dmpred_ret[retcur, retcur := i.ret_signed, on = .(yearm)]

    # find list of strats that has not been examined yet
    stratneeded = stratlist[(i+1):nrow(stratlist)]

    # compute correlation
    cor_cur = dmpred_ret[
      id %in% stratneeded$id
      , .(cor = cor(ret_signed, retcur, use = "pairwise"))
      , by = c('id')
    ] %>% 
      mutate(id2 = stratcur) %>%
      transmute(id1 = id, id2, cor)

    return(cor_cur)
  } # end dmcor = foreach 
  stopCluster(cl)
  print(difftime(Sys.time(), tic, units = 'secs'))

  ## Make matrix form --------------------------------------

  # add symmetrical correlations
  symcor = dmcor[ , .(id2 = id1, id1= id2, cor)]
  dmcor = rbind(dmcor, symcor)

  # add self correlations
  templist = dmcor$id1 %>% unique()
  selfcor = data.table(id1 = templist, id2 = templist, cor = 1)
  dmcor = rbind(dmcor, selfcor)

  # reshape  into matrix
  cmat_id = dmcor %>% dcast(id1 ~ id2, value.var = 'cor')  
  temprowname = cmat_id$id1
  cmat_id = cmat_id[ , id1 := NULL]
  cmat_id = as.matrix(cmat_id)
  rownames(cmat_id) = temprowname

  # make alternative cmat with more descriptive names
  cmat_desc = cmat_id
  temp = data.table(i = 1:nrow(cmat_id), name = rownames(cmat_id)) %>% 
      left_join(dm_linktable[,.(dmcode, desc)], by = c('name' = 'dmcode')) %>% 
      arrange(i)
  rownames(cmat_desc) = temp$desc
  colnames(cmat_desc) = temp$desc

  return(list(id = cmat_id, desc = cmat_desc))

} # end make_cmat

select_cluster = function(hc, nclustermax = 20, mean_nstrat_min = NA){
  # hc is a hclust object

  # find height that gives < nclustermax
  heights = floor(seq(1, floor(max(hc$height)/2), length.out = 500)) %>% 
      unique() 

  df = tibble(h = heights, ngroup = NA, meannstrat = NA)
  for (i in 1:nrow(df)){
    temp = cutree(hc, h = df$h[i])
    df[i, ]$ngroup = length(unique(temp))
    df[i, ]$meannstrat = mean(table(temp))
  }
  
  if (is.na(mean_nstrat_min)){
    # keep cutting until ngroup <= nclustermax
    hstar = df %>% filter(ngroup <= nclustermax) %>% 
      arrange(h) %>% head(1) %>% pull(h) 
  } else {
    # find cutting until meannstrat > mean_nstrat_min
    hstar = df %>% filter(meannstrat > mean_nstrat_min) %>% 
      arrange(h) %>% head(1) %>% pull(h)
  }

  # cut tree 
  sub_grp = cutree(hc, h=hstar)

  # create cluster data table
  clust = data.table(dmcode = names(sub_grp), cluster = sub_grp) 

  return(clust)
} # end make_cluster

# Crop to top n_dm_total by group for desired sample ----------------------

# make dt of dm predictors for sampcur
dm_ycur = dmpred$sum %>% 
  arrange(-abs(tstat)) %>%
  filter(row_number() <= n_dm_total) %>% 
  ungroup() %>% 
  setDT() 

# Find correlations --------------------------------
# 1000 strats takes 45 sec (2x1000 => 90 sec)

cmat = make_cmat(dm_ycur
  , sampstart=sampcur$sampstart, sampend=sampcur$sampend)

# Find hierarchical clusters --------------------------------

hc = dist(1-cmat$id) %>% hclust(method = 'ward.D')  

toc0 = Sys.time()
print(paste0('total time = ', difftime(toc0, tic0, units = 'mins')))

# read(reread) dm signal descriptions ---------------------------------------

# for easy editing of xlsx
dm_linktable = import_docs()

# Select clusters and describe ---------------------------------------
nclustselect = 20
# meannstratselect = 10

clust = list()

# select clusters
clust$def = select_cluster(hc, nclustermax = nclustselect)
tempret = merge(dmpred$ret, clust$def, by.x = 'id', by.y = 'dmcode')

# generate cluster portfolios
clust$ret = tempret %>% 
  .[, .(ret=mean(ret_signed), nstrat=.N), by = c('yearm', 'cluster')] 

# cluster performance
# tbc: clean up nstrat filter. Right now it's needed to clean up
# noise in a single cluster's returns in just a couple years of
# data (which leads to negative in-sample returns that are not 
# representative)

clust$ret[cluster == 16] %>% 
  as_tibble() %>%
  print(n=40)

clust$perf = clust$ret %>% 
  mutate(samp=if_else(yearm >= sampcur$sampstart & yearm <= sampcur$sampend, 'insamp', 'oos')) %>%
  filter(nstrat >= 20) %>%
  .[floor(yearm)>= 1963 & nstrat >= 20 , 
  .(nstrat = mean(nstrat), rbar=mean(ret), tstat=mean(ret)/sd(ret)*sqrt(.N)
    , nmonth=.N), by = c('cluster', 'samp')]  %>% 
    arrange(cluster,samp)

# correlation with selected signals
pubselect = c('BMdec','Size','Mom12m')
temppub = czret %>% 
    filter(signalname %in% pubselect) %>%
    transmute(pubname=signalname,yearm=date,pubret=ret) %>% 
    dcast(yearm ~ pubname, value.var = 'pubret') 
clust$cor = clust$ret %>% 
    merge(temppub, by='yearm') %>% 
    filter(yearm >= sampcur$sampstart & yearm <= sampcur$sampend)   %>% 
  group_by(cluster) %>%
  summarize(
    cor_BMdec = cor(ret, BMdec)
    , cor_Size = cor(ret, Size)
    , cor_Mom12m = cor(ret, Mom12m, use = 'pairwise')
    , model = list(lm(ret ~ BMdec + Size + Mom12m))
    , rsq = summary(model[[1]])$r.squared
    ) %>%  
  arrange(-rsq) %>% 
  setDT()

# cluster representatives
cdat = foreach(i = 1:max(clust$def$cluster), .combine = rbind) %do% {
  print(paste0('correlations within cluster ', i))
  tempwide = tempret[cluster==i 
    & yearm >= sampcur$sampstart & yearm <= sampcur$sampend] %>% 
    dcast(yearm ~ id, value.var = 'ret_signed') %>% 
    select(-yearm) %>% 
    as.matrix() 
  tempcor = cor(tempwide, use = 'pairwise') %>% colMeans() 
  tempdat = data.table(id = names(tempcor), meancor = tempcor
    , cluster = i) %>% 
    arrange(-meancor) 
} # end foreach

clust$rep = cdat %>% arrange(cluster, -meancor) %>% 
  group_by(cluster) %>% slice(1) %>% ungroup() %>% 
  merge(dm_linktable[ , .(dmcode, desc)], by.x = 'id', by.y = 'dmcode')

# remove temp variables
rm(list = ls(pattern = 'temp'))

# make tables --------------------------------------------------------

tab = clust$perf[samp=='insamp', .(cluster, nstrat, tstat, rbar)] %>% 
  # add oos perf
  merge(clust$perf[samp=='oos', ] %>% 
    transmute(cluster, rbaroos = rbar), by = 'cluster') %>% 
  mutate(rbaroos_rbar = rbaroos/rbar) %>% 
  select(-rbaroos)  %>% 
  # add representatives
  merge(clust$rep %>% 
    transmute(cluster, rep=desc, repcor=meancor), by = 'cluster') %>% 
  # add correlations
  merge(clust$cor %>% 
    select(cluster, starts_with('cor_'), rsq), by = 'cluster') %>% 
    as_tibble() %>% 
    arrange(-rsq)

# tbc: output nice latex table
tab %>% select(cluster, rbaroos_rbar, rep, rsq) %>% print()
tab %>% select(-rep) %>% print()
