# Pairwise correlations and PCA decomp for all data mined strategies with t-stat greater than 2

# Environment ------------------------
source('0_Environment.R')
library(doParallel)

# settings
ncores <- round(detectCores() / 2)
minShareTG2 = .1  # Include strategies with t-stat >2 in at least X % of matches
TG2Set = '1994-2020' # 'Matches' 
# 1994-2020: DM strategies evaluated over 1994-2020
# Matches: all sample matching periods

DMname = paste0('../Data/Processed/',
                globalSettings$dataVersion, 
                ' LongShort.RData')

dmcomp <- list()
dmcomp$name <- paste0('../Data/Processed/',
                      globalSettings$dataVersion, 
                      ' LongShort.RData')

# these are treated as globals (don't modify pls)
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(Keep)


# read in DM strats
dm_rets <- readRDS(DMname)$ret
dm_info <- readRDS(DMname)$port_list

dm_rets <- dm_rets %>%
  left_join(
    dm_info %>% select(portid, sweight),
    by = c("portid")
  ) %>%
  transmute(
    sweight,
    dmname = signalid,
    yearm,
    ret,
    nstock_long,
    nstock_short
  ) %>%
  setDT()

# settings and functions ------------------------

## function for computing DM strat sumstats in pub samples
sumstats_for_DM_Strats <- function(
    DMname = DMname,
    nsampmax = Inf) {
  
  # read in DM strats
  dm_rets <- readRDS(DMname)$ret
  dm_info <- readRDS(DMname)$port_list
  
  dm_rets <- dm_rets %>%
    left_join(
      dm_info %>% select(portid, sweight),
      by = c("portid")
    ) %>%
    transmute(
      sweight,
      dmname = signalid,
      yearm,
      ret,
      nstock_long,
      nstock_short
    ) %>%
    setDT()
  
  # Finds sum stats for dm in each pub sample
  # the output for this can be used for all dm selection methods
  samplist <- czsum %>%
    distinct(sampstart, sampend) %>%
    arrange(sampstart, sampend)
  
  # set up for parallel
  cl <- makePSOCKcluster(ncores)
  registerDoParallel(cl)
  
  # loop setup
  nsamp <- dim(samplist)[1]
  nsamp <- min(nsamp, nsampmax)
  dm_insamp <- list()
  
  # dopar in a function needs some special setup
  # https://stackoverflow.com/questions/6689937/r-problem-with-foreach-dopar-inside-function-called-by-optim
  dm_insamp <- foreach(
    sampi = 1:nsamp,
    .combine = rbind,
    .packages = c("data.table", "tidyverse", "zoo"),
    .export = ls(envir = globalenv())
  ) %dopar% {
    # feedback
    print(paste0("DM sample stats for sample ", sampi, " of ", nsamp))
    
    # find sum stats for the current sample
    sampcur <- samplist[sampi, ]
    sumcur <- dm_rets[
      yearm >= sampcur$sampstart &
        yearm <= sampcur$sampend &
        !is.na(ret),
      .(
        rbar = mean(ret), tstat = mean(ret) / sd(ret) * sqrt(.N),
        min_nstock_long = min(nstock_long),
        min_nstock_short = min(nstock_short),
        nmonth = sum(!is.na(ret))
      ),
      by = c("sweight", "dmname")
    ]
    # find number of obs in the last year of the sample
    filtcur <- dm_rets[
      floor(yearm) == year(sampcur$sampend) &
        !is.na(ret),
      .(nlastyear = .N),
      by = c("sweight", "dmname")
    ]
    
    # combine and save
    sumcur <- sumcur %>%
      left_join(filtcur, by = c("sweight", "dmname")) %>%
      mutate(
        sampstart = sampcur$sampstart, sampend = sampcur$sampend
      )
    
    return(sumcur)
  } # end dm_insamp loop
  stopCluster(cl)
  
  # Merge with czsum
  # insampsum key is c(pubname,dmname). Each row is a dm strat that matches a pub
  insampsum <- czsum %>%
    transmute(
      pubname = signalname, rbar_op = rbar, tstat_op = tstat, sampstart, sampend,
      sweight = tolower(sweight)
    ) %>%
    left_join(
      dm_insamp,
      by = c("sampstart", "sampend", "sweight"),
      relationship = "many-to-many" # required to suppress warning
    ) %>%
    arrange(pubname, desc(tstat))
  
  setDT(insampsum)
  
  return(insampsum)
} # end Sumstats function


compute_pca = function(ret1){
  
  # make wide matrix
  temp = dcast(ret1, yearm ~ dmname, value.var = 'ret') 
  retmat0 = as.matrix(temp[ , -1])
  rownames(retmat0) = temp$yearm
  
  # drop signals with missing values
  nmonthmissmax = 0
  signalmiss = colSums(is.na(retmat0))
  retmat = retmat0[ , signalmiss <= nmonthmissmax] 
  
  # drop months with missing values (redundant right now)
  # nstratmissmax = 0.1*nstrat
  # monthmiss = rowSums(is.na(retmat)) 
  # retmat = retmat[monthmiss <= nstratmissmax , ]
  
  # PCA
  A = (retmat - colMeans(retmat))/ sqrt(nrow(retmat))
  Asvd = svd(A)
  pcadat = tibble(n_pc = 1:length(Asvd$d) , eval = Asvd$d^2)  %>% 
    mutate(cum_pct_exp = cumsum(eval)/sum(eval)*100) %>% 
    mutate(nstrat = dim(retmat)[2])
  
  # # sanity check (this requires a lot more compute)
  # coveig = eigen(cov(retmat))
  # temp = cumsum(coveig$values)/sum(coveig$values)*100
  # temp %>% head()
  # pcadat$cum_pct_exp %>% head()
  
  return(pcadat)
} # end compute_pca 


# Find relevant TG2 set  -----------------------------------------------------

if (TG2Set == 'Matches') {
  
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
  
  # Find DM strategies with t-stat greater 2 in at least 10% of cases
  DMstratT = dmcomp$insampsum %>% 
    mutate(TG2 = abs(tstat) > 2) %>% 
    group_by(dmname, sweight) %>%
    summarise(ShareTG2 = mean(TG2)) %>% 
    ungroup()
  
  # DMstratT %>% 
  #   ggplot(aes(x = ShareTG2)) +
  #   geom_histogram() +
  #   facet_wrap(~sweight)
  
  DMstratTG2 = list(
    ew = DMstratT %>% 
      filter(ShareTG2 > minShareTG2, sweight == 'ew') %>%
      pull(dmname),
    vw = DMstratT %>% 
      filter(ShareTG2 > minShareTG2, sweight == 'vw') %>%
      pull(dmname)
  )  
  
} else if (TG2Set == '1994-2020') {
  
  # Alternative: Regardless of published sample matches, just look at t-stats in 1994-2020
  DMStratT19942020 = dm_rets[
    yearm >= as.yearmon('1994-01-01') &
      yearm <= as.yearmon('2020-12-31') &   
      !is.na(ret),
    .(
      rbar = mean(ret), tstat = mean(ret) / sd(ret) * sqrt(.N),
      min_nstock_long = min(nstock_long),
      min_nstock_short = min(nstock_short),
      nmonth = sum(!is.na(ret))
    ),
    by = c("sweight", "dmname")
  ]
  
  # Find DM strategies with t-stat greater 2
  DMstratTG2 = list(
    ew = DMStratT19942020 %>% 
      filter(abs(tstat) > 2, sweight == 'ew') %>% 
      pull(dmname),
    vw = DMStratT19942020 %>% 
      filter(abs(tstat) > 2, sweight == 'vw') %>% 
      pull(dmname)
  )
  
} else {
  print('TG2Set has to be one of Matches or 1994-2020')
}


# Calculate correlations -------------------------------------------------------

# Define the task for each core
cor_task <- function(index_pair) {
  x <- retwide[, index_pair[1], drop = FALSE]
  y <- retwide[, index_pair[2], drop = FALSE]
  cor(x, y, use = "pairwise.complete.obs")
}

for (weights in c('vw', 'ew')) {
  
  #  weights = 'vw'
  
  retwide = dm_rets %>% 
    filter(dmname %in% unlist(DMstratTG2[weights]),
           sweight == weights) %>% 
    transmute(dmname, 
              yearm, 
              ret) %>% 
    pivot_wider(names_from = dmname, values_from = ret) %>% 
    select(-yearm)
  
  # Parallelize
  cl <- makeCluster(ncores)
  clusterExport(cl, "retwide")
  
  # Generate all unique column pairs
  column_indices <- seq_len(ncol(retwide))
  combinations <- t(combn(column_indices, 2))
  
  # Compute correlations in parallel
  cor_results <- parLapply(cl, split(combinations, seq_len(nrow(combinations))), cor_task)
  
  # Stop the cluster
  stopCluster(cl)
  
  # Unlist and save
  allRhosDM = cor_results %>% unlist()
  saveRDS(allRhosDM, paste0('../Results/PairwiseCorrelationsDM_', weights, '.RDS'))
  
  # housekeeping
  rm(cor_results, column_indices, combinations, retwide, allRhosDM)
  
}

# Prep table
quantilesCorDM = tibble()
for (weights in c('vw', 'ew')) {
  
  allRhosDM = readRDS(paste0('../Results/PairwiseCorrelationsDM_', weights, '.RDS')) 
  
  tmp = tibble(rho = allRhosDM) %>% 
    summarise(
      Q1  = quantile(rho, probs = 0.01, na.rm = TRUE),
      Q5  = quantile(rho, probs = 0.05, na.rm = TRUE),
      Q10 = quantile(rho, probs = 0.1, na.rm = TRUE),
      Q25 = quantile(rho, probs = 0.25, na.rm = TRUE),
      Q50 = quantile(rho, probs = 0.50, na.rm = TRUE),
      Q75 = quantile(rho, probs = 0.75, na.rm = TRUE),
      Q90 = quantile(rho, probs = 0.90, na.rm = TRUE),
      Q95 = quantile(rho, probs = 0.95, na.rm = TRUE),
      Q99 = quantile(rho, probs = 0.99, na.rm = TRUE),
    ) %>% 
    mutate(weight = weights)
  
  quantilesCorDM = bind_rows(quantilesCorDM, tmp)
  
  
}


#Output for TeX
quantilesCorDM %>% 
  mutate(Kind = ifelse(weight == 'ew', 'Equal-Weighted', 'Value-Weighted')) %>% 
  transmute(Kind,
            empty1 = NA_character_,
            Q1,
            Q5,
            Q10,
            Q25,
            Q50,
            Q75,
            Q90,
            Q95,
            Q99) %>% 
  arrange(Kind) %>% 
  xtable(digits = c(0, 0, 2, 2 , 2, 2, 2, 2, 2, 2, 2, 2)) %>% 
  print(
    include.rownames = FALSE,
    include.colnames = FALSE,
    hline.after = NULL,
    only.contents = TRUE,
    file = "../Results/quantilesCorDM.tex"  
  )



# PCA  -------------------------------------------------------------------------

# fix sample 
yearm_min = 1994
yearm_max = 2020

pca_ew = compute_pca(ret1 = dm_rets %>% 
                       filter(sweight == 'ew',
                              dmname %in% unlist(DMstratTG2['ew']),
                              yearm >= yearm_min, 
                              yearm <= yearm_max) %>% 
                       select(yearm, dmname, ret)
)

pca_vw = compute_pca(ret1 = dm_rets %>% 
                       filter(sweight == 'vw',
                              dmname %in% unlist(DMstratTG2['vw']),
                              yearm >= yearm_min, 
                              yearm <= yearm_max) %>% 
                       select(yearm, dmname, ret)
)

# make a nice table 
tab = pca_ew %>% transmute(n_pc, pct_exp_ew = cum_pct_exp) %>% 
  left_join(
    pca_vw %>% transmute(n_pc, pct_exp_vw = cum_pct_exp),
    by = c('n_pc')
  ) %>% 
  filter(n_pc %in% c(1, 5, seq(10, 100, 10))) %>% 
  mutate(
    n_pc = as.character(as.integer(n_pc))
    , pct_exp_ew = round(pct_exp_ew, 0)
    , pct_exp_vw = round(pct_exp_vw, 0)
  )  %>% 
  t() 

alignment = paste0('cl', rep('c', ncol(tab)-1) %>% paste(collapse = ''))

# initialize latex table
library(xtable)
xtable(tab, align = alignment) %>% 
  print(include.colnames = FALSE, floating = FALSE
        , booktabs = TRUE) %>% 
  cat(file = '../Results/DM_pca.tex')

# read in tex and edit
texline = readLines('../Results/DM_pca.tex')

texline = append(texline, paste0(
  ' & \\multicolumn{'
  , nchar(alignment)-2
  , '}{c}{Panel (b): PCA Explained Variance (\\%)} \\\\'
), after = 4)
texline = append(texline,  '\\midrule', after = 7)

texline = str_replace(texline, fixed('n\\_pc'), 'Number of PCs') %>% 
  str_replace(fixed('pct\\_exp\\_ew'), 'Equal-Weighted') %>%
  str_replace(fixed('pct\\_exp\\_vw'), 'Value-Weighted') 

writeLines(texline, '../Results/DM_pca.tex')
