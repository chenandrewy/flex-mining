# Pairwise correlations and PCA decomp for all data mined strategies with t-stat greater than 2

# Environment ------------------------
source('0_Environment.R')
library(doParallel)

# settings
ncores <- globalSettings$num_cores
minShareTG2 = globalSettings$minShareTG2
TG2Set = globalSettings$TG2Set

DMname = paste0('../Data/Processed/',
                globalSettings$dataVersion, 
                ' LongShort.RData')

dmcomp <- list()
dmcomp$name <- paste0('../Data/Processed/',
                      globalSettings$dataVersion, 
                      ' LongShort.RData')


# Load data ---------------------------------------------------------------
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
  
} else if (TG2Set == 'Rolling1994-2020') {
  
  dt = dm_rets %>%
    transmute(
      dmname, yearm, ret, sweight
    ) %>% 
    arrange(dmname, sweight, yearm) %>% 
    as.data.table() %>% 
    setkey(dmname, yearm)
  
  # Compute rolling t-stats
  dt[, ret_30y_l := data.table::shift(frollmean(ret, 12*30, NA)), by = .(dmname, sweight)]
  dt[, t_30y_l   := data.table::shift(frollapply(ret, 12*30, f.custom.t, fill = NA)), by = .(dmname, sweight)]
  
  
  # Find share of each DM strategies with t-stat greater 2
  DMstratT = dt %>% 
    filter(yearm >= as.yearmon('1994-01-01'),
           yearm <= as.yearmon('2020-12-31')) %>% 
    mutate(TG2 = abs(t_30y_l) > 2) %>% 
    group_by(dmname, sweight) %>%
    summarise(ShareTG2 = mean(TG2, na.rm = TRUE)) %>% 
    ungroup()
  
  # Find DM strategies with t-stat greater 2 share of at least minShareTG2
  DMstratTG2 = list(
    ew = DMstratT %>% 
      filter(ShareTG2 > minShareTG2, sweight == 'ew') %>%
      pull(dmname),
    vw = DMstratT %>% 
      filter(ShareTG2 > minShareTG2, sweight == 'vw') %>%
      pull(dmname)
  )  
  
} else {
  print('TG2Set has to be one of Matches or 1994-2020 or Rolling1994-2020')
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
