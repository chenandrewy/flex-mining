# Precompute correlation- and PCA-spanning analysis.
#
# How to run: source from 3_Precompute.R with the working directory set to
#   flex-mining/.
# Inputs:  chapter-2 mined strategies and cleaned published-signal data
# Outputs: ../Data/Processed/dm_span_analysis.RDS
#
# Exhibit rendering is owned by Appendices/SA11_DMSpanPCAPlots.R.

# Setup --------------------------------------------------------

rm(list = ls())
source("0_Environment.R")
library(doParallel)


## Settings -----------------------------------------------------

# Compustat mined-strategy file
dm_path <- paste0(
  "../Data/Processed/", globalSettings$dataVersion, " LongShort.RData"
)

# maximum correlation (signed)
# For ref, cor(ret BMdec, ret diff(at)/lag(at)) = -0.64
maxcor = 0.5

# Matching settings
npubmax <- Inf
use_sign_info <- TRUE
match_screen <- list(
  # tolerance in levels
  t_tol = globalSettings$t_tol,
  r_tol = globalSettings$r_tol,
  # tolerance relative to op stat
  t_reltol = globalSettings$t_reltol,
  r_reltol = globalSettings$r_reltol,
  # alternative filtering
  t_min = globalSettings$t_min, # Default = 0, minimum screened t-stat
  t_max = globalSettings$t_max, # maximum screened t-stat
  t_rankpct_min = globalSettings$t_rankpct_min, # top x% of data mined t-stats, 100% for off
  minNumStocks = globalSettings$minNumStocks
)

## Load data ----------------------------------------------------

inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType, 
                                  topT = globalSettings$topT)


# published
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
    filter(Keep) %>% 
    filter(signalname %in% inclSignals) %>% 
    setDT()

czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  filter(signalname %in% inclSignals) %>%
  mutate(ret_scaled = ret / rbar * 100)

# Generate Compustat DM sumstats --------------------------------

print("creating Compustat mining in-sample sumstats")
print("Takes about 4 minutes using 4 cores")
start_time <- Sys.time()
dm_insamp_sum <- sumstats_for_DM_Strats(
  DMname = dm_path,
  nsampmax = Inf
)
print("finished")
stop_time <- Sys.time()
stop_time - start_time


# Select matched DM strategies and classify correlation spanning ----------

## Select strats -----------------------

matched_strategies <- SelectDMStrats(dm_insamp_sum, match_screen)

## Loop -----------------------
# mark dm strats that are spanned by current pub or previous pub
sampendlist <- sort(unique(czsum$sampend))

for (i in 1:length(sampendlist)){

  # initialize
  if (i==1) {
    spanned = data.table(); matched_strategies$spanned_ever = FALSE
  }
  
  # find sweight, dmnames that are spanned now
  spanned_now = matched_strategies[sampend == sampendlist[i]] %>%
    filter(sign(rbar)*cor > maxcor) %>% 
    mutate(sampend = sampendlist[i]) %>%
    select(sampend, sweight, dmname) 

  # combine with previous
  spanned = rbind(spanned, spanned_now)

  # mark ew dm strats that have ever been spanned
  badlist_ew = spanned[sampend <= sampendlist[i] & sweight == 'ew']$dmname
  matched_strategies[sampend == sampendlist[i] & sweight == 'ew'
    , spanned_ever := dmname %in% badlist_ew]

  # mark vw dm strats that have ever been spanned
  badlist_vw = spanned[sampend <= sampendlist[i] & sweight == 'vw']$dmname
  matched_strategies[sampend == sampendlist[i] & sweight == 'vw'
    , spanned_ever := dmname %in% badlist_vw]
  
} # end for i in 1:length sampendlist

library(pcaMethods)

print("Running span against PCA")
print("Takes about 2 hours using 4 cores")
print("It can probably be way faster")
pca_span_dt <- adj_R2_with_PPCA(  DMname = dm_path,
                                  nsampmax = Inf)
pca_span_dt[, spanned_pca :=  ifelse(N_pca > 30 & adj_r2 > 0.25, TRUE, FALSE)]

pca_span_dt %>% setorder(dmname, sweight, sampend)

pca_span_dt[, spanned_ever:= as.logical(cummax(as.integer(spanned_pca))), by = .(dmname, sweight)]


dt_with_spanned_ever <- pca_span_dt[
  , .(sweight, dmname, sampstart, sampend, adj_r2,
      npcs, N_pca, spanned_pca, spanned_ever)][
    matched_strategies, on = c('dmname', 'sampstart', 'sampend', 'sweight'  )]

#####################################
# PCA
#####################################
# Plot decay --------------------------------------------------

## make event time returns -----------------------
print("Making spanned accounting event time returns")
print("Can take a few minutes...")
start_time <- Sys.time()

pca_spanned_event_time <- make_DM_event_returns(
  DMname = dm_path, match_strats = dt_with_spanned_ever[spanned_ever == TRUE],
  npubmax = npubmax, czsum = czsum, use_sign_info = use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

# Plot decay t > t op --------------------------------------------------

print("Making unspanned accounting event time returns t > t op")
print("Can take a few minutes...")
start_time <- Sys.time()
pca_unspanned_gt_event_time <- make_DM_event_returns(
  DMname = dm_path,
  match_strats = dt_with_spanned_ever[
    spanned_ever == FALSE & abs(tstat) > tstat_op
  ],
  npubmax = npubmax, czsum = czsum, use_sign_info = use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

print("Making unspanned accounting event time returns t < t op")
print("Can take a few minutes...")
start_time <- Sys.time()
pca_unspanned_le_event_time <- make_DM_event_returns(
  DMname = dm_path,
  match_strats = dt_with_spanned_ever[
    spanned_ever == FALSE & abs(tstat) <= tstat_op
  ],
  npubmax = npubmax, czsum = czsum, use_sign_info = use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

# join and reformat for plotting function
ret_for_plotting_pca <- czret %>%
  transmute(pubname = signalname, eventDate, ret = ret_scaled) %>%
  left_join(
    pca_spanned_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
  ) %>%
  left_join(
    pca_unspanned_gt_event_time %>%
      transmute(pubname, eventDate, matchRetAlt = dm_mean)
  ) %>%
  left_join(
    pca_unspanned_le_event_time %>%
      transmute(pubname, eventDate, newRet = dm_mean)
  ) %>%
  select(eventDate, ret, matchRet, matchRetAlt, newRet, pubname) %>%
  # keep only rows where both matchrets are observed
  filter(!is.na(matchRet) & !is.na(matchRetAlt))

#####################################
# Corrs
#####################################

# Plot decay --------------------------------------------------

## make event time returns -----------------------
print("Making spanned accounting event time returns")
print("Can take a few minutes...")
start_time <- Sys.time()
corr_spanned_event_time <- make_DM_event_returns(
  DMname = dm_path, match_strats = matched_strategies[spanned_ever == TRUE],
  npubmax = npubmax, czsum = czsum, use_sign_info = use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

# Plot decay t > t op --------------------------------------------------

print("Making unspanned accounting event time returns t > t op")
print("Can take a few minutes...")
start_time <- Sys.time()
corr_unspanned_gt_event_time <- make_DM_event_returns(
  DMname = dm_path,
  match_strats = matched_strategies[
    spanned_ever == FALSE & abs(tstat) > tstat_op
  ],
  npubmax = npubmax, czsum = czsum, use_sign_info = use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

print("Making unspanned accounting event time returns t < t op")
print("Can take a few minutes...")
start_time <- Sys.time()
corr_unspanned_le_event_time <- make_DM_event_returns(
  DMname = dm_path,
  match_strats = matched_strategies[
    spanned_ever == FALSE & abs(tstat) <= tstat_op
  ],
  npubmax = npubmax, czsum = czsum, use_sign_info = use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

# join and reformat for plotting function
ret_for_plotting_cor <- czret %>%
  transmute(pubname = signalname, eventDate, ret = ret_scaled) %>%
  left_join(
    corr_spanned_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
  ) %>%
  left_join(
    corr_unspanned_gt_event_time %>%
      transmute(pubname, eventDate, matchRetAlt = dm_mean)
  ) %>%
  left_join(
    corr_unspanned_le_event_time %>%
      transmute(pubname, eventDate, newRet = dm_mean)
  ) %>%
  select(eventDate, ret, matchRet, matchRetAlt, newRet, pubname) %>%
  # keep only rows where both matchrets are observed
  filter(!is.na(matchRet) & !is.na(matchRetAlt))

# Describe spanning over time -----------------------------------
# Initialize an empty data frame to store the results
tab_span2 <- data.table(sampend = character(),
                        n_dm_tg2 = integer(),
                        n_span = integer(),
                        n_unspan = integer(),
                        pct_unspan = numeric(),
                        stringsAsFactors = FALSE)

# Loop over each sampendlist
for (sampend_loop in sampendlist %>% as.character()) {
  print(sampend_loop)
  # Subset data up to the current sampend
  data_subset <- matched_strategies[sampend <= sampend_loop, ]
  
  # Calculate the total number of strategies
  n_dm_tg2 <- nrow(unique(data_subset[, .(sweight, dmname)]))
  
  # Calculate the number of spanned strategies
  spanned_strategies <- unique(data_subset[spanned_ever == TRUE, .(sweight, dmname)])
  n_span <- nrow(spanned_strategies)
  
  # Calculate the number and percentage of unspanned strategies
  n_unspan <- n_dm_tg2 - n_span
  pct_unspan <- 100 * n_unspan / n_dm_tg2
  
  # Append the results to the tab_span data frame
  tab_span2 <- rbind(tab_span2, data.table(sampend = sampend_loop %>% as.character(),
                                           n_dm_tg2 = n_dm_tg2,
                                           n_span = n_span,
                                           n_unspan = n_unspan,
                                           pct_unspan = pct_unspan))
}

# Convert sampend to Date format
tab_span2$sampend <- dmy(paste("01", tab_span2$sampend))

#####################################
# PCA
#####################################

# Initialize an empty data frame to store the results
tab_span_pca <- data.table(sampend = character(),
                        n_dm_tg2 = integer(),
                        n_span = integer(),
                        n_unspan = integer(),
                        pct_unspan = numeric(),
                        stringsAsFactors = FALSE)

# Loop over each sampendlist
for (sampend_loop in sampendlist %>% as.character()) {
  print(sampend_loop)
  # Subset data up to the current sampend
  data_subset <- dt_with_spanned_ever[sampend <= sampend_loop, ]
  
  # Calculate the total number of strategies
  n_dm_tg2 <- nrow(unique(data_subset[, .(sweight, dmname)]))
  
  # Calculate the number of spanned strategies
  spanned_strategies <- unique(data_subset[spanned_ever == TRUE, .(sweight, dmname)])
  n_span <- nrow(spanned_strategies)
  
  # Calculate the number and percentage of unspanned strategies
  n_unspan <- n_dm_tg2 - n_span
  pct_unspan <- 100 * n_unspan / n_dm_tg2
  
  # Append the results to the tab_span data frame
  tab_span_pca <- rbind(tab_span_pca, data.table(sampend = sampend_loop %>% as.character(),
                                           n_dm_tg2 = n_dm_tg2,
                                           n_span = n_span,
                                           n_unspan = n_unspan,
                                           pct_unspan = pct_unspan))
}


# Convert sampend to Date format
tab_span_pca$sampend <- dmy(paste("01", tab_span_pca$sampend))

saveRDS(
  list(
    ret_for_plotting_pca = ret_for_plotting_pca,
    ret_for_plotting_cor = ret_for_plotting_cor,
    tab_span_cor = tab_span2,
    tab_span_pca = tab_span_pca
  ),
  "../Data/Processed/dm_span_analysis.RDS"
)
