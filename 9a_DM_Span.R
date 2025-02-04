# Created 2024 05. Finds spanned and unspanned dm strats, makes 
# themes based off hierarchical clustering using correlation matrix
# tbc

# Setup --------------------------------------------------------

rm(list = ls())
source("0_Environment.R")
library(doParallel)

# hierarchical clustering
library(ggdendro)
library(dendextend)

## User Settings ------------------------------------------------

# in serial, 300 strats in 6 sec, 30000 in 17 hours
# with 4 cores, 1500 strats in 20 sec, 30000 strats in 2.5 hours
# for 6000 strats, minutes  = 6000^2/1500^2 * 20/60 = 5

# number of cores
ncores = globalSettings$num_cores

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

# aesthetic settings
fontsizeall = 28
legposall = c(30,15)/100
ylaball = 'Trailing 5-Year Return (bps pm)'
linesizeall = 1.5

## Load data ----------------------------------------------------

inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType, 
                                  topT = globalSettings$topT)


# published
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
    filter(Keep) %>% 
    filter(signalname %in% inclSignals) %>% 
    setDT()

czcat <- fread("DataInput/SignalsTheoryChecked.csv") %>%
    select(signalname, Year, theory) %>% 
    filter(signalname %in% inclSignals) 

czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  left_join(czcat, by = "signalname") %>% 
  filter(signalname %in% inclSignals) %>% 
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

# Convenience save ----------------------------------------------
save.image('../Data/9a_DMThemes.RData')

# Convenience load ----------------------------------------------

load('../Data/9a_DMThemes.RData')

library(pcaMethods)

source('0_Environment.R')

print("Running span against PCA")
print("Takes about 2 hours using 4 cores")
print("It can probably be way faster")
pca_span_dt <- adj_R2_with_PPCA(  DMname = dmcomp$name,
                                  nsampmax = Inf)
# Convenience save ----------------------------------------------
save.image('../Data/9a_DMThemes_with_pca.RData')

# Convenience load ----------------------------------------------
load('../Data/9a_DMThemes_with_pca.RData')
source('0_Environment.R')
pca_span_dt[, spanned_pca :=  ifelse(N_pca > 30 & adj_r2 > 0.25, TRUE, FALSE)]

pca_span_dt[, spanned_pca_ever_end := any(spanned_pca), by = .(sweight, dmname)]
pca_span_dt[spanned_pca_ever_end == FALSE,
            paste(sweight, dmname) %>%
              unique() %>% length()]/pca_span_dt[,
                paste(sweight, dmname) %>% unique() %>% length()]
pca_span_dt %>% setorder(dmname, sweight, sampend)

pca_span_dt[, spanned_ever:= as.logical(cummax(as.integer(spanned_pca))), by = .(dmname, sweight)]


dt_with_spanned_ever <- pca_span_dt[
  , .(sweight, dmname, sampstart, sampend, adj_r2,
      npcs, N_pca, spanned_pca, spanned_ever)][
    dmpred$matched, on = c('dmname', 'sampstart', 'sampend', 'sweight'  )]

#####################################
# PCA
#####################################
# Plot decay --------------------------------------------------

## make event time returns -----------------------
print("Making spanned accounting event time returns")
print("Can take a few minutes...")
start_time <- Sys.time()

dmpred$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = dt_with_spanned_ever[spanned_ever==TRUE]
  , npubmax = plotdat$npubmax, 
  czsum = czsum, use_sign_info = plotdat$use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

plotdat$comp_matched <- dt_with_spanned_ever[spanned_ever==TRUE]
plotdat$comp_event_time <- dmpred$event_time

# Plot decay t > t op --------------------------------------------------

print("Making unspanned accounting event time returns t > t op")
print("Can take a few minutes...")
start_time <- Sys.time()
dmpred$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = dt_with_spanned_ever[spanned_ever==FALSE  & abs(tstat) > tstat_op]
  , npubmax = plotdat$npubmax, 
  czsum = czsum, use_sign_info = plotdat$use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

plotdat$unspan_matched_t_g <- dt_with_spanned_ever[spanned_ever==FALSE   & abs(tstat) > tstat_op]
plotdat$unspan_event_time_t_g <- dmpred$event_time

print("Making unspanned accounting event time returns t < t op")
print("Can take a few minutes...")
start_time <- Sys.time()
dmpred$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = dt_with_spanned_ever[spanned_ever==FALSE  & abs(tstat) <= tstat_op]
  , npubmax = plotdat$npubmax, 
  czsum = czsum, use_sign_info = plotdat$use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

plotdat$unspan_matched_t_l <- dt_with_spanned_ever[spanned_ever==FALSE & abs(tstat) <= tstat_op]
plotdat$unspan_event_time_t_l <- dmpred$event_time


# join and reformat for plotting function
ret_for_plotting_pca <- czret %>%
  transmute(pubname = signalname, eventDate, ret = ret_scaled) %>%
  left_join(
    plotdat$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
  ) %>%
  left_join(
    plotdat$unspan_event_time_t_g %>% transmute(pubname, eventDate, matchRetAlt = dm_mean)
  ) %>%
  left_join(
    plotdat$unspan_event_time_t_l %>% transmute(pubname, eventDate, newRet = dm_mean)
  ) %>%
  select(eventDate, ret, matchRet, matchRetAlt, newRet, pubname) %>%
  # keep only rows where both matchrets are observed
  filter(!is.na(matchRet) & !is.na(matchRetAlt))

## actually plot ----------------------------------------------
printme = ReturnPlotsWithDM4series(
  dt = ret_for_plotting_pca, 
  basepath = "../Results/Fig_DM",
  suffix = 'unspan_match_t_g_PCA',
  rollmonths = 60,
  colors = c(colors, "#7E2F8E"),
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  legendlabels =
    c(
      paste0("Published"),
      paste0("Adj. R2 > 0.25"),
      paste0("Adj. R2 < 0.25 \n& t-stat > t pub"),
      paste0("Adj. R2 < 0.25 \n& t-stat <= t pub")
    ),
  legendpos = c(25,30)/100,
  fontsize = 48,
  yaxislab = ylaball,
  linesize = 2
)
#####################################
# Corrs
#####################################

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

plotdat$comp_matched_cor <- dmpred$matched[spanned_ever==TRUE]
plotdat$comp_event_time_cor <- dmpred$event_time

# Plot decay t > t op --------------------------------------------------

print("Making unspanned accounting event time returns t > t op")
print("Can take a few minutes...")
start_time <- Sys.time()
dmpred$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = dmpred$matched[spanned_ever==FALSE  & abs(tstat) > tstat_op]
  , npubmax = plotdat$npubmax, 
  czsum = czsum, use_sign_info = plotdat$use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

plotdat$unspan_matched_t_g_cor <- dmpred$matched[spanned_ever==FALSE   & abs(tstat) > tstat_op]
plotdat$unspan_event_time_t_g_cor <- dmpred$event_time

print("Making unspanned accounting event time returns t < t op")
print("Can take a few minutes...")
start_time <- Sys.time()
dmpred$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = dmpred$matched[spanned_ever==FALSE  & abs(tstat) <= tstat_op]
  , npubmax = plotdat$npubmax, 
  czsum = czsum, use_sign_info = plotdat$use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

plotdat$unspan_matched_t_l_cor <- dmpred$matched[spanned_ever==FALSE & abs(tstat) <= tstat_op]
plotdat$unspan_event_time_t_l_cor <- dmpred$event_time


# join and reformat for plotting function
ret_for_plotting_cor <- czret %>%
  transmute(pubname = signalname, eventDate, ret = ret_scaled) %>%
  left_join(
    plotdat$comp_event_time_cor %>% transmute(pubname, eventDate, matchRet = dm_mean)
  ) %>%
  left_join(
    plotdat$unspan_event_time_t_g_cor %>% transmute(pubname, eventDate, matchRetAlt = dm_mean)
  ) %>%
  left_join(
    plotdat$unspan_event_time_t_l_cor %>% transmute(pubname, eventDate, newRet = dm_mean)
  ) %>%
  select(eventDate, ret, matchRet, matchRetAlt, newRet, pubname) %>%
  # keep only rows where both matchrets are observed
  filter(!is.na(matchRet) & !is.na(matchRetAlt))

## actually plot ----------------------------------------------
printme = ReturnPlotsWithDM4series(
  dt = ret_for_plotting_cor, 
  basepath = "../Results/Fig_DM",
  suffix = 'unspan_match_t_g_cor',
  rollmonths = 60,
  colors = c(colors, "#7E2F8E"),
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  legendlabels =
    c(
      paste0("Published"),
      paste0("Adj. R2 > 0.25"),
      paste0("Adj. R2 < 0.25 \n& t-stat > t pub"),
      paste0("Adj. R2 < 0.25 \n& t-stat <= t pub")
    ),
  legendpos = c(25,30)/100,
  fontsize = 48,
  yaxislab = ylaball,
  linesize = 2
)

# Describe spanning over time -----------------------------------
# Convert sampendlist to Date objects assuming all dates are the first of the month
# make list of samples
samplist <- czsum %>%
  distinct(sampstart, sampend) %>%
  arrange(sampstart, sampend)

# mark dm strats that are spanned by current pub or previous pub
sampendlist = samplist$sampend %>% sort() %>% unique()
# sampendlist <- as.Date(paste0(sampendlist, "-01"), format="%b %Y-%d")

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
  data_subset <- dmpred$matched[sampend <= sampend_loop, ]
  
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

tab_span2
# Melt the data frame for ggplot
library(reshape2)

# Melt the data frame for ggplot
tab_span2_melt <- melt(tab_span2, id.vars = "sampend") %>% setDT()
tab_span2_melt <- tab_span2_melt[variable != 'pct_unspan']
# Change the variable names for the plot
tab_span2_melt$variable <- factor(tab_span2_melt$variable, 
                                  levels = c("n_dm_tg2", "n_span", "n_unspan"), 
                                  labels = c("Cummulative Number of Matched DM Portfolios",
                                             "Number of Spanned DM Portfolios",
                                             "Number of Unspanned DM Portfolios"))

# Create the ggplot
plot <- ggplot(tab_span2_melt, aes(x = sampend, y = value, color = variable)) +
  geom_line(aes(group = variable), linewidth = 1.1) +
  geom_point() +
  labs(x = "",
       y = "N",
       color = "") +
  theme_minimal() +
  scale_color_manual(values = colors)+
  theme_light(base_size = fontsizeall)+
  theme(legend.position = c(0.05, 0.95),
        legend.justification = c(0, 1),
        legend.spacing.y = unit(0.1, units = 'cm'),
        legend.background = element_rect(fill = 'transparent'),
        legend.key.width = unit(1.5, units = 'cm')
  )
# Save the plot to a PDF file
ggsave("../Results/Fig_spanned_over_time.pdf", plot = plot, device = "pdf", width = 15, height = 12)

library(xtable)
table_latex <- xtable(tab_span2, caption = "Spanning of Strategies Over Time", label = "tab:spanning")
# Set the table alignment and format
align(table_latex) <- "llllll"
digits(table_latex) <- c(0, 0, 0, 0, 0, 1)  # Adjust the number of digits for each column

# Print the LaTeX code
print(table_latex, type = "latex", include.rownames = FALSE, caption.placement = "top", 
      hline.after = seq(from = 0, to = nrow(tab_span2), by = 1))




tab_span = dmpred$matched %>%
  distinct(sampend, sweight, dmname, spanned_ever) %>%
  .[ , .(n_dm_tg2 = .N, n_span = sum(spanned_ever)
          , n_unspan = sum(!spanned_ever)
          , pct_unspan = 100*mean(!spanned_ever))
    , by = c('sampend')] %>%
  arrange(sampend)

# # tbc: make a nice table pls
tab_span %>% as_tibble() %>% print(n = 100)

#####################################
# PCA
#####################################

# Convert sampendlist to Date objects assuming all dates are the first of the month
# make list of samples
samplist <- czsum %>%
  distinct(sampstart, sampend) %>%
  arrange(sampstart, sampend)

# mark dm strats that are spanned by current pub or previous pub
sampendlist = samplist$sampend %>% sort() %>% unique()
# sampendlist <- as.Date(paste0(sampendlist, "-01"), format="%b %Y-%d")

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


tab_span_pca
# Convert sampend to Date format
tab_span_pca$sampend <- dmy(paste("01", tab_span_pca$sampend))

# Melt the data frame for ggplot
tab_span2_melt <- melt(tab_span_pca, id.vars = "sampend") %>% setDT()
tab_span2_melt <- tab_span2_melt[variable != 'pct_unspan']

# Melt the data frame for ggplot
library(reshape2)

# Change the variable names for the plot
tab_span2_melt$variable <- factor(tab_span2_melt$variable, 
                                  levels = c("n_dm_tg2", "n_span", "n_unspan"), 
                                  labels = c("Cummulative Number of Matched DM Portfolios",
                                             "Number of Spanned DM Portfolios",
                                             "Number of Unspanned DM Portfolios"))
colors
# Create the ggplot
plot <- ggplot(tab_span2_melt, aes(x = sampend, y = value, color = variable)) +
  geom_line(aes(group = variable), linewidth = 1.1) +
  geom_point() +
  labs(x = "",
       y = "N",
       color = "") +
  theme_minimal() +
  scale_color_manual(values = colors)+
  theme_light(base_size = fontsizeall)+
  theme(legend.position = c(0.05, 0.95),
        legend.justification = c(0, 1),
    legend.spacing.y = unit(0.1, units = 'cm'),
    legend.background = element_rect(fill = 'transparent'),
    legend.key.width = unit(1.5, units = 'cm')
  )
plot
# Save the plot to a PDF file
ggsave("../Results/Fig_spanned_over_time_pca.pdf", plot = plot, device = "pdf", width = 15, height = 12)
