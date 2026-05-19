# This file produces regressions of the form 
# return = constant + post-sample + post-publication + fixed effects 
# in the published and data-mined predictors

# Setup --------------------------------------------------------

rm(list = ls())
source("0_Environment.R")
library(doParallel)

# Load and prep Data -------------------------------------------

# these are treated as globals (don't modify pls)
inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType, 
                                  topT = globalSettings$topT)

czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(Keep) %>% 
  filter(signalname %in% inclSignals) %>% 
  setDT()

czcat <- fread("DataInput/SignalsTheoryChecked.csv") %>%
  select(signalname, Year, theory, Journal) %>% 
  filter(signalname %in% inclSignals)

czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  left_join(czcat, by = "signalname") %>%
  mutate(ret_scaled = ret / rbar * 100) %>% 
  filter(signalname %in% inclSignals)

# Load pre-computed matched returns
ret_for_plot0 <- readRDS("../Data/Processed/ret_for_plot0.RDS")



# Add relevant sample and pub dates
regData = ret_for_plot0 %>% 
  left_join(czsum %>% 
              transmute(pubname = signalname,
                        sampstart,
                        sampend,
                        pubdate
                        )) %>% 
  # Add indicators
  mutate(postSample = ifelse(calendarDate >= sampend & calendarDate < pubdate , 1, 0),
         postPub    = ifelse(calendarDate >= pubdate, 1, 0)) %>% 
  # Add outcome
  mutate(diffRet = ret - matchRet,
         diffRet_unscaled = ret_unscaled - matchRet_unscaled) %>% 
  # Make sure complete data (a couple of NAs in matchRet)
  filter(complete.cases(.) == TRUE)


# Formatting for etable -------------------------------------------------------
etable_dict <- c(
  # Regressors
  postSample             = "Post-Sample",
  postPub                = "Post-Pub",
  grouppub               = "Published",
  "grouppub:postSample"  = "Published x Post-Sample",
  "grouppub:postPub"     = "Published x Post-Pub",
  # Dependent variables
  ret                    = "Return (scaled)",
  matchRet               = "DM Matched Return (scaled)",
  diffRet                = "Difference (scaled)",
  ret_unscaled           = "Return (unscaled)",
  matchRet_unscaled      = "DM Matched Return (unscaled)",
  diffRet_unscaled       = "Difference (unscaled)",
  ret_scaled             = "Return (scaled)",
  # Fixed effects
  pubname                = "Predictor",
  calendarDate           = "Month",
  dmname                 = "DM strategy"
)

# Regressions -------------------------------------------------------------

## Scaled ----
# Outcome: Pub return
fitLM1 = fixest::feols(ret ~ postSample + postPub | pubname, 
                       data = regData %>% 
                         filter(calendarDate >= sampstart),
                       cluster = ~pubname+calendarDate)

fitLM1a = fixest::feols(ret ~ postSample + postPub | pubname + calendarDate, 
                       data = regData %>% 
                         filter(calendarDate >= sampstart),
                       cluster = ~pubname+calendarDate)

# Outcome: DM benchmark return
# fitLM2 = fixest::feols(matchRet ~ postSample + postPub | pubname, 
#                        data = regData %>% 
#                          filter(calendarDate >= sampstart),
#                        cluster = ~pubname+calendarDate)
# 
# fitLM2a = fixest::feols(matchRet ~ postSample + postPub | pubname + calendarDate, 
#                        data = regData %>% 
#                          filter(calendarDate >= sampstart),
#                        cluster = ~pubname+calendarDate)

# Outcome: difference in returns (ret - matchret)
# fitLM3 = fixest::feols(diffRet ~ postSample + postPub | pubname, 
#                        data = regData %>% 
#                          filter(calendarDate >= sampstart),
#                        cluster = ~pubname+calendarDate)
# 
# fitLM3a = fixest::feols(diffRet ~ postSample + postPub | pubname + calendarDate, 
#                        data = regData %>% 
#                          filter(calendarDate >= sampstart),
#                        cluster = ~pubname+calendarDate)
# 
# fixest::etable(
#   list(fitLM1, fitLM1a, fitLM2, fitLM2a, fitLM3, fitLM3a),
#   tex = FALSE,
#   dict = etable_dict,
#   depvar = FALSE,
#   headers = c("Predictor Return", "Predictor Return", "DM Matched Return", "DM Matched Return", "Pred-Matched Ret", "Pred-Matched Ret"),
#   fitstat = ~ n + r2 + wr2
# )


## Unscaled Regressions -------------------------------------------------------

# Outcome: Pub return (unscaled)
fitLM1_u = fixest::feols(ret_unscaled ~ postSample + postPub | pubname,
                       data = regData %>%
                         filter(calendarDate >= sampstart),
                       cluster = ~pubname+calendarDate)

fitLM1a_u = fixest::feols(ret_unscaled ~ postSample + postPub | pubname + calendarDate,
                       data = regData %>%
                         filter(calendarDate >= sampstart),
                       cluster = ~pubname+calendarDate)

# Outcome: DM benchmark return (unscaled)
# fitLM2_u = fixest::feols(matchRet_unscaled ~ postSample + postPub | pubname,
#                        data = regData %>%
#                          filter(calendarDate >= sampstart),
#                        cluster = ~pubname+calendarDate)
# 
# fitLM2a_u = fixest::feols(matchRet_unscaled ~ postSample + postPub | pubname + calendarDate,
#                        data = regData %>%
#                          filter(calendarDate >= sampstart),
#                        cluster = ~pubname+calendarDate)

# Outcome: difference in returns (unscaled)
# fitLM3_u = fixest::feols(diffRet_unscaled ~ postSample + postPub | pubname,
#                        data = regData %>%
#                          filter(calendarDate >= sampstart),
#                        cluster = ~pubname+calendarDate)
# 
# fitLM3a_u = fixest::feols(diffRet_unscaled ~ postSample + postPub | pubname + calendarDate,
#                        data = regData %>%
#                          filter(calendarDate >= sampstart),
#                        cluster = ~pubname+calendarDate)
# 
# fixest::etable(
#   list(fitLM1_u, fitLM1a_u, fitLM2_u, fitLM2a_u, fitLM3_u, fitLM3a_u),
#   tex = FALSE,
#   dict = etable_dict,
#   depvar = FALSE,
#   # headers = list(
#   #   "Published Return" = 1:2,
#   #   "DM Matched Return" = 3:4,
#   #   "Difference" = 5:6
#   # ),
#   fitstat = ~ n + r2 + wr2
# )


# fixest::etable(
#   list(fitLM1_u, fitLM1a_u, fitLM2_u, fitLM2a_u, fitLM3_u, fitLM3a_u),
#   tex = FALSE,
#   dict = etable_dict,
#   depvar = FALSE,
#   headers = c("Predictor Return", "Predictor Return", "DM Matched Return", "DM Matched Return", "Pred-Matched Ret", "Pred-Matched Ret"),
#   fitstat = ~ n + r2 + wr2
# )


# Correlation-Filtered DM Regressions ----------------------------------------
# Excludes DM strategies with high in-sample correlation to the published predictor
# before computing the DM mean. Similar to 4d approach but for the 4c1 matching set.

corr_threshold <- 0.10  # exclude strategies with aligned correlation > 10%

# Load matched strategy info (has 'cor' column from sumstats_for_DM_Strats)
plotdat0 <- readRDS("../Data/Processed/plotdat0.RDS")
matchinfo_all <- plotdat0$comp_matched

# Sign-adjust correlation and filter
# cor is raw cor(dm_ret, pub_ret); sign(rbar) aligns the DM strategy direction
matchinfo_filtered <- matchinfo_all %>%
  mutate(cor_aligned = cor * sign(rbar)) %>%
  filter(cor_aligned <= corr_threshold) %>%
  transmute(pubname, sweight, dmname, sign = sign(rbar), rbar) %>%
  setDT()

# Report filtering diagnostics
n_before <- matchinfo_all[, .(n = .N), by = pubname]
n_after  <- matchinfo_filtered[, .(n_after = .N), by = pubname]
n_summary <- n_before %>% left_join(n_after, by = "pubname") %>%
  mutate(n_after = coalesce(n_after, 0L))
cat("Correlation filter (<=", corr_threshold, "):\n")
cat("  Signals losing ALL matches:", sum(n_summary$n_after == 0), "\n")
cat("  Median strategies before:", median(n_summary$n), "after:", median(n_summary$n_after), "\n")

dropped_signals <- n_summary %>% filter(n_after == 0) %>% pull(pubname)
if (length(dropped_signals) > 0) {
  cat("  Dropped signals:", paste(dropped_signals, collapse = ", "), "\n")
}

# Load DM returns
DMname <- paste0('../Data/Processed/', globalSettings$dataVersion, ' LongShort.RData')
dm_rets <- readRDS(DMname)$ret
dm_info <- readRDS(DMname)$port_list
dm_rets <- dm_rets %>%
  left_join(dm_info %>% select(portid, sweight), by = "portid") %>%
  transmute(sweight, dmname = signalid, yearm, ret) %>%
  setDT()
rm(dm_info)

# Pre-filter dm_rets to matched strategies only (large memory savings)
dm_rets <- dm_rets[unique(matchinfo_filtered[, .(sweight, dmname)]),
                   on = c("sweight", "dmname"), nomatch = 0]

# data.table cartesian join: expand to (pubname, dmname, yearm)
dmPanel <- matchinfo_filtered[dm_rets, on = c("sweight", "dmname"),
                               allow.cartesian = TRUE, nomatch = 0]
rm(dm_rets)

# Compute scaled/unscaled returns
dmPanel[, `:=`(
  ret_scaled   = ret * sign / abs(rbar) * 100,
  ret_unscaled = ret * sign * 100
)]

# Add calendarDate for joining with regData
dmPanel[, calendarDate := yearm]

# Aggregate: mean across filtered strategies per (pubname, calendarDate)
dm_means_excl <- dmPanel[, .(
  matchRet_excl          = mean(ret_scaled,   na.rm = TRUE),
  matchRet_unscaled_excl = mean(ret_unscaled, na.rm = TRUE),
  n_dm_strats            = .N
), by = .(pubname, calendarDate)]

rm(dmPanel, matchinfo_all, matchinfo_filtered, plotdat0, n_before, n_after, n_summary, dropped_signals); gc()

# Merge filtered DM means into existing regData
regData_excl <- regData %>%
  left_join(dm_means_excl, by = c("pubname", "calendarDate")) %>%
  mutate(
    diffRet_excl          = ret - matchRet_excl,
    diffRet_unscaled_excl = ret_unscaled - matchRet_unscaled_excl
  ) %>%
  filter(!is.na(matchRet_excl))

cat("regData_excl rows:", nrow(regData_excl),
    "(original regData rows:", nrow(regData), ")\n")
cat("Unique predictors in regData_excl:", length(unique(regData_excl$pubname)),
    "(original:", length(unique(regData$pubname)), ")\n")

## --- Scaled regressions (excl correlated) ----
fitLM2_excl = fixest::feols(matchRet_excl ~ postSample + postPub | pubname,
                            data = regData_excl %>% filter(calendarDate >= sampstart),
                            cluster = ~pubname+calendarDate)

fitLM2a_excl = fixest::feols(matchRet_excl ~ postSample + postPub | pubname + calendarDate,
                             data = regData_excl %>% filter(calendarDate >= sampstart),
                             cluster = ~pubname+calendarDate)

fitLM3_excl = fixest::feols(diffRet_excl ~ postSample + postPub | pubname,
                            data = regData_excl %>% filter(calendarDate >= sampstart),
                            cluster = ~pubname+calendarDate)

fitLM3a_excl = fixest::feols(diffRet_excl ~ postSample + postPub | pubname + calendarDate,
                             data = regData_excl %>% filter(calendarDate >= sampstart),
                             cluster = ~pubname+calendarDate)

### Main Table ---- 
fixest::etable(
  list(fitLM1, fitLM1a, fitLM2_excl, fitLM2a_excl, fitLM3_excl, fitLM3a_excl),
  tex = TRUE,
  dict = etable_dict,
  style.tex = fixest::style.tex('aer'),
  digits = 3,
  digits.stats = "r3",
  signif.code=NA,
  depvar = FALSE,
  headers = c("Predictor Return", "Predictor Return", "DM Matched Return", "DM Matched Return", "Pred - Matched Ret", "Pred - Matched Ret"),
  fitstat = ~ n + r2 + wr2,
  file = '../../../risk-vs-rfs-sub/latex-risk-vs/exhibits/Table_MPStyleRegsMain.tex'
)



## --- Unscaled regressions (excl correlated) ----
fitLM2_excl_u = fixest::feols(matchRet_unscaled_excl ~ postSample + postPub | pubname,
                              data = regData_excl %>% filter(calendarDate >= sampstart),
                              cluster = ~pubname+calendarDate)

fitLM2a_excl_u = fixest::feols(matchRet_unscaled_excl ~ postSample + postPub | pubname + calendarDate,
                               data = regData_excl %>% filter(calendarDate >= sampstart),
                               cluster = ~pubname+calendarDate)

fitLM3_excl_u = fixest::feols(diffRet_unscaled_excl ~ postSample + postPub | pubname,
                              data = regData_excl %>% filter(calendarDate >= sampstart),
                              cluster = ~pubname+calendarDate)

fitLM3a_excl_u = fixest::feols(diffRet_unscaled_excl ~ postSample + postPub | pubname + calendarDate,
                               data = regData_excl %>% filter(calendarDate >= sampstart),
                               cluster = ~pubname+calendarDate)


### Supporting Table: Unscaled ---- 
fixest::etable(
  list(fitLM1_u, fitLM1a_u, fitLM2_excl_u, fitLM2a_excl_u, fitLM3_excl_u, fitLM3a_excl_u),
  tex = TRUE,
  dict = etable_dict,
  style.tex = fixest::style.tex('aer'),
  digits = 3,
  digits.stats = "r3",
  signif.code=NA,
  depvar = FALSE,
  headers = c("Predictor Return", "Predictor Return", "DM Matched Return", "DM Matched Return", "Pred - Matched Ret", "Pred - Matched Ret"),
  fitstat = ~ n + r2 + wr2,
  file = '../../../risk-vs-rfs-sub/latex-risk-vs/exhibits/Table_MPStyleRegsMainUnscaled.tex'
)


rm(dm_means_excl); gc()
# Note: regData_excl kept alive for the Theory Interaction Regressions section below.


# Individual DM Regressions -----------------------------------------------
# Uses individual DM strategy returns (not averaged) with the 4c1 matching set.
# To keep memory manageable we apply two filters to matchinfo BEFORE building the panel:
#   (1) Drop DM strategies with aligned in-sample correlation > 10% to the published signal
#       (same filter used in the Correlation-Filtered DM Regressions section above).
#   (2) Randomly subsample to at most max_strats_per_pub strategies per published signal.

corr_threshold      <- 0.10  # same threshold as correlation-filtered section
max_strats_per_pub  <- 100   # cap on DM strategies retained per published predictor
subsample_seed      <- 42    # for reproducibility

plotdat0 <- readRDS("../Data/Processed/plotdat0.RDS")
DMname <- paste0('../Data/Processed/', globalSettings$dataVersion, ' LongShort.RData')
dm_rets <- readRDS(DMname)$ret
dm_info <- readRDS(DMname)$port_list
dm_rets <- dm_rets %>%
  left_join(dm_info %>% select(portid, sweight), by = "portid") %>%
  transmute(sweight, dmname = signalid, yearm, ret) %>%
  setDT()
rm(dm_info)

# Build matchinfo with correlation filter applied
matchinfo <- plotdat0$comp_matched %>%
  mutate(cor_aligned = cor * sign(rbar)) %>%
  filter(cor_aligned <= corr_threshold) %>%
  transmute(pubname, sweight, dmname, sign = sign(rbar), rbar) %>%
  setDT()
rm(plotdat0)

# Diagnostic: count after correlation filter
n_after_corr <- matchinfo[, .N, by = pubname]
cat("After correlation filter (<=", corr_threshold, "):\n")
cat("  Predictors with >0 matches:", nrow(n_after_corr), "\n")
cat("  Total (pubname, dmname) pairs:", nrow(matchinfo), "\n")
cat("  Median strategies per predictor:", median(n_after_corr$N), "\n")
cat("  Max strategies per predictor:", max(n_after_corr$N), "\n")

# Random subsample: at most max_strats_per_pub per predictor
set.seed(subsample_seed)
matchinfo <- matchinfo[
  , .SD[sample(.N, min(.N, max_strats_per_pub))],
  by = pubname
]

# Diagnostic: count after subsampling
n_after_sub <- matchinfo[, .N, by = pubname]
cat("After random subsample (<=", max_strats_per_pub, " per predictor):\n")
cat("  Total (pubname, dmname) pairs:", nrow(matchinfo), "\n")
cat("  Median strategies per predictor:", median(n_after_sub$N), "\n")
cat("  Max strategies per predictor:", max(n_after_sub$N), "\n")
rm(n_after_corr, n_after_sub)

# Pre-filter dm_rets to matched strategies only (drops most rows)
dm_rets <- dm_rets[unique(matchinfo[, .(sweight, dmname)]),
                   on = c("sweight", "dmname"), nomatch = 0]

# data.table keyed join (more memory-efficient than dplyr many-to-many)
dmPanel <- matchinfo[dm_rets, on = c("sweight", "dmname"),
                     allow.cartesian = TRUE, nomatch = 0]
rm(dm_rets, matchinfo); gc()

dmPanel[, `:=`(
  candSignalname = dmname,
  calendarDate   = yearm,
  ret_scaled     = ret * sign / abs(rbar) * 100,
  ret_unscaled   = ret * sign * 100
)]

# Add sample/pub dates and indicators (in-place join, no copy)
pubdates <- czsum[, .(pubname = signalname, sampstart, sampend, pubdate)]
dmPanel[pubdates, on = "pubname", `:=`(
  sampstart = i.sampstart, sampend = i.sampend, pubdate = i.pubdate
)]
rm(pubdates)
# Non-overlapping postSample/postPub dummies, matching regData (line 45)
# so that fitDM coefficients are directly comparable to fitLM2/fitLM3.
dmPanel[, `:=`(
  postSample = fifelse(calendarDate >= sampend & calendarDate < pubdate, 1, 0),
  postPub    = fifelse(calendarDate >= pubdate, 1, 0)
)]
dmPanel <- dmPanel[calendarDate >= sampstart]

cat("Final individual DM panel rows:", nrow(dmPanel), "\n")

# Scaled
fitDM1 = fixest::feols(ret_scaled ~ postSample + postPub | dmname,
                       data = dmPanel, cluster = ~dmname+calendarDate)

fitDM1a = fixest::feols(ret_scaled ~ postSample + postPub | dmname + calendarDate,
                        data = dmPanel, cluster = ~dmname+calendarDate)

# Unscaled
fitDM2 = fixest::feols(ret_unscaled ~ postSample + postPub | dmname,
                       data = dmPanel, cluster = ~dmname+calendarDate)

fitDM2a = fixest::feols(ret_unscaled ~ postSample + postPub | dmname + calendarDate,
                        data = dmPanel, cluster = ~dmname+calendarDate)

## Save table ----
fixest::etable(
  list(fitDM1, fitDM1a, fitDM2, fitDM2a),
  tex = FALSE,
  dict = etable_dict,
  depvar = FALSE,
  # headers = list(
  #   "Scaled" = 1:2,
  #   "Unscaled" = 3:4
  # ),
  headers = c('Scaled ret', 'Scaled ret', 'Unscaled ret', 'Unscaled ret'),
  fitstat = ~ n + r2 + wr2
)


fixest::etable(
  list(fitDM1, fitDM1a, fitDM2, fitDM2a),
  tex = TRUE,
  dict = etable_dict,
  style.tex = fixest::style.tex('aer'),
  digits = 3,
  digits.stats = "r3",
  signif.code=NA,
  depvar = FALSE,
  headers = c('Scaled returns', 'Scaled returns', 'Unscaled returns', 'Unscaled returns'),
  fitstat = ~ n + r2 + wr2,
  file = '../../../risk-vs-rfs-sub/latex-risk-vs/exhibits/Table_MPStyleRegsIndividualDM.tex'
)

