# Tables 3-4 restricted to annual-accounting-only published predictors
# (Andrew's Aug 13 request, for the Jeff meeting). Same regressions and
# correlation-filtered DM benchmark as 4c6_MPStyleDecayTables.R; the only
# change is restricting published signals to the annual-Compustat subset
# used for Figure 2 panel (b) (selection copied from 4c20a_Fig2Data.R).
#
# Outputs (../risk-vs-rfs-sub/latex-risk-vs/exhibits/):
#   Table_MPStyleRegsMain_AcctOnly.tex
#   Table_MPStyleRegsMainUnscaled_AcctOnly.tex
# Hand-table mapping (same as the full-sample versions): no-time-FE table
# uses columns (1),(3),(5) of each; time-FE table uses columns (2),(4),(6).

# Setup --------------------------------------------------------

rm(list = ls())
source("0_Environment.R")

# Load and prep Data -------------------------------------------

inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType,
                                  topT = globalSettings$topT)

czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(Keep) %>%
  filter(signalname %in% inclSignals) %>%
  left_join(readRDS("../Data/Processed/czret_keeponly.RDS") %>%
              distinct(signalname, pubdate),
            by = "signalname") %>%
  setDT()

# Annual-accounting-only published signals (selection copied from 4c20a_Fig2Data.R)
czacct = readRDS('../Data/Processed/czsum_allpredictors.RDS') %>%
  left_join(fread('../Data/Raw/SignalDoc.csv') %>%
              transmute(Acronym, Cat.Data, Cat.Form, Def = tolower(`Detailed Definition`)),
            by = c('signalname' = 'Acronym')) %>%
  filter(signalname %in% inclSignals & Cat.Data == 'Accounting') %>%
  mutate(
    drop = FALSE
    , drop = if_else(grepl('quarter', Def), TRUE, drop)
    , drop = if_else(grepl('analyst|meanest|earningssurprise', paste(tolower(signalname), Def)), TRUE, drop)
    , drop = if_else(Cat.Form == 'discrete', TRUE, drop)
    , drop = if_else(signalname %in% c('ShareIss1Y', 'ShareIss5Y'), TRUE, drop)
  )
acct_signals = czacct[czacct$drop == FALSE, ]$signalname
print(paste0('Compustat Annual Accounting signals: ', length(acct_signals)))

ret_for_plot0 <- readRDS("../Data/Processed/ret_for_plot0.RDS")

regData = ret_for_plot0 %>%
  filter(pubname %in% acct_signals) %>%
  left_join(czsum %>%
              transmute(pubname = signalname,
                        sampstart,
                        sampend,
                        pubdate
                        )) %>%
  mutate(
    postSample = ifelse(calendarDate >= sampend, 1, 0),
    postPub    = ifelse(calendarDate >= pubdate, 1, 0)) %>%
  mutate(diffRet = ret - matchRet,
         diffRet_unscaled = ret_unscaled - matchRet_unscaled) %>%
  filter(complete.cases(.) == TRUE)

print(paste0('Predictors in regData: ', length(unique(regData$pubname))))

# Formatting for etable -------------------------------------------------------
etable_dict <- c(
  postSample             = "Post-Sample",
  postPub                = "Post-Pub",
  ret                    = "Return (scaled)",
  matchRet               = "DM Matched Return (scaled)",
  diffRet                = "Difference (scaled)",
  ret_unscaled           = "Return (unscaled)",
  matchRet_unscaled      = "DM Matched Return (unscaled)",
  diffRet_unscaled       = "Difference (unscaled)",
  pubname                = "Predictor",
  calendarDate           = "Month"
)

# Published-signal regressions ------------------------------------------------

fitLM1 = fixest::feols(ret ~ postSample + postPub | pubname,
                       data = regData %>% filter(calendarDate >= sampstart),
                       cluster = ~pubname+calendarDate)

fitLM1a = fixest::feols(ret ~ postSample + postPub | pubname + calendarDate,
                       data = regData %>% filter(calendarDate >= sampstart),
                       cluster = ~pubname+calendarDate)

fitLM1_u = fixest::feols(ret_unscaled ~ postSample + postPub | pubname,
                       data = regData %>% filter(calendarDate >= sampstart),
                       cluster = ~pubname+calendarDate)

fitLM1a_u = fixest::feols(ret_unscaled ~ postSample + postPub | pubname + calendarDate,
                       data = regData %>% filter(calendarDate >= sampstart),
                       cluster = ~pubname+calendarDate)

# Correlation-Filtered DM Regressions ----------------------------------------

corr_threshold <- 0.10

plotdat0 <- readRDS("../Data/Processed/plotdat0.RDS")
matchinfo_filtered <- plotdat0$comp_matched %>%
  filter(pubname %in% acct_signals) %>%
  mutate(cor_aligned = cor * sign(rbar)) %>%
  filter(cor_aligned <= corr_threshold) %>%
  transmute(pubname, sweight, dmname, sign = sign(rbar), rbar) %>%
  setDT()
rm(plotdat0)

DMname <- paste0('../Data/Processed/', globalSettings$dataVersion, ' LongShort.RData')
dm_rets <- readRDS(DMname)$ret
dm_info <- readRDS(DMname)$port_list
dm_rets <- dm_rets %>%
  left_join(dm_info %>% select(portid, sweight), by = "portid") %>%
  transmute(sweight, dmname = signalid, yearm, ret) %>%
  setDT()
rm(dm_info)

dm_rets <- dm_rets[unique(matchinfo_filtered[, .(sweight, dmname)]),
                   on = c("sweight", "dmname"), nomatch = 0]

dmPanel <- matchinfo_filtered[dm_rets, on = c("sweight", "dmname"),
                               allow.cartesian = TRUE, nomatch = 0]
rm(dm_rets)

dmPanel[, `:=`(
  ret_scaled   = ret * sign / abs(rbar) * 100,
  ret_unscaled = ret * sign * 100
)]
dmPanel[, calendarDate := yearm]

dm_means_excl <- dmPanel[, .(
  matchRet_excl          = mean(ret_scaled,   na.rm = TRUE),
  matchRet_unscaled_excl = mean(ret_unscaled, na.rm = TRUE),
  n_dm_strats            = .N
), by = .(pubname, calendarDate)]

rm(dmPanel, matchinfo_filtered); gc()

regData_excl <- regData %>%
  left_join(dm_means_excl, by = c("pubname", "calendarDate")) %>%
  mutate(
    diffRet_excl          = ret - matchRet_excl,
    diffRet_unscaled_excl = ret_unscaled - matchRet_unscaled_excl
  ) %>%
  filter(!is.na(matchRet_excl))

cat("regData_excl rows:", nrow(regData_excl),
    "(regData rows:", nrow(regData), ")\n")
cat("Unique predictors in regData_excl:", length(unique(regData_excl$pubname)),
    "(regData:", length(unique(regData$pubname)), ")\n")

## Scaled (excl correlated) ----
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

### Main Table (annual accounting only) ----
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
  file = '../risk-vs-rfs-sub/latex-risk-vs/exhibits/Table_MPStyleRegsMain_AcctOnly.tex'
)

## Unscaled (excl correlated) ----
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

### Supporting Table: Unscaled (annual accounting only) ----
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
  file = '../risk-vs-rfs-sub/latex-risk-vs/exhibits/Table_MPStyleRegsMainUnscaled_AcctOnly.tex'
)

# Console summary for quick reading -----------------------------------------
fixest::etable(
  list(fitLM1, fitLM2_excl, fitLM3_excl, fitLM1_u, fitLM2_excl_u, fitLM3_excl_u),
  tex = FALSE, dict = etable_dict, depvar = FALSE,
  headers = c("Pub scl", "DM scl", "Diff scl", "Pub unscl", "DM unscl", "Diff unscl"),
  fitstat = ~ n + r2 + wr2
)
cat("\n--- time FE versions ---\n")
fixest::etable(
  list(fitLM1a, fitLM2a_excl, fitLM3a_excl, fitLM1a_u, fitLM2a_excl_u, fitLM3a_excl_u),
  tex = FALSE, dict = etable_dict, depvar = FALSE,
  headers = c("Pub scl", "DM scl", "Diff scl", "Pub unscl", "DM unscl", "Diff unscl"),
  fitstat = ~ n + r2 + wr2
)
