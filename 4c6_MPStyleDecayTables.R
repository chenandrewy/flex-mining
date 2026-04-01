# This file produces regressions of the form 
# return = constant + post-sample + post-publication + fixed effects 
# in the published and data-mined predictors

# Fast Setup --------------------------------------------------------

rm(list = ls())
source("0_Environment.R")
library(doParallel)

## Load Global Data -------------------------------------------

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



# Add relevant sample and pub dates ---------------------------------------
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
  calendarDate           = "Month"
)

# Regressions -------------------------------------------------------------

# Outcome: Pub return
fitLM1 = fixest::feols(ret ~ postSample + postPub | pubname, 
                       data = regData %>% 
                         filter(calendarDate >= sampstart),
                       vcov = 'HC1')

fitLM1a = fixest::feols(ret ~ postSample + postPub | pubname + calendarDate, 
                       data = regData %>% 
                         filter(calendarDate >= sampstart),
                       vcov = 'HC1')

# Outcome: DM benchmark return
fitLM2 = fixest::feols(matchRet ~ postSample + postPub | pubname, 
                       data = regData %>% 
                         filter(calendarDate >= sampstart),
                       vcov = 'HC1')

fitLM2a = fixest::feols(matchRet ~ postSample + postPub | pubname + calendarDate, 
                       data = regData %>% 
                         filter(calendarDate >= sampstart),
                       vcov = 'HC1')

# Outcome: difference in returns (ret - matchret)
fitLM3 = fixest::feols(diffRet ~ postSample + postPub | pubname, 
                       data = regData %>% 
                         filter(calendarDate >= sampstart),
                       vcov = 'HC1')

fitLM3a = fixest::feols(diffRet ~ postSample + postPub | pubname + calendarDate, 
                       data = regData %>% 
                         filter(calendarDate >= sampstart),
                       vcov = 'HC1')

# Variant 3: LHS = any ret, RHS ~ dummies for pub and for dm
dat = regData %>% 
  transmute(ret,
            group = 'pub',
            calendarDate,
            postSample, 
            postPub,
            pubname) %>% 
  bind_rows(
    regData %>% 
      transmute(ret = matchRet,
                group = 'dm',
                calendarDate,
                postSample, 
                postPub,
                pubname)
  )

fitLM4 = fixest::feols(ret ~ group*(postSample + postPub) | pubname, 
              data = dat,
              vcov = 'HC1') 

fitLM4a = fixest::feols(ret ~ group*(postSample + postPub) | pubname + calendarDate, 
                       data = dat,
                       vcov = 'HC1') 

fixest::etable(
  list(fitLM1, fitLM1a, fitLM2, fitLM2a, fitLM3, fitLM3a, fitLM4, fitLM4a),
  tex = FALSE,
  dict = etable_dict,
  depvar = FALSE,
  # headers = list(
  #   "Published Return" = 1:2,
  #   "DM Matched Return" = 3:4,
  #   "Difference" = 5:6
  # ),
  fitstat = ~ n + r2 + wr2
)


fixest::etable(
  list(fitLM1, fitLM1a, fitLM2, fitLM2a, fitLM3, fitLM3a),
  tex = FALSE,
  dict = etable_dict,
  depvar = FALSE,
  headers = c("Predictor Return", "Predictor Return", "DM Matched Return", "DM Matched Return", "Pred-Matched Ret", "Pred-Matched Ret"),
  fitstat = ~ n + r2 + wr2
)


# Unscaled Regressions -------------------------------------------------------

# Outcome: Pub return (unscaled)
fitLM1_u = fixest::feols(ret_unscaled ~ postSample + postPub | pubname,
                       data = regData %>%
                         filter(calendarDate >= sampstart),
                       vcov = 'HC1')

fitLM1a_u = fixest::feols(ret_unscaled ~ postSample + postPub | pubname + calendarDate,
                       data = regData %>%
                         filter(calendarDate >= sampstart),
                       vcov = 'HC1')

# Outcome: DM benchmark return (unscaled)
fitLM2_u = fixest::feols(matchRet_unscaled ~ postSample + postPub | pubname,
                       data = regData %>%
                         filter(calendarDate >= sampstart),
                       vcov = 'HC1')

fitLM2a_u = fixest::feols(matchRet_unscaled ~ postSample + postPub | pubname + calendarDate,
                       data = regData %>%
                         filter(calendarDate >= sampstart),
                       vcov = 'HC1')

# Outcome: difference in returns (unscaled)
fitLM3_u = fixest::feols(diffRet_unscaled ~ postSample + postPub | pubname,
                       data = regData %>%
                         filter(calendarDate >= sampstart),
                       vcov = 'HC1')

fitLM3a_u = fixest::feols(diffRet_unscaled ~ postSample + postPub | pubname + calendarDate,
                       data = regData %>%
                         filter(calendarDate >= sampstart),
                       vcov = 'HC1')

fixest::etable(
  list(fitLM1_u, fitLM1a_u, fitLM2_u, fitLM2a_u, fitLM3_u, fitLM3a_u),
  tex = FALSE,
  dict = etable_dict,
  depvar = FALSE,
  # headers = list(
  #   "Published Return" = 1:2,
  #   "DM Matched Return" = 3:4,
  #   "Difference" = 5:6
  # ),
  fitstat = ~ n + r2 + wr2
)


fixest::etable(
  list(fitLM1_u, fitLM1a_u, fitLM2_u, fitLM2a_u, fitLM3_u, fitLM3a_u),
  tex = FALSE,
  dict = etable_dict,
  depvar = FALSE,
  headers = c("Predictor Return", "Predictor Return", "DM Matched Return", "DM Matched Return", "Pred-Matched Ret", "Pred-Matched Ret"),
  fitstat = ~ n + r2 + wr2
)


