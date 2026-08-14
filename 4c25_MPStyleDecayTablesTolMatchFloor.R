# 4c22 variant with the 60-month minimum in-sample history floor applied to
# the tolerance-matched pairs (completing the 'tbc: make flexible' screen that
# SelectDMStrats has had since 05669a0 but 2b never received). Specs:
#   Tol30Floor / Tol10Floor: as 4c22's Tol30/Tol10 + pair nmonth_insamp >= 60.
# Published-return columns are re-estimated on each spec's surviving pubs so
# all columns in a table share a sample. Prints loss diagnostics vs the
# baseline |t|>2 benchmark of 4c6.
#
# Outputs: ../Results/TolMatch/Table_MPStyleRegs{Main,MainUnscaled}_{Tol30,Tol10}Floor.tex

rm(list = ls())
source("0_Environment.R")
source("helpers/mp_table_helpers.R")

outdir = '../Results/TolMatch'
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

# Load and prep (as 4c6) ------------------------------------------------------

inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType,
                                  topT = globalSettings$topT)

czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(Keep) %>%
  filter(signalname %in% inclSignals) %>%
  left_join(readRDS("../Data/Processed/czret_keeponly.RDS") %>%
              distinct(signalname, pubdate),
            by = "signalname") %>%
  setDT()

ret_for_plot0 <- readRDS("../Data/Processed/ret_for_plot0.RDS")

regData = ret_for_plot0 %>%
  left_join(czsum %>%
              transmute(pubname = signalname, sampstart, sampend, pubdate)) %>%
  mutate(
    postSample = ifelse(calendarDate >= sampend, 1, 0),
    postPub    = ifelse(calendarDate >= pubdate, 1, 0)) %>%
  filter(!is.na(ret), !is.na(ret_unscaled))

# Tolerance-matched pairs (2b: 10% t, 30% rbar; sign-aligned returns) ---------

matchname = paste0('../Data/Processed/', globalSettings$dataVersion, ' MatchPub.RData')
tmp = readRDS(matchname)
candidateReturns = tmp$candidateReturns %>%
  filter(actSignal %in% czsum$signalname) %>%
  setDT()
rm(tmp); gc()

# per-pair in-sample mean and the extra 10% mean-return screen (as 4c20c)
rbar_pair = candidateReturns[samptype == 'insamp',
                             .(rbar_insampMatched = mean(ret),
                               nmonth_insamp = sum(!is.na(ret))),
                             by = .(actSignal, candSignalname)] %>%
  left_join(czsum %>% transmute(actSignal = signalname, rbar_op = rbar),
            by = 'actSignal') %>%
  mutate(tight = abs(rbar_insampMatched - rbar_op) / rbar_op <= 0.1) %>%
  setDT()

# correlation filter (as 4c6/4c20c: aligned in-sample corr <= 0.10)
allRhos = readRDS('../Results/PairwiseCorrelationsActualAndMatches.RDS') %>% setDT()
rbar_pair = allRhos[, .(actSignal, candSignalname = candidateSignal, rho)][
  rbar_pair, on = c('actSignal', 'candSignalname')]
rbar_pair[, keep_corr := rho <= 0.10]

# Diagnostics ----------------------------------------------------------------
n_pubs_all = length(unique(regData$pubname))
diag_pairs = rbar_pair[, .(
  pairs      = .N,
  pairs_corr = sum(keep_corr, na.rm = TRUE),
  pairs_t10  = sum(tight, na.rm = TRUE),
  pairs_t10c = sum(tight & keep_corr, na.rm = TRUE)
), by = actSignal]
cat('=== Sample loss diagnostics (universe:', n_pubs_all, 'published predictors) ===\n')
cat('Tol30 (10% t / 30% rbar):        pubs with >=1 pair:',
    sum(diag_pairs$pairs > 0), '\n')
cat('Tol30 + cor<=10%:                pubs with >=1 pair:',
    sum(diag_pairs$pairs_corr > 0), '\n')
cat('Tol10 (10% t / 10% rbar):        pubs with >=1 pair:',
    sum(diag_pairs$pairs_t10 > 0), '\n')
cat('Tol10 + cor<=10%:                pubs with >=1 pair:',
    sum(diag_pairs$pairs_t10c > 0), '\n')
cat('Median pairs per pub: Tol30corr', median(diag_pairs$pairs_corr),
    '| Tol10corr', median(diag_pairs$pairs_t10c), '\n')

# DM event-time series per spec ----------------------------------------------

cat('Pairs failing the 60-month floor:', rbar_pair[nmonth_insamp < 60, .N],
    'of', nrow(rbar_pair), '\n')
pairsets = list(
  Tol30Floor = rbar_pair[keep_corr == TRUE & nmonth_insamp >= 60,
                         .(actSignal, candSignalname, rbar_insampMatched)],
  Tol10Floor = rbar_pair[tight == TRUE & keep_corr == TRUE & nmonth_insamp >= 60,
                         .(actSignal, candSignalname, rbar_insampMatched)]
)

dm_series = list()
for (nm in names(pairsets)) {
  cand = candidateReturns[pairsets[[nm]], on = c('actSignal', 'candSignalname'),
                          nomatch = 0]
  dm_series[[nm]] = cand[, .(
    matchRet          = mean(100 * ret / rbar_insampMatched, na.rm = TRUE),
    matchRet_unscaled = mean(100 * ret, na.rm = TRUE),
    n_dm = .N
  ), by = .(pubname = actSignal, eventDate)]
  rm(cand); gc()
}

# Regressions per spec --------------------------------------------------------

etable_dict <- c(
  postSample = "Post-Sample", postPub = "Post-Pub",
  pubname = "Predictor", calendarDate = "Month"
)

run_spec = function(nm) {
  rd = regData %>%
    select(-matchRet, -matchRet_unscaled) %>%
    inner_join(dm_series[[nm]], by = c('pubname', 'eventDate')) %>%
    mutate(diffRet = ret - matchRet,
           diffRet_unscaled = ret_unscaled - matchRet_unscaled) %>%
    filter(calendarDate >= sampstart)

  cat('\n===', nm, ': pubs', length(unique(rd$pubname)),
      '| signal-months', nrow(rd), '===\n')

  f = function(lhs, fe) fixest::feols(
    as.formula(paste0(lhs, ' ~ postSample + postPub | ', fe)),
    data = rd, cluster = ~pubname + calendarDate)

  fits_s = list(f('ret', 'pubname'), f('ret', 'pubname + calendarDate'),
                f('matchRet', 'pubname'), f('matchRet', 'pubname + calendarDate'),
                f('diffRet', 'pubname'), f('diffRet', 'pubname + calendarDate'))
  fits_u = list(f('ret_unscaled', 'pubname'), f('ret_unscaled', 'pubname + calendarDate'),
                f('matchRet_unscaled', 'pubname'), f('matchRet_unscaled', 'pubname + calendarDate'),
                f('diffRet_unscaled', 'pubname'), f('diffRet_unscaled', 'pubname + calendarDate'))

  hdrs = c("Predictor Return", "Predictor Return", "DM Matched Return",
           "DM Matched Return", "Pred - Matched Ret", "Pred - Matched Ret")
  for (tabs in list(list(fits_s, 'Main'), list(fits_u, 'MainUnscaled'))) {
    fixest::etable(tabs[[1]], tex = TRUE, dict = etable_dict,
                   style.tex = fixest::style.tex('aer'),
                   digits = 3, digits.stats = "r3", signif.code = NA,
                   depvar = FALSE, headers = hdrs, fitstat = ~ n + r2 + wr2,
                   file = paste0(outdir, '/Table_MPStyleRegs', tabs[[2]], '_', nm, '.tex'))
  }
  saveRDS(list(fits_s = fits_s, fits_u = fits_u,
               meta = list(generated = Sys.time(),
                           input_files = c('../Data/Processed/ret_for_plot0.RDS',
                                           matchname,
                                           '../Results/PairwiseCorrelationsActualAndMatches.RDS'))),
          paste0('../Data/Processed/mp_decay_fits_', nm, '.RDS'))

  # combined manuscript-layout drafts (for the in-paper spec comparison)
  make_combined_table(c(fits_s[c(1, 3, 5)], fits_u[c(1, 3, 5)]), timeFE = FALSE,
    file = paste0('../risk-vs-rfs-sub/latex-risk-vs/exhibits/HandTable_MPStyleRegsMain_', nm, '.tex'))
  make_combined_table(c(fits_s[c(2, 4, 6)], fits_u[c(2, 4, 6)]), timeFE = TRUE,
    file = paste0('../risk-vs-rfs-sub/latex-risk-vs/exhibits/HandTable_MPStyleRegsTimeFE_', nm, '.tex'))

  # console: no-FE and FE columns, scaled then unscaled
  print(fixest::etable(fits_s[c(1, 3, 5)], tex = FALSE, dict = etable_dict, depvar = FALSE,
                       headers = c('Pub scl', 'DM scl', 'Diff scl'), fitstat = ~n))
  print(fixest::etable(fits_s[c(2, 4, 6)], tex = FALSE, dict = etable_dict, depvar = FALSE,
                       headers = c('Pub sclFE', 'DM sclFE', 'Diff sclFE'), fitstat = ~n))
  print(fixest::etable(fits_u[c(1, 3, 5)], tex = FALSE, dict = etable_dict, depvar = FALSE,
                       headers = c('Pub un', 'DM un', 'Diff un'), fitstat = ~n))
  print(fixest::etable(fits_u[c(2, 4, 6)], tex = FALSE, dict = etable_dict, depvar = FALSE,
                       headers = c('Pub unFE', 'DM unFE', 'Diff unFE'), fitstat = ~n))
  invisible(NULL)
}

for (nm in names(pairsets)) run_spec(nm)
