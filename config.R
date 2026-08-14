# Run configuration for the flex-mining pipeline.
#
# How to run: this file is data only -- no packages, no functions. It is sourced
#   by 0_Environment.R (so every chapter script inherits globalSettings) and by
#   MAIN.R (which reads runStages to decide which chapters to run). The two
#   chapter drivers that only need a versioned path (3_Precompute.R,
#   7_BestPredictors.R) source it directly to read globalSettings$dataVersion.
# Inputs:  none.
# Outputs: defines globalSettings, runStages; sets the RNG seed.
#
# Keep this file free of library() calls and function definitions so it stays
# cheap to source standalone. Reference data (compnames, colors) and the R
# session bootstrap live in 0_Environment.R.

# Pipeline stage switches (read by MAIN.R) ---------------------------------
# Each chapter runs as its own Rscript subprocess; see MAIN.R.
runStages <- list(
  download_and_clean       = FALSE,  # Re-pull ../Data/Raw; changes the vintage
  data_mining              = FALSE,  # Chapter 2; hours
  precompute               = TRUE,   # Chapter 3; slow reusable analysis
  research_vs_data_mining  = TRUE,   # Chapter 4; intro and Section 2
  learning                 = TRUE,   # Chapter 5; Section 3
  heterogeneity            = TRUE,   # Chapter 6; Section 4
  best_predictors          = TRUE,   # Chapter 7; Section 4b
  appendices               = TRUE,   # Chapter 8
  export_data_to_csv       = TRUE    # Chapter 9
)

# Consequential run choices (affect expensive cached outputs) --------------
globalSettings = list(
  dataVersion  = 'CZ-style-v8b',

  # published signal choices
  restrictType = 'topT', # 'topT' or NULL for all signals
  topT         = 2, # number of top t-stat signals to keep from each paper

  # signal choices
  minNumStocks   = 20, # Minimum number of stocks in any month over the in-sample period to include a DM strategy for matching to published strategies (ie minNumStocks/2 in each leg)
  signalnum      = Inf, # number of signals to sample or Inf for all
  form           = c('v1/v2', 'diff(v1)/lag(v2)'), # 'pdiff(v1/v2)', 'pdiff(v1)', 'diff(v1/v2)', 'pdiff(v1)-pdiff(v2)')
  denom_min_fobs = 0.25, # minimum fraction of non-missing observations in 1963
  # portfolio choices
  longshort_form = 'ls_extremes',
  portnum        = c(10),
  sweight        = c('ew','vw'),
  trim           = NA_real_,  # NA or some quantile e.g. .005
  # data basic choices
  backfill_dropyears = 0, # number of years to drop for backfill bias adj (the CZ repo lacks this adjustment)
  reup_months        = 6, # stocks are traded using new data at end of these months
  data_avail_lag     = 6, # months
  toostale_months    = 18, # months after datadate to keep signal for
  delist_adj         = 'ghz', # 'none' or 'ghz'
  crsp_filter        = NA_character_, # use NA_character_ for no filter
  nmonth_min         = 120, # minimum number of months to keep DM signal in EZ themes code

  # debugging
  prep_data = T,
  num_cores = round(.4*parallel::detectCores()),  # Adjust number of cores used as you see fit (use num_cores = 1 for serial)
  shortlist = F,
  interactive_mode = FALSE,  # Set to TRUE for interactive execution

  # DM vs OP matching requirements
  t_tol    = .1*Inf, # tolerance in t-statistics (DM vs OP) for matching
  r_tol    = .3*Inf, # tolerance in mean return (DM vs OP) for matching
  t_reltol = .1*Inf, # relative (to OP) tolerance in t-statistics (DM vs OP) for matching
  r_reltol = .3*Inf, # relative (to OP) tolerance in mean return (DM vs OP) for matching
  t_min    = 2,  # minimum screened t-stat
  t_max    = Inf, # maximum screened t-stat
  t_rankpct_min = 100, # top x% of data mined t-stats, 100% for off

  # DM requirements
  minShareTG2 = .1,  # Include strategies with t-stat > 2 in at least X % of published time periods
  TG2Set = '1994-2020', # 1994-2020: DM strategies evaluated over 1994-2020
                        # Matches:   all sample matching periods
                        # Rolling1994-2020: DM strategies evaluated on rolling t-stats in 1994-2020

  # Finance and Accounting journals
  finlistAll  = c('JF','RFS','JFE','JFQA','MS', 'ROF', 'JEmpFin', 'JFM'),
  acctlistAll = c('AR','RAS','JAR','JAE', 'CAR', 'BAR', 'JBFA'),

  # Top 3 journals for main analysis
  top3Finance = c('JF', 'RFS', 'JFE'),
  top3Accounting = c('AR', 'JAR', 'JAE')
)

# Set seed for random sampling (affects cached sampling)
set.seed(1337)
