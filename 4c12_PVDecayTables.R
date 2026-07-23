# This file produces mean returns and MP-style decay regressions
#   return = constant + post-sample + post-publication + fixed effects
# split by whether a published predictor uses PRESENT-VALUE (PV) logic.
#
# Motivation: Campbell (NBER AP 2024 discussion) proposes classifying predictors
# by whether they use present-value logic (compare market price to a fundamental
# value from discounting cash flows), and asks how many published vs data-mined
# anomalies are in this category, and whether they behave differently.
#
# PV flags come from DataInput/SignalsTheoryChecked_withPV.csv (a NON-destructive
# merge onto the original SignalsTheoryChecked.csv; original is left untouched).
# Two specs:
#   pv_narrow : price-scaled valuation ratios only (BM, EP, SP, CF/P, ...)
#   pv_broad  : narrow + QMJ-style profitability/quality (GP, OperProf, RoE, ...)
#
# Structure and conventions follow 4c6_MPStyleDecayTables.R.

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

# Fail fast if the cached czsum predates the pipeline change that added pubdate
# (rerun 1_Download_and_Clean.R and 4c1_ResearchVsDMprep.R to refresh caches).
req_cols <- c("sampstart", "sampend", "pubdate")
if (!all(req_cols %in% names(czsum))) {
  stop("czsum_allpredictors.RDS is missing: ",
       paste(setdiff(req_cols, names(czsum)), collapse = ", "),
       ". Rerun 1_Download_and_Clean.R (and 4c1_ResearchVsDMprep.R) to refresh caches.")
}

# PV classification (NEW merged file; original SignalsTheoryChecked.csv untouched).
# Robust boolean parse: the CSV stores Python-style "True"/"False" strings.
czpv <- fread("DataInput/SignalsTheoryChecked_withPV.csv") %>%
  transmute(signalname,
            Year, theory, Journal,
            pv_narrow = toupper(trimws(as.character(pv_narrow))) == "TRUE",
            pv_broad  = toupper(trimws(as.character(pv_broad)))  == "TRUE") %>%
  filter(signalname %in% inclSignals)

# Sanity: every included signal should have a PV flag
missing_pv <- setdiff(unique(czsum$signalname), czpv$signalname)
if (length(missing_pv) > 0) {
  cat("WARNING: signals with no PV classification (dropped from PV splits):",
      paste(missing_pv, collapse = ", "), "\n")
}

# Load pre-computed matched returns
ret_for_plot0 <- readRDS("../Data/Processed/ret_for_plot0.RDS")

# Add relevant sample and pub dates + PV flags (mirrors 4c6 regData construction)
regData = ret_for_plot0 %>%
  left_join(czsum %>%
              transmute(pubname = signalname,
                        sampstart,
                        sampend,
                        pubdate)) %>%
  # PV flags only (theory already present on ret_for_plot0; avoid a duplicate join)
  left_join(czpv %>%
              transmute(pubname = signalname, pv_narrow, pv_broad),
            by = "pubname") %>%
  # Add indicators (non-overlapping post-sample / post-pub, as in 4c6 line 45)
  mutate(postSample = ifelse(calendarDate >= sampend & calendarDate < pubdate, 1, 0),
         postPub    = ifelse(calendarDate >= pubdate, 1, 0)) %>%
  # Add outcome (only the scaled difference is used below)
  mutate(diffRet = ret - matchRet)

# Keep rows with complete returns/dates (matches 4c6). PV flags handled separately
# below so a signal missing a PV label does not silently drop return rows.
regData <- regData %>%
  filter(complete.cases(ret, matchRet, sampstart, sampend, pubdate, calendarDate))

cat("regData rows:", nrow(regData),
    "| rows with pv_narrow:", sum(!is.na(regData$pv_narrow)),
    "| rows with pv_broad:", sum(!is.na(regData$pv_broad)), "\n")


# Formatting for etable -------------------------------------------------------
etable_dict <- c(
  postSample           = "Post-Sample",
  postPub              = "Post-Pub",
  PV                   = "PV",
  "postSample:PV"      = "Post-Sample x PV",
  "postPub:PV"         = "Post-Pub x PV",
  ret                  = "Return (scaled)",
  matchRet             = "DM Matched Return (scaled)",
  diffRet              = "Difference (scaled)",
  pubname              = "Predictor",
  calendarDate         = "Month"
)


# Mean returns by PV group ("mean return by PV or not") -----------------------
# Period definitions match the indicators above:
#   in-sample : sampstart <= t < sampend
#   post-samp : sampend   <= t < pubdate
#   post-pub  : t >= pubdate
mean_by_group <- function(dat, flagcol) {
  dat %>%
    filter(!is.na(.data[[flagcol]])) %>%
    mutate(period = case_when(
      calendarDate >= sampstart & calendarDate < sampend  ~ "InSample",
      calendarDate >= sampend   & calendarDate < pubdate  ~ "PostSample",
      calendarDate >= pubdate                             ~ "PostPub",
      TRUE ~ NA_character_
    )) %>%
    filter(!is.na(period)) %>%
    group_by(PVgroup = ifelse(.data[[flagcol]], "PV", "non-PV"), period) %>%
    summarise(n_signals = n_distinct(pubname),
              n_obs     = n(),
              mean_ret  = mean(ret, na.rm = TRUE),
              mean_diff = mean(diffRet, na.rm = TRUE),
              .groups = "drop") %>%
    arrange(PVgroup, factor(period, levels = c("InSample", "PostSample", "PostPub")))
}

cat("\n===== Mean scaled returns by PV group (NARROW spec) =====\n")
print(mean_by_group(regData, "pv_narrow"))
cat("\n===== Mean scaled returns by PV group (BROAD spec) =====\n")
print(mean_by_group(regData, "pv_broad"))


# Decay regressions by PV group ----------------------------------------------
# Specification follows 4c6: ret ~ postSample + postPub | FE, clustered by
# predictor and calendar month, restricted to t >= sampstart. We report BOTH
# fixed-effect variants used in 4c6 (| pubname  and  | pubname + calendarDate).
#
# Two complementary outputs per spec:
#   (A) DESCRIPTIVE subsample table: PV and non-PV fit separately (side by side).
#   (B) INTERACTION test: a single fit on the pooled spec sample with PV dummy
#       interacted with the decay indicators. The postPub:PV coefficient is the
#       DIRECT, jointly-estimated test of whether PV signals decay differently
#       post-publication -- which subsample fits cannot provide.
#
# Note on the "Pred - DM" columns: diffRet = ret - matchRet uses the *unfiltered*
# average DM match from ret_for_plot0. The paper's headline MP table
# (Table_MPStyleRegsMain) instead uses a correlation-filtered DM benchmark
# (corr <= 0.10). These columns are labelled "Pred-DM (raw)" to avoid confusion;
# swapping in the corr-filtered benchmark is a natural follow-up.

# 4c6-style single fit: outcome ~ postSample + postPub | FE
fit_decay <- function(dat, yvar, fe = "pubname") {
  fixest::feols(
    stats::as.formula(paste0(yvar, " ~ postSample + postPub | ", fe)),
    data    = dat %>% filter(calendarDate >= sampstart),
    cluster = ~ pubname + calendarDate
  )
}

# Interaction fit: outcome ~ (postSample + postPub) * PV | FE, on pooled sample
fit_interact <- function(dat, yvar, fe = "pubname") {
  fixest::feols(
    stats::as.formula(paste0(yvar, " ~ postSample + postPub + postSample:PV + postPub:PV | ", fe)),
    data    = dat %>% filter(calendarDate >= sampstart),
    cluster = ~ pubname + calendarDate
  )
}

run_spec <- function(flagcol, tex_main, tex_interact, spec_label) {
  # per-spec sample: only rows whose flag for THIS spec is present
  dat <- regData %>%
    filter(!is.na(.data[[flagcol]])) %>%
    mutate(PV = as.integer(.data[[flagcol]]))   # 1 = PV, 0 = non-PV

  pv    <- dat %>% filter(PV == 1L)
  nonpv <- dat %>% filter(PV == 0L)

  cat("\n[", spec_label, "] PV predictors:", length(unique(pv$pubname)),
      "| non-PV predictors:", length(unique(nonpv$pubname)),
      "| pooled predictors:", length(unique(dat$pubname)), "\n")

  # (A) Descriptive subsample table: PV return, non-PV return (both FE variants)
  fit_pv_1     <- fit_decay(pv,    "ret", "pubname")
  fit_pv_2     <- fit_decay(pv,    "ret", "pubname + calendarDate")
  fit_nonpv_1  <- fit_decay(nonpv, "ret", "pubname")
  fit_nonpv_2  <- fit_decay(nonpv, "ret", "pubname + calendarDate")
  # raw predictor-minus-DM difference (unfiltered DM), pubname FE only
  fit_pv_diff    <- fit_decay(pv,    "diffRet", "pubname")
  fit_nonpv_diff <- fit_decay(nonpv, "diffRet", "pubname")

  main_list    <- list(fit_pv_1, fit_nonpv_1, fit_pv_2, fit_nonpv_2, fit_pv_diff, fit_nonpv_diff)
  main_headers <- c("PV Return", "non-PV Return", "PV Return", "non-PV Return",
                    "PV Pred-DM (raw)", "non-PV Pred-DM (raw)")

  fixest::etable(main_list, tex = FALSE, dict = etable_dict, depvar = FALSE,
                 headers = main_headers, fitstat = ~ n + r2 + wr2)
  fixest::etable(main_list, tex = TRUE, dict = etable_dict,
                 style.tex = fixest::style.tex('aer'),
                 digits = 3, digits.stats = "r3", signif.code = NA, depvar = FALSE,
                 headers = main_headers, fitstat = ~ n + r2 + wr2, file = tex_main)

  # (B) Interaction test (the difference is postPub:PV)
  fit_int_1 <- fit_interact(dat, "ret", "pubname")
  fit_int_2 <- fit_interact(dat, "ret", "pubname + calendarDate")
  int_list    <- list(fit_int_1, fit_int_2)
  int_headers <- c("Return (pub FE)", "Return (pub + month FE)")

  cat("\n[", spec_label, "] Interaction test (postPub:PV = differential post-pub decay):\n")
  fixest::etable(int_list, tex = FALSE, dict = etable_dict, depvar = FALSE,
                 headers = int_headers, fitstat = ~ n + r2 + wr2)
  fixest::etable(int_list, tex = TRUE, dict = etable_dict,
                 style.tex = fixest::style.tex('aer'),
                 digits = 3, digits.stats = "r3", signif.code = NA, depvar = FALSE,
                 headers = int_headers, fitstat = ~ n + r2 + wr2, file = tex_interact)
}

# Output dir matches the working repo convention (fix_fig7_legend.R): one level up.
exhibits <- "../risk-vs-rfs-sub/latex-risk-vs/exhibits/"
run_spec("pv_narrow",
         paste0(exhibits, "Table_PVDecayNarrow.tex"),
         paste0(exhibits, "Table_PVDecayNarrowInteract.tex"),
         "NARROW")
run_spec("pv_broad",
         paste0(exhibits, "Table_PVDecayBroad.tex"),
         paste0(exhibits, "Table_PVDecayBroadInteract.tex"),
         "BROAD")

cat("\nDone. Wrote PV decay tables (narrow/broad, descriptive + interaction) to\n  ",
    normalizePath(exhibits, mustWork = FALSE), "\n")
