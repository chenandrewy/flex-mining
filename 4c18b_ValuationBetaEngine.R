# Valuation-beta engine (Campbell NBER AP 2024 discussion, slide 17):
# For each CZ predictor, run monthly cross-sectional regressions
#     r_{i,t+1} = a_t + b_t * signal_{i,t}
# on the LEVEL of the signal (winsorized 1/99 by month, NOT rank-standardized:
# rank-standardizing would rebuild constant exposure and destroy the
# spread-compression mechanism the test is about).
#
# Per signal-month, also computes:
#   - spread_t  : mean(signal | top decile) - mean(signal | bottom decile)
#   - ls_ret    : EW extreme-decile long-short return (internal validation)
#   - b*spread  : regression-implied L/S return for that signal gap
#
# Inputs:  ../Data/Raw/CZ_FirmSignals/<signal>.parquet  (4c18a_DownloadFirmSignals.py)
#          ../Data/Raw/crspm.RData  (GHZ delisting adjustment applied here,
#          same as 2a_CompustatToLongshort.R)
#          ../Data/Raw/SignalDoc.csv (Sign column: sign so high signal = long leg)
# Output:  ../Data/Processed/valbeta_monthly.RDS
#          (per signal-month: n, b, se_b, a, spread, ls_ret, mean_x, sd_x)

# Setup --------------------------------------------------------

rm(list = ls())
source("0_Environment.R")
library(arrow)

min_obs_month <- 50 # min cross-sectional obs for a monthly regression

# CRSP return panel with GHZ delisting adjustment ---------------
# (identical treatment to 2a_CompustatToLongshort.R lines 192-224)

crsp_cache <- "../Data/Processed/crsp_ret_panel_ghz.RDS"
if (file.exists(crsp_cache)) {
  crsp <- readRDS(crsp_cache)
} else {
  # yearm is numeric year + (month-1)/12; convert robustly
  crsp <- readRDS("../Data/Raw/crspm.RData") %>%
    transmute(permno, yearm, ret, dlret, dlstcd, exchcd) %>%
    mutate(
      dlret = ifelse(
        is.na(dlret) & (dlstcd == 500 | (dlstcd >= 520 & dlstcd <= 584)) &
          (exchcd == 1 | exchcd == 2),
        -35, dlret
      ),
      dlret = ifelse(
        is.na(dlret) & (dlstcd == 500 | (dlstcd >= 520 & dlstcd <= 584)) &
          (exchcd == 3),
        -55, dlret
      ),
      dlret = ifelse(dlret < -100 & !is.na(dlret), -100, dlret),
      dlret = ifelse(is.na(dlret), 0, dlret),
      ret = 100 * ((1 + ret / 100) * (1 + dlret / 100) - 1),
      ret = ifelse(is.na(ret) & (dlret != 0), dlret, ret)
    ) %>%
    filter(!is.na(ret)) %>%
    mutate(
      yr = floor(yearm + 1e-6),
      mo = round(12 * (yearm - yr)) + 1,
      ret_yyyymm = yr * 100 + mo
    ) %>%
    transmute(permno, ret_yyyymm, ret) %>%
    as.data.table()
  saveRDS(crsp, crsp_cache)
}
setkey(crsp, permno, ret_yyyymm)
cat("CRSP return panel:", nrow(crsp), "obs,",
    min(crsp$ret_yyyymm), "-", max(crsp$ret_yyyymm), "\n")

# Signal doc: sign convention -----------------------------------

doc <- fread("../Data/Raw/SignalDoc.csv")
sign_map <- doc[`Cat.Signal` == "Predictor", .(signalname = Acronym, sgn = Sign)]
sign_map[is.na(sgn), sgn := 1]

# Month arithmetic helper: yyyymm + 1 month
next_month <- function(yyyymm) {
  yr <- yyyymm %/% 100
  mo <- yyyymm %% 100
  ifelse(mo == 12, (yr + 1) * 100 + 1, yyyymm + 1)
}

# Engine ---------------------------------------------------------

signal_files <- list.files("../Data/Raw/CZ_FirmSignals",
                           pattern = "\\.parquet$", full.names = TRUE)
cat("Signal files found:", length(signal_files), "\n")

process_signal <- function(f) {
  sname <- gsub("\\.parquet$", "", basename(f))
  dt <- as.data.table(read_parquet(f))
  setnames(dt, c("permno", "yyyymm", "x"))
  dt <- dt[!is.na(x) & is.finite(x)]
  if (nrow(dt) == 0) return(NULL)

  # sign so that high signal = long leg (published direction)
  sgn_i <- sign_map[signalname == sname, sgn]
  if (length(sgn_i) == 1 && !is.na(sgn_i)) dt[, x := sgn_i * x]

  # signal at t predicts return in t+1
  dt[, ret_yyyymm := next_month(yyyymm)]
  dt <- merge(dt, crsp, by = c("permno", "ret_yyyymm"))
  if (nrow(dt) == 0) return(NULL)

  # winsorize signal 1/99 within month
  dt[, `:=`(
    x_lo = quantile(x, 0.01, na.rm = TRUE),
    x_hi = quantile(x, 0.99, na.rm = TRUE)
  ), by = ret_yyyymm]
  dt[, x := pmin(pmax(x, x_lo), x_hi)]

  # monthly cross-sectional OLS (closed form) + decile spread + EW L/S
  out <- dt[, {
    n <- .N
    sdx <- sd(x)
    if (n >= min_obs_month && !is.na(sdx) && sdx > 0) {
      vx <- var(x)
      b <- cov(x, ret) / vx
      a <- mean(ret) - b * mean(x)
      resid <- ret - a - b * x
      se_b <- sqrt(sum(resid^2) / (n - 2) / ((n - 1) * vx))
      # rank-based deciles (robust to discrete signals with tied quantiles)
      dec <- ceiling(10 * frank(x, ties.method = "first") / n)
      hi <- dec == 10; lo <- dec == 1
      list(
        n = n, b = b, se_b = se_b, a = a,
        mean_x = mean(x), sd_x = sd(x),
        spread = mean(x[hi]) - mean(x[lo]),
        ls_ret = mean(ret[hi]) - mean(ret[lo])
      )
    } else {
      list(n = n, b = NA_real_, se_b = NA_real_, a = NA_real_,
           mean_x = NA_real_, sd_x = NA_real_,
           spread = NA_real_, ls_ret = NA_real_)
    }
  }, by = ret_yyyymm]

  out[, signalname := sname]
  out
}

tic("valuation-beta engine")
res_list <- lapply(seq_along(signal_files), function(i) {
  f <- signal_files[i]
  out <- tryCatch(process_signal(f), error = function(e) {
    cat("ERROR", basename(f), ":", conditionMessage(e), "\n")
    NULL
  })
  if (i %% 20 == 0) cat("  ...", i, "/", length(signal_files), "\n")
  out
})
toc()

valbeta <- rbindlist(res_list, use.names = TRUE)
setcolorder(valbeta, c("signalname", "ret_yyyymm"))
setorder(valbeta, signalname, ret_yyyymm)

cat("\nSignals processed:", uniqueN(valbeta$signalname), "\n")
cat("Signal-months:", nrow(valbeta), "\n")

saveRDS(valbeta, "../Data/Processed/valbeta_monthly.RDS")
cat("Saved ../Data/Processed/valbeta_monthly.RDS\n")
