# Bootstrap standard errors for the empirical Bayes decomposition (4c10c).
# Two cluster bootstraps:
#   A. Predictor cluster: resample the 167 signals with replacement.
#   B. Calendar-month cluster: resample OOS calendar months with replacement,
#      recompute each signal's OOS mean with month multiplicities.
# Both are conditional on the estimated prior parameters; parameter uncertainty
# is characterized separately by the likelihood-ridge range in 4c10c.

rm(list = ls())
source("0_Environment.R")

set.seed(2026)
B <- 2000

decomp <- readRDS("../Data/Processed/eb_decomposition_Exact.RDS")
ret_for_plot0 <- readRDS("../Data/Processed/ret_for_plot0.RDS")
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>% filter(Keep)

signal_info <- czsum %>% transmute(signalname, rbar_pub = rbar)

panel <- ret_for_plot0 %>%
  filter(!is.na(matchRet), eventDate > 0, pubname %in% decomp$pubname) %>%
  left_join(signal_info, by = c("pubname" = "signalname")) %>%
  transmute(pubname, calendarDate,
            ret_raw = ret * rbar_pub / 100,
            matchRet_raw = matchRet * rbar_pub / 100)

# month x signal matrices
months <- sort(unique(panel$calendarDate))
sigs <- decomp$pubname
mi <- match(panel$calendarDate, months)
si <- match(panel$pubname, sigs)
Rpub <- matrix(NA_real_, length(months), length(sigs))
Rdm <- matrix(NA_real_, length(months), length(sigs))
Rpub[cbind(mi, si)] <- panel$ret_raw
Rdm[cbind(mi, si)] <- panel$matchRet_raw

# per-signal constants
d <- decomp[match(sigs, decomp$pubname), ]
stopifnot(all(d$pubname == sigs))

# sanity: matrix-based OOS means reproduce the decomposition
oos_chk <- colMeans(Rpub, na.rm = TRUE)
stopifnot(max(abs(oos_chk - d$pub_oos)) < 1e-10)
oos_dm_chk <- colMeans(Rdm, na.rm = TRUE)
stopifnot(max(abs(oos_dm_chk - d$dm_oos)) < 1e-10)

stats_fun <- function(pub_is, pub_adj, pub_adj_dm, pub_oos, dm_is, dm_adj, dm_oos) {
  pub_total <- mean(pub_is - pub_oos)
  dm_total <- mean(dm_is - dm_oos)
  c(
    pub_total = pub_total,
    pub_sel_own = mean(pub_is - pub_adj),
    pub_learn_own = mean(pub_adj - pub_oos),
    pub_selshare_own = mean(pub_is - pub_adj) / pub_total * 100,
    pub_sel_dm = mean(pub_is - pub_adj_dm),
    pub_learn_dm = mean(pub_adj_dm - pub_oos),
    pub_selshare_dm = mean(pub_is - pub_adj_dm) / pub_total * 100,
    dm_total = dm_total,
    dm_sel = mean(dm_is - dm_adj),
    dm_learn = mean(dm_adj - dm_oos),
    dm_selshare = mean(dm_is - dm_adj) / dm_total * 100,
    learn_diff_own = mean(pub_adj - pub_oos) - mean(dm_adj - dm_oos),
    learn_diff_dmprior = mean(pub_adj_dm - pub_oos) - mean(dm_adj - dm_oos)
  )
}

point <- stats_fun(d$pub_is, d$pub_adj, d$pub_adj_dm, d$pub_oos,
                   d$dm_is_raw, d$dm_adj_raw, d$dm_oos)

# --- A. Predictor cluster bootstrap ---
N <- length(sigs)
bootA <- replicate(B, {
  idx <- sample.int(N, N, replace = TRUE)
  stats_fun(d$pub_is[idx], d$pub_adj[idx], d$pub_adj_dm[idx], d$pub_oos[idx],
            d$dm_is_raw[idx], d$dm_adj_raw[idx], d$dm_oos[idx])
})
seA <- apply(bootA, 1, sd)

# --- B. Calendar-month cluster bootstrap ---
Tm <- length(months)
obs_pub <- !is.na(Rpub)
obs_dm <- !is.na(Rdm)
Rpub0 <- ifelse(obs_pub, Rpub, 0)
Rdm0 <- ifelse(obs_dm, Rdm, 0)

bootB <- replicate(B, {
  w <- tabulate(sample.int(Tm, Tm, replace = TRUE), nbins = Tm)
  den_pub <- crossprod(w, obs_pub)[1, ]
  den_dm <- crossprod(w, obs_dm)[1, ]
  oos_pub <- ifelse(den_pub > 0, crossprod(w, Rpub0)[1, ] / den_pub, d$pub_oos)
  oos_dm <- ifelse(den_dm > 0, crossprod(w, Rdm0)[1, ] / den_dm, d$dm_oos)
  stats_fun(d$pub_is, d$pub_adj, d$pub_adj_dm, oos_pub,
            d$dm_is_raw, d$dm_adj_raw, oos_dm)
})
seB <- apply(bootB, 1, sd)

out <- data.frame(
  stat = names(point),
  point = as.numeric(point),
  se_predictor = as.numeric(seA),
  se_month = as.numeric(seB)
)
print(out, digits = 3)

write.csv(out, "../Results/EB_Bootstrap_SE.csv", row.names = FALSE)
cat("\nSaved ../Results/EB_Bootstrap_SE.csv  (B =", B, ", N =", N,
    ", months =", Tm, ")\n")
