# Verify the compact pair catalog and on-demand materializer that replace
# MatchPub.RData.
#
# How to run: from flex-mining/, run
#   Rscript tests/test_matchpub_replacement.R
# Inputs:  dmcomp_sumstats.RDS, the versioned LongShort.RData, and source files
# Outputs: none; exits nonzero on failure.

source("0_Environment.R")

dm_sumstats_path <- "../Data/Processed/dmcomp_sumstats.RDS"
if (file.exists(dm_sumstats_path)) {
  keep <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
    filter(Keep) %>%
    pull(signalname)
  pairs <- select_matched_dm_pairs(
    readRDS(dm_sumstats_path)$insampsum,
    pubnames = keep
  )
  pair_keys <- paste(pairs$pubname, pairs$dmname, sep = "\t")
  stopifnot(
    nrow(pairs) == 22157L,
    digest::digest(
      paste(pair_keys, collapse = "\n"),
      algo = "sha256", serialize = FALSE
    ) == "54dcd5242f510906c87ae763a09e3f48d226bff08c548c36a4528fb9c02eec5d"
  )

  # A small materialization checks schema and event-time/sign semantics without
  # constructing the full 16-million-row panel.
  sample_pairs <- pairs[seq_len(min(3L, nrow(pairs)))]
  returns <- materialize_matched_dm_returns(
    sample_pairs,
    paste0(
      "../Data/Processed/", globalSettings$dataVersion, " LongShort.RData"
    )
  )
  stopifnot(
    identical(
      names(returns),
      c("candSignalname", "eventDate", "sign", "ret", "samptype",
        "actSignal", "sweight")
    ),
    all(unique(returns$actSignal) %in% sample_pairs$pubname),
    all(unique(returns$candSignalname) %in% sample_pairs$dmname),
    all(returns$sign %in% c(-1, 1))
  )
}

live_files <- c(
  "2_DataMining.R", "3_Precompute.R", "3a_PrepDMBenchmarks.R",
  "3c_FactorAdjustedDMPrep.R", "S5_BestPredictors.R",
  "S5a_InspectTables.R",
  "Appendices/SA10_ResearchVsDMRobustnessCorrelationsEtc.R"
)
live_source <- unlist(lapply(live_files, readLines), use.names = FALSE)
stopifnot(
  !file.exists("2b_MatchDataMinedToPub.R"),
  !file.exists("2d_RiskAdjustDataMinedSignals.R"),
  !any(grepl("MatchPub.RData", live_source, fixed = TRUE)),
  !any(grepl("MatchPubRiskAdjusted.RData", live_source, fixed = TRUE))
)
