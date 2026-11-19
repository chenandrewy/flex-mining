# Download the Chen-Zimmermann signal-level predictor panel.
#
# How to run: from flex-mining/,
#   Rscript Appendices/TimeFERobustness/run.R download
# Inputs:  the pinned Open Source Asset Pricing release on Hugging Face
#            (OpenSourceAP/CrossSection: firm characteristics, signal
#            documentation, original portfolio returns), the pinned
#            meta-replication mapping, and CRSP via WRDS for the three
#            CRSP-derived signals the bulk file omits
#          WRDS credentials, resolved as in 1_Download_and_Clean.R: from a
#            .env file if present, then WRDS_USERNAME/WRDS_PASSWORD in the
#            environment, otherwise an interactive getPass prompt
# Outputs: ../Data/Raw/TimeFERobustness/opensourceap/ (pinned downloads)
#          ../Data/Processed/TimeFERobustness/opensourceap/
#            signed_predictors_all_wide-<release>.parquet/
#
# This stage is separate from the build because it downloads several gigabytes
# and can require a WRDS connection. The Parquet output is a directory-backed
# Arrow dataset: the bulk firm-characteristic file is about 2 GB as Parquet and
# far larger in memory, so it is streamed rather than read into an R
# data.frame.

source("Appendices/TimeFERobustness/setup.R")
suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(OpenSourceAP.DownloadR)
})

release <- timefeSettings$pins$release
raw_dir <- timefeSettings$paths$openap_raw
firm_char_parquet <- file.path(
  raw_dir, paste0("signed_predictors_dl_wide-", release, ".parquet")
)
doc_csv <- timefeSettings$files$signal_doc
op_csv <- timefeSettings$files$op_portfolios
meta_csv <- timefeSettings$files$meta_replications
panel_parquet <- timefeSettings$files$signal_panel

# Resolve WRDS credentials the same way as 1_Download_and_Clean.R: load a .env
# file if one is present, which populates WRDS_USERNAME/WRDS_PASSWORD. Anything
# still missing is prompted for downstream by OpenSourceAP.DownloadR (for the
# CRSP signals) via getPass, the same package the main pipeline prompts with.
if (file.exists(".env")) {
  library(dotenv)
  load_dot_env(".env")
}

# OpenSourceAP.DownloadR reads the shorter WRDS_USER/WRDS_PASS names, while the
# repository convention (and .env) uses WRDS_USERNAME/WRDS_PASSWORD. Bridge the
# two without printing the values.
if (!nzchar(Sys.getenv("WRDS_USER")) &&
    nzchar(Sys.getenv("WRDS_USERNAME"))) {
  Sys.setenv(WRDS_USER = Sys.getenv("WRDS_USERNAME"))
}
if (!nzchar(Sys.getenv("WRDS_PASS")) &&
    nzchar(Sys.getenv("WRDS_PASSWORD"))) {
  Sys.setenv(WRDS_PASS = Sys.getenv("WRDS_PASSWORD"))
}

# Versions before 0.2.0 read the retired Google Drive folders.
check(packageVersion("OpenSourceAP.DownloadR") >= "0.2.0",
      "OpenSourceAP.DownloadR >= 0.2.0 is required (Hugging Face downloads).")

# Carried over from the Google Drive downloads, which could fail under
# libcurl's HTTP/2 implementation in this container. In httr/libcurl,
# http_version = 2 requests HTTP/1.1.
httr::set_config(httr::config(http_version = 2))

# Download to a temporary name and rename on success, so an interrupted
# transfer is not mistaken for a finished file on the next run.
download_pinned <- function(url, path, timeout = 3600) {
  partial <- paste0(path, ".part")
  on.exit(unlink(partial), add = TRUE)
  status <- withr::with_options(
    new = list(timeout = timeout),
    code = download.file(url, partial, mode = "wb", quiet = FALSE)
  )
  check(status == 0L && file.exists(partial) && file.info(partial)$size > 0,
        "Could not download %s.", url)
  check(file.rename(partial, path), "Could not move the download to %s.", path)
}

message("Initializing Open Source Asset Pricing release ", release, " ...")
openap <- OpenAP$new(release_year = release)

if (!file.exists(doc_csv) || file.info(doc_csv)$size == 0) {
  message("Downloading signal documentation ...")
  # Fetch the release's SignalDoc.csv as is: dl_signal_doc() goes through
  # read.csv(), which rewrites headers such as `Predictability in OP`.
  download_pinned(openap$get_url("signal_doc"), doc_csv)
}

if (!file.exists(op_csv) || file.info(op_csv)$size == 0) {
  message("Downloading original CZ portfolio returns ...")
  fwrite(as.data.table(openap$dl_port("op")), op_csv)
}

if (!file.exists(meta_csv) || file.info(meta_csv)$size == 0) {
  mapping_url <- paste0(
    "https://raw.githubusercontent.com/OpenSourceAP/CrossSection/",
    timefeSettings$pins$cz_mapping_commit,
    "/Docs/Comparison_to_MetaReplications.csv"
  )
  message("Downloading pinned meta-replication mapping ...")
  download.file(mapping_url, meta_csv, mode = "wb", quiet = TRUE)
  check(file.exists(meta_csv) && file.info(meta_csv)$size > 0,
        "Could not download the pinned meta-replication mapping.")
}
mapping_sha256 <- digest::digest(meta_csv, algo = "sha256", file = TRUE)
check(
  identical(mapping_sha256, timefeSettings$pins$cz_mapping_sha256),
  "Meta-replication mapping hash mismatch: expected %s, found %s.",
  timefeSettings$pins$cz_mapping_sha256, mapping_sha256
)

# Use the package to resolve the selected release's official bulk-file URL,
# but avoid dl_all_signals(): that method reads the whole signed file into an
# R data.frame and exceeds the devcontainer's memory.
if (!file.exists(firm_char_parquet) ||
    file.info(firm_char_parquet)$size == 0) {
  message("Downloading the bulk CZ firm-characteristic file ...")
  download_pinned(openap$get_url("firm_char"), firm_char_parquet)
}

if (!dir.exists(panel_parquet)) {
  message("Downloading the three CRSP-derived signals from WRDS ...")
  # The bulk file is already signed. dl_signal_crsp3() likewise constructs
  # signed Price, Size, and STreversal, so no second sign transformation is
  # appropriate here.
  crsp_signals <- as.data.table(openap$dl_signal_crsp3(signed = TRUE))
  crsp_signals[, `:=`(
    permno = as.integer(permno),
    yyyymm = as.integer(yyyymm)
  )]

  message("Streaming the merged CZ signal panel to Parquet ...")
  openap_dataset <- open_dataset(firm_char_parquet, format = "parquet")
  bulk_columns <- openap_dataset$schema$names
  check(all(c("permno", "yyyymm") %in% bulk_columns),
        "The bulk firm-characteristic file has no permno/yyyymm keys.")
  check(!any(names(crsp_signals) %in% setdiff(bulk_columns,
                                              c("permno", "yyyymm"))),
        "The bulk file already contains a CRSP-derived signal column.")
  # Fix the column types the downstream stages rely on, whatever the
  # release's Parquet file stores: integer keys and double characteristics.
  merged_dataset <- openap_dataset |>
    mutate(across(all_of(c("permno", "yyyymm")), ~ cast(.x, int32()))) |>
    mutate(across(-all_of(c("permno", "yyyymm")), ~ cast(.x, float64()))) |>
    left_join(arrow_table(crsp_signals), by = c("permno", "yyyymm"))
  write_dataset(
    merged_dataset,
    path = panel_parquet,
    format = "parquet",
    existing_data_behavior = "error"
  )
}

panel_dataset <- open_dataset(panel_parquet, format = "parquet")
panel_rows <- panel_dataset |>
  summarise(rows = n()) |>
  collect() |>
  pull(rows)
signal_columns <- setdiff(
  panel_dataset$schema$names, c("permno", "yyyymm")
)
message(
  "Saved ", format(panel_rows, big.mark = ","), " firm-month rows and ",
  length(signal_columns), " characteristics to ", panel_parquet
)
message("Saved signal documentation to ", doc_csv)
message("Saved original portfolio returns to ", op_csv)
message("Saved pinned meta-replication mapping to ", meta_csv)
