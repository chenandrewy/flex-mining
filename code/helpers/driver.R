# Driver helpers: run child scripts and check required inputs.
#
# How to run: source from a driver (MAIN.R, 2_DataMining.R, 3_Precompute.R,
#   S2-S4, SA_Appendices.R) with the working directory set to code/. Also
#   sourced by 0_Environment.R so child scripts can call require_files().
# Inputs:  none.
# Outputs: defines run_script(), require_files(), exit_if_preflight_only(),
#   and longshort_path().
#
# Keep this file free of library() calls: drivers source it without loading the
# analysis packages.

# Run one script in its own Rscript process, so memory is returned to the
# operating system before the next script, and stop on a nonzero exit.
run_script <- function(path, stage = "Pipeline") {
  message("\n--- ", stage, ": ", path, " ---")
  status <- system2(file.path(R.home("bin"), "Rscript"), path)
  if (!identical(status, 0L)) {
    stop(stage, " script failed (exit ", status, "): ", path, call. = FALSE)
  }
  invisible(status)
}

# Stop with a readable message when any required input file is missing.
require_files <- function(files, what,
                          hint = "Run the required upstream chapter first.") {
  missing_files <- files[!file.exists(files)]
  if (length(missing_files) > 0L) {
    stop(
      "Missing ", what, " input(s): ", paste(missing_files, collapse = ", "),
      ". ", hint,
      call. = FALSE
    )
  }
  invisible(files)
}

# `Rscript <driver>.R --preflight-only` checks inputs and exits without running.
exit_if_preflight_only <- function(what) {
  if ("--preflight-only" %in% commandArgs(trailingOnly = TRUE)) {
    message(what, " preflight passed.")
    quit(save = "no", status = 0)
  }
}

# Path of the versioned mined-strategy file. Reads config.R in a private
# environment so drivers get the path without loading the analysis packages.
longshort_path <- function() {
  settings_env <- new.env(parent = globalenv())
  sys.source("config.R", envir = settings_env)
  paste0(
    file.path(
      "../Data/Processed", settings_env$globalSettings$universe$dataVersion
    ),
    " LongShort.RData"
  )
}
