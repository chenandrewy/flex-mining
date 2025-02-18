# Main script to run all numbered R scripts (excluding letter-indexed scripts)


# Set up logging ----------------------------------------------------------


timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
log_file <- paste0("MAIN_log_", timestamp, ".txt")

# Function to write to both console and log file
log_message <- function(msg) {
  cat(msg)
  cat(msg, file = log_file, append = TRUE)
}

log_message("Starting main analysis pipeline...\n")


# Find Scripts ------------------------------------------------------------

# Get all R scripts in the current directory
scripts <- list.files(pattern = "^[0-9]+_.*\\.R$")

# Filter out scripts with letter indices (e.g., 2a_, 2b_)
main_scripts <- scripts[!grepl("^[0-9]+[a-zA-Z]_", scripts)]

# Sort scripts numerically
main_scripts <- main_scripts[order(as.numeric(gsub("^([0-9]+)_.*$", "\\1", main_scripts)))]

# Print script names to console and log
log_message("Scripts to be executed:\n")
for (script in main_scripts) {
  log_message(sprintf("  %s\n", script))
}

# keyboard check
log_message("Press Enter to continue...")
readLines(n = 1)


# Run Scripts -------------------------------------------------------------

# Run each script in order
for (script in main_scripts) {
  log_message(sprintf("\n%s\n", paste(rep("=", 50), collapse="")))
  log_message(sprintf("Executing %s at %s...\n", script, format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
  
  tryCatch({
    # Capture all output from source() using sink()
    sink(log_file, append = TRUE, split = TRUE)
    source(script, echo = TRUE)
    sink()
    
    log_message(sprintf("Completed %s successfully\n", script))
  }, error = function(e) {
    if (sink.number() > 0) sink() # Close sink if error occurs
    log_message(sprintf("Error in %s: %s\n", script, e$message))
    stop(sprintf("Pipeline stopped due to error in %s", script))
  })
}

log_message("\nMain analysis pipeline completed.\n") 