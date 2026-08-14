# Render the Section 2 data-mining summary tables from chapter-3 caches.
#
# How to run: normally run through S2_ResearchVsDataMining.R with the working directory set to
#   flex-mining/.
# Inputs:  ../Data/Processed/sumsignal_oos_30y_{ew,vw}_unit_level.csv
#          ../Data/Processed/sumsignal_oos_30y_post_2003_{ew,vw}_unit_level.csv
# Outputs: ../Results/dm-sortsFull.tex
#          ../Results/dm-sortsPost2003.tex

source("0_Environment.R")

required_files <- c(
  "../Data/Processed/sumsignal_oos_30y_ew_unit_level.csv",
  "../Data/Processed/sumsignal_oos_30y_vw_unit_level.csv",
  "../Data/Processed/sumsignal_oos_30y_post_2003_ew_unit_level.csv",
  "../Data/Processed/sumsignal_oos_30y_post_2003_vw_unit_level.csv"
)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing chapter-3 data-mining summary cache(s): ",
    paste(missing_files, collapse = ", "),
    ". Run 3_Precompute.R first."
  )
}

format_sort_data <- function(ew_path, vw_path) {
  fs_ew <- readr::read_csv(ew_path, show_col_types = FALSE) %>%
    transmute(
      bin = as.integer(bin),
      empty1 = NA_character_,
      rbar_is = round(100 * rbar_is, 1),
      avg_tstat_is = round(avg_tstat_is, 2),
      empty2 = NA_character_,
      rbar_oos = round(100 * rbar_oos, 1),
      Decay = ifelse(bin != 4, round(100 * (1 - rbar_oos / rbar_is), 1), NA_real_),
      empty3 = NA_character_
    )

  fs_vw <- readr::read_csv(vw_path, show_col_types = FALSE) %>%
    transmute(
      rbar_isvw = round(100 * rbar_is, 1),
      avg_tstat_isvw = round(avg_tstat_is, 2),
      empty1vw = NA_character_,
      rbar_oosvw = round(100 * rbar_oos, 1),
      Decayvw = ifelse(bin != 4, round(100 * (1 - rbar_oos / rbar_is), 1), NA_real_)
    )

  bind_cols(fs_ew, fs_vw)
}

write_sort_table <- function(tab, output_path) {
  tab %>%
    xtable::xtable(digits = c(0, 0, 0, 1, 2, 0, 1, 1, 0, 1, 2, 0, 1, 1)) %>%
    print(
      include.rownames = FALSE,
      include.colnames = FALSE,
      hline.after = NULL,
      only.contents = TRUE,
      file = output_path
    )
}

write_sort_table(
  format_sort_data(required_files[1], required_files[2]),
  "../Results/dm-sortsFull.tex"
)
write_sort_table(
  format_sort_data(required_files[3], required_files[4]),
  "../Results/dm-sortsPost2003.tex"
)
