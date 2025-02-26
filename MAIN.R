# Main script to run all numbered R scripts (excluding letter-indexed scripts)

# Script List -------------------------------------------------------------

main_scripts <- c(
  "0_Environment.R",
  "1_Download_and_Clean.R",
  "2_DataMining.R",
  "3_RiskVsMispricing.R",
  "4_ResearchVsDataMining.R",
  "6_TextAnalysis.R",
  "7_Breaks.R",
  "8_DMThemes.R",
  "99_ExportDataToCsv.R"
)

# Run each script in order ------------------------------------------------

for (script in main_scripts) {
    source(script, echo = TRUE)
}