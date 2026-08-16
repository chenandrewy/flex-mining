# Validate the corrected phase-two full-sample Tables IA.6, IA.7, and IA.11.
#
# How to run: set the working directory to flex-mining/, run
#   Rscript Appendices/SA07_FullSampleRiskAdjustedResearchVsDMPlots.R
#   Rscript tests/test_s4b_fullsample_phase2_expected.R
# Inputs:  three full-sample audit CSV/TeX pairs under
#          ../Results/RiskAdjusted/FullSampleTstatFilter
# Outputs: no files; exits nonzero on a displayed-value or fragment change

live_dir <- "../Results/RiskAdjusted/FullSampleTstatFilter"
expected_csv <- list(
  Table_RiskAdjusted_FullSample_t2 = c(
    '"Category","Group","Raw_Return","Raw_Outperformance","CAPM_Return","CAPM_Outperformance","FF3_Return","FF3_Outperformance"',
    '"Theoretical Explanation","Risk","43 (11)","5 (11)","43 (14)","1 (14)","53 (13)","7 (11)"',
    '"Theoretical Explanation","Mispricing","55 (8)","4 (7)","59 (6)","5 (6)","59 (6)","2 (7)"',
    '"Theoretical Explanation","Agnostic","65 (12)","9 (14)","79 (12)","25 (13)","85 (14)","25 (15)"',
    '"Modeling Formalism","No Model","56 (8)","5 (7)","61 (6)","9 (6)","64 (6)","7 (7)"',
    '"Modeling Formalism","Stylized","63 (16)","15 (14)","57 (22)","5 (19)","68 (20)","10 (17)"',
    '"Modeling Formalism","Dynamic or Quantitative","34 (9)","-2 (9)","49 (12)","4 (12)","42 (17)","-8 (16)"',
    '"Overall","All","56 (7)","5 (7)","61 (6)","8 (6)","63 (6)","7 (6)"'
  ),
  Table_RiskAdjusted_FullSample_DisciplineJournal_t2 = c(
    '"Category","Group","Raw_Return","Raw_Outperformance","CAPM_Return","CAPM_Outperformance","FF3_Return","FF3_Outperformance"',
    '"Discipline","Finance","59 (8)","8 (7)","66 (6)","13 (7)","69 (7)","12 (7)"',
    '"Discipline","Accounting","43 (9)","-5 (10)","47 (8)","-5 (9)","45 (7)","-10 (9)"',
    '"Journal Rank","JF, JFE, RFS","60 (8)","8 (8)","67 (7)","13 (8)","70 (8)","12 (8)"',
    '"Journal Rank","AR, JAR, JAE","43 (9)","-6 (10)","46 (8)","-6 (9)","45 (8)","-11 (9)"',
    '"Journal Rank","Other","53 (9)","8 (9)","57 (8)","9 (8)","62 (9)","8 (9)"'
  ),
  Table_RiskAdjusted_FullSample_AnyModelVsNoModel_t2 = c(
    '"Category","Group","Raw_Return","Raw_Outperformance","CAPM_Return","CAPM_Outperformance","FF3_Return","FF3_Outperformance"',
    '"","No Model","56 (8)","5 (7)","61 (6)","9 (6)","64 (6)","7 (7)"',
    '"","Any Model","54 (12)","10 (11)","55 (15)","5 (13)","59 (15)","4 (13)"'
  )
)

for (base in names(expected_csv)) {
  csv <- readLines(file.path(live_dir, paste0(base, ".csv")), warn = FALSE)
  if (!identical(csv, expected_csv[[base]])) {
    stop("Corrected full-sample CSV differs: ", base)
  }
  tex <- readLines(file.path(live_dir, paste0(base, ".tex")), warn = FALSE)
  if (sum(grepl("^\\\\begin\\{tabular\\}", tex)) != 1L ||
      sum(grepl("^\\\\end\\{tabular\\}", tex)) != 1L) {
    stop("Full-sample audit TeX is not one complete tabular: ", base)
  }
}

message("All corrected full-sample audit outputs match phase-two expectations.")
