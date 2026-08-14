# Validate the corrected phase-two full-sample Tables IA.6, IA.7, and IA.11.
#
# How to run: set the working directory to flex-mining/, run
#   Rscript S4b_RVsDM_ByGroup.R
#   Rscript tests/test_s4b_fullsample_phase2_expected.R
# Inputs:  three full-sample audit CSV/TeX pairs under
#          ../Results/RiskAdjusted/FullSampleTstatFilter
# Outputs: no files; exits nonzero on a displayed-value or fragment change

live_dir <- "../Results/RiskAdjusted/FullSampleTstatFilter"
expected_csv <- list(
  Table_RiskAdjusted_FullSample_t2 = c(
    '"Category","Group","Raw_Return","Raw_Outperformance","CAPM_Return","CAPM_Outperformance","FF3_Return","FF3_Outperformance"',
    '"Theoretical Explanation","Risk","43 (11)","5 (11)","42 (14)","0 (14)","53 (13)","7 (11)"',
    '"Theoretical Explanation","Mispricing","55 (8)","4 (7)","57 (6)","4 (6)","57 (7)","0 (7)"',
    '"Theoretical Explanation","Agnostic","65 (12)","9 (14)","82 (13)","25 (15)","88 (15)","26 (16)"',
    '"Modeling Formalism","No Model","56 (8)","5 (7)","60 (6)","8 (6)","63 (7)","6 (7)"',
    '"Modeling Formalism","Stylized","63 (16)","15 (14)","57 (22)","1 (19)","68 (20)","6 (18)"',
    '"Modeling Formalism","Dynamic or Quantitative","34 (9)","-2 (9)","54 (15)","17 (14)","43 (23)","2 (21)"',
    '"Overall","All","56 (7)","5 (7)","60 (6)","8 (6)","63 (6)","6 (6)"'
  ),
  Table_RiskAdjusted_FullSample_DisciplineJournal_t2 = c(
    '"Category","Group","Raw_Return","Raw_Outperformance","CAPM_Return","CAPM_Outperformance","FF3_Return","FF3_Outperformance"',
    '"Discipline","Finance","59 (8)","8 (7)","65 (7)","13 (7)","69 (7)","12 (8)"',
    '"Discipline","Accounting","43 (9)","-5 (10)","47 (8)","-6 (8)","45 (7)","-10 (8)"',
    '"Journal Rank","JF, JFE, RFS","60 (8)","8 (8)","66 (8)","14 (8)","69 (8)","13 (9)"',
    '"Journal Rank","AR, JAR, JAE","43 (9)","-6 (10)","46 (8)","-6 (8)","45 (8)","-9 (9)"',
    '"Journal Rank","Other","53 (9)","8 (9)","57 (8)","5 (9)","62 (9)","3 (9)"'
  ),
  Table_RiskAdjusted_FullSample_AnyModelVsNoModel_t2 = c(
    '"Category","Group","Raw_Return","Raw_Outperformance","CAPM_Return","CAPM_Outperformance","FF3_Return","FF3_Outperformance"',
    '"","No Model","56 (8)","5 (7)","60 (6)","8 (6)","63 (7)","6 (7)"',
    '"","Any Model","54 (12)","10 (11)","57 (16)","5 (14)","61 (17)","5 (14)"'
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
