# Validate the corrected phase-two Tables 6, 7, and IA.10 outputs.
#
# How to run: set the working directory to flex-mining/, run
#   Rscript S4b_RVsDM_ByGroup.R
#   Rscript tests/test_s4b_phase2_expected.R
# Inputs:  the six live files under ../Results/RiskAdjusted/TstatFilter
# Outputs: no files; exits nonzero on a displayed-value or output-contract change

live_dir <- "../Results/RiskAdjusted/TstatFilter"
expected_csv <- list(
  Table_RiskAdjusted_TimeVarying_ff4_t2 = c(
    '"Category","Group","Raw_Return","Raw_Outperformance","CAPM_Return","CAPM_Outperformance","FF4_Return","FF4_Outperformance"',
    '"Theoretical Foundation","Risk","43 (11)","5 (11)","38 (14)","-4 (14)","44 (14)","-6 (12)"',
    '"Theoretical Foundation","Mispricing","55 (8)","4 (7)","60 (8)","6 (7)","61 (8)","-3 (9)"',
    '"Theoretical Foundation","Agnostic","65 (12)","9 (14)","79 (14)","23 (15)","105 (22)","36 (24)"',
    '"Modeling Formalism","No Model","56 (8)","5 (7)","62 (7)","9 (7)","69 (8)","6 (9)"',
    '"Modeling Formalism","Stylized","63 (16)","15 (14)","49 (19)","-5 (17)","50 (22)","-13 (16)"',
    '"Modeling Formalism","Dynamic or Quantitative","34 (9)","-2 (9)","50 (17)","4 (15)","32 (29)","-13 (25)"',
    '"Overall","All","56 (7)","5 (7)","60 (7)","8 (7)","67 (7)","4 (8)"'
  ),
  Table_RiskAdjusted_TimeVarying_DisciplineJournal_ff4_t2 = c(
    '"Category","Group","Raw_Return","Raw_Outperformance","CAPM_Return","CAPM_Outperformance","FF4_Return","FF4_Outperformance"',
    '"Discipline","Finance","59 (8)","8 (7)","66 (7)","13 (8)","75 (9)","12 (10)"',
    '"Discipline","Accounting","43 (9)","-5 (10)","48 (9)","-6 (10)","47 (9)","-15 (10)"',
    '"Journal Rank","JF, JFE, RFS","60 (8)","8 (8)","68 (8)","16 (9)","78 (10)","15 (12)"',
    '"Journal Rank","AR, JAR, JAE","43 (9)","-6 (10)","45 (9)","-8 (9)","44 (8)","-17 (10)"',
    '"Journal Rank","Other","53 (9)","8 (9)","55 (9)","2 (10)","61 (12)","-4 (13)"'
  ),
  Table_RiskAdjusted_TimeVarying_AnyModelVsNoModel_ff4_t2 = c(
    '"Category","Group","Raw_Return","Raw_Outperformance","CAPM_Return","CAPM_Outperformance","FF4_Return","FF4_Outperformance"',
    '"","No Model","56 (8)","5 (7)","62 (7)","9 (7)","69 (8)","6 (9)"',
    '"","Any Model","54 (12)","10 (11)","49 (15)","-3 (13)","45 (18)","-13 (13)"'
  )
)

for (base in names(expected_csv)) {
  csv_file <- file.path(live_dir, paste0(base, ".csv"))
  tex_file <- file.path(live_dir, paste0(base, ".tex"))
  if (!identical(readLines(csv_file, warn = FALSE), expected_csv[[base]])) {
    stop("Phase-two displayed CSV differs: ", base)
  }
  tex <- readLines(tex_file, warn = FALSE)
  if (length(grep("^\\\\begin\\{tabular\\}", tex)) != 1L ||
      length(grep("^\\\\end\\{tabular\\}", tex)) != 1L) {
    stop("TeX is not one complete tabular fragment: ", base)
  }
}

expected_files <- sort(c(
  paste0(names(expected_csv), ".csv"),
  paste0(names(expected_csv), ".tex")
))
actual_files <- sort(list.files(live_dir, all.files = FALSE, no.. = TRUE))
if (!identical(actual_files, expected_files)) {
  stop(
    "Risk-adjusted output contract differs. Expected: ",
    paste(expected_files, collapse = ", "), "; found: ",
    paste(actual_files, collapse = ", ")
  )
}

message("All six retained outputs match the corrected phase-two expectations.")
