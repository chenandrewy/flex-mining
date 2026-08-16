# Validate the corrected phase-two Tables 6, 7, and IA.10 outputs.
#
# How to run: set the working directory to flex-mining/, run
#   Rscript S4b_RVsDM_ByGroup.R
#   Rscript tests/test_s4b_phase2_expected.R
# Inputs:  the six live files under ../Results/FactorAdjusted/TstatFilter
# Outputs: no files; exits nonzero on a displayed-value or output-contract change

live_dir <- "../Results/FactorAdjusted/TstatFilter"
expected_csv <- list(
  Table_FactorAdjusted_TimeVarying_ff4_t2 = c(
    '"Category","Group","Raw_Return","Raw_Outperformance","CAPM_Return","CAPM_Outperformance","FF4_Return","FF4_Outperformance"',
    '"Theoretical Foundation","Risk","43 (11)","5 (11)","38 (13)","-3 (14)","45 (13)","-5 (12)"',
    '"Theoretical Foundation","Mispricing","55 (8)","4 (7)","63 (7)","8 (7)","64 (7)","2 (9)"',
    '"Theoretical Foundation","Agnostic","65 (12)","9 (14)","77 (13)","23 (14)","101 (20)","35 (22)"',
    '"Modeling Formalism","No Model","56 (8)","5 (7)","63 (7)","10 (7)","70 (8)","9 (9)"',
    '"Modeling Formalism","Stylized","63 (16)","15 (14)","49 (19)","-3 (18)","50 (22)","-10 (15)"',
    '"Modeling Formalism","Dynamic or Quantitative","34 (9)","-2 (9)","48 (12)","6 (10)","39 (18)","-10 (17)"',
    '"Overall","All","56 (7)","5 (7)","62 (6)","9 (7)","68 (7)","7 (8)"'
  ),
  Table_FactorAdjusted_TimeVarying_DisciplineJournal_ff4_t2 = c(
    '"Category","Group","Raw_Return","Raw_Outperformance","CAPM_Return","CAPM_Outperformance","FF4_Return","FF4_Outperformance"',
    '"Discipline","Finance","59 (8)","8 (7)","67 (7)","14 (7)","77 (9)","16 (9)"',
    '"Discipline","Accounting","43 (9)","-5 (10)","48 (9)","-4 (10)","47 (9)","-16 (10)"',
    '"Journal Rank","JF, JFE, RFS","60 (8)","8 (8)","70 (8)","16 (8)","80 (9)","17 (10)"',
    '"Journal Rank","AR, JAR, JAE","43 (9)","-6 (10)","45 (9)","-7 (10)","44 (8)","-19 (10)"',
    '"Journal Rank","Other","53 (9)","8 (9)","55 (9)","6 (9)","60 (12)","6 (13)"'
  ),
  Table_FactorAdjusted_TimeVarying_AnyModelVsNoModel_ff4_t2 = c(
    '"Category","Group","Raw_Return","Raw_Outperformance","CAPM_Return","CAPM_Outperformance","FF4_Return","FF4_Outperformance"',
    '"","No Model","56 (8)","5 (7)","63 (7)","10 (7)","70 (8)","9 (9)"',
    '"","Any Model","54 (12)","10 (11)","49 (14)","0 (13)","46 (16)","-10 (12)"'
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
    "Factor-adjusted output contract differs. Expected: ",
    paste(expected_files, collapse = ", "), "; found: ",
    paste(actual_files, collapse = ", ")
  )
}

message("All six retained outputs match the corrected phase-two expectations.")
