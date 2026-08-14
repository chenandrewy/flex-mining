# Archived text-analysis workflow

Archived 2026-08-13 for possible later deletion. These scripts are not part of
the active `MAIN.R` workflow.

`6a_TextCleaning.R` is the old regeneration path for the tracked
`DataIntermediate/TextClassification.csv`. The active
`Appendices/SA04_DecayVsWordcountPlot.R` reads that checked-in CSV directly. The regeneration
path requires R's `spacyr` package plus Python spaCy and the
`en_core_web_sm` model, which are not part of the active project environment.
`Papers_to_df.ipynb` is the preceding Python/OCR step that generated
`DataIntermediate/text_with_pdf_name.csv` from the original-paper PDFs.

`6b_TextTables.R` writes text-analysis tables and CSVs that are not included in
the paper's exhibit set.
