# `4c4` phase-one baseline

These six files are the retained manuscript-source outputs from the final run
of `4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R` against the 2026-08-14 processed
data vintage. The baseline completed in 118.889 seconds (101.147 user and
20.401 system seconds). `/usr/bin/time` was unavailable, so peak memory was not
recorded.

The legacy run wrote 23 files. They were classified as follows:

- retained main-paper sources: the TimeVarying theory/model and
  discipline/journal CSV/TeX pairs (Tables 6 and 7);
- retained appendix source: the TimeVarying AnyModelVsNoModel CSV/TeX pair
  (Table IA.10);
- removed raw-only tables: the TheoryModel, DisciplineJournal, and
  AnyModelVsNoModel CSV/TeX pairs;
- removed superseded figures: raw, CAPM-alpha, and FF4-alpha PDFs;
- removed diagnostics: `tv_alpha_summary_t2.csv` and four `Raw_*.csv` files;
- removed accidental files: three `../Results/temp__*.pdf` files.

The writing repository references the live Figure 2 panels and the dedicated
accounting-only appendix panels, and contains no reference to the three removed
`Fig_RiskAdj_*` files.

The baseline TeX files contain complete table floats. Phase one deliberately
changes the live TeX contract to the enclosed `tabular` environment only; the
comparison test therefore checks that environment exactly.
