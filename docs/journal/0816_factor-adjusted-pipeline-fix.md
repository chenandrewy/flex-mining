# Factor-adjusted pipeline fix checkpoint

2026-08-16 00:16 EDT

Implemented the fix identified in `FOUND_BUGS_in_risk_adjusted_code.md`.
The raw and factor-adjusted accounting benchmarks now start from one canonical
selection: `abs(tstat) > 2`, at least 10 stocks in each leg, at least 60 return
months, and a 12-month final formation period. The selection has 1,068,052
pairs, 173 mined predictors, 127 distinct publication sample windows, and SHA-256
fingerprint
`16ef030e152648c584fd02f32af656095f6f5719d6253bda1394d8e7b6fa216a`.

The old risk-adjusted code silently started from the much narrower matching
universe (roughly 68 thousand pairs), so the raw and adjusted plots did not have
the same DM benchmark population. `3a_PrepDMBenchmarks.R` now records the
canonical universe and fingerprint. The new `3c_FactorAdjustedDMPrep.R` checks
that contract, estimates sample-specific CAPM and FF4 models in 127 window
batches, and streams plot/table aggregates rather than saving pair-month data.
Its production run took 2m10s and wrote a 3.8 MB contract. Model-complete counts
were 1,016,398 CAPM pairs and 861,810 FF4 pairs.

Full-sample CAPM/FF3 specifications moved to Appendix SA07. Its preparation run
took 1m52s and wrote a 3.9 MB contract. Correlation/PCA and PCA-span preparation
moved from Chapter 3 to Appendix SA11. The old `2d` and `3e` producers are
retired; downstream figure and table scripts read only the focused contracts.

The corrected production outputs were rendered for Figure 2, Section 4 tables,
SA07, and SA09. Regression fixtures were intentionally refreshed to the fixed
values, while the old phase-one fixture remains only as a known-bug guard.
