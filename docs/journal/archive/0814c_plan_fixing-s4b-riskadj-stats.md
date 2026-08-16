# Fixing the statistics in `S4b_RVsDM_ByGroup.R`

Snapshot written 2026-08-14 11:51 EDT. This is the handoff plan for the agent
doing the phase-two statistical work. Do not treat it as an implementation log.

## Starting point

Phase one is complete in commits `55d8946` and `06a92e1`. The old `4c4`
monolith is gone, `S4b_RVsDM_ByGroup.R` owns Tables 6, 7, and IA.10, and one
run produces exactly three TeX tabulars and three matching audit CSVs under
`../Results/RiskAdjusted/TstatFilter/`.

The phase-one test is `tests/test_4c4_phase1_equivalence.R`. Its fixtures under
`tests/fixtures/4c4_phase1_baseline/` reproduce the old reported values
exactly. Preserve those fixtures as the historical frozen-logic baseline. They
are evidence about what phase one reproduced, not the target values for phase
two.

The full calculation remains deliberately mechanical and verbose inside
`S4b_RVsDM_ByGroup.R`. Do not start by cleaning it up. First identify each
statistical correction and the cells it should change.

## First deliverable: an audit, not code

Before editing, inspect the paper specification, `S4b_RVsDM_ByGroup.R`,
`helpers/risk_adjusted_helpers_tv.R`, and every caller of the helper functions
that would be changed. Write a short audit that answers these questions:

1. What is the observation used to estimate each reported mean: a
   signal-month, an event-time observation, or something else after joins and
   aggregation?
2. Are `(pubname, date)` and the corresponding data-mined observations unique
   at the point where means and standard errors are estimated? If not, explain
   the weighting induced by duplicates.
3. What exact clustering does the paper promise for published returns and for
   published-minus-data-mined outperformance?
4. Should outperformance be estimated from a paired difference on the common
   sample? State how missing values are handled.
5. Do raw, CAPM, and FF4 columns intentionally use different screened signal
   sets? Document the filters and sample counts for each column.
6. Which signal classifications differ when discipline uses the complete
   accounting-journal list rather than only the top three?
7. Which existing pooled Any Model result is the correct input for IA.10, and
   why is it preferable to averaging the two displayed model subgroups?

Do not infer the estimand solely from current code comments. Reconcile the
paper text, table notes, data shape, and calculation. Stop for a decision if
they conflict materially.

## Corrections to evaluate

### 1. Clustered standard errors

`compute_outperformance()` currently calculates `sd(x) / sqrt(n)`, although
the table descriptions say standard errors are clustered by calendar month and
predictor. Replace this only after the audit fixes the estimation unit and
cluster identifiers precisely.

The implementation should estimate a mean as an intercept and obtain the
stated multiway clustered variance using an established installed R package.
Do not hand-roll a cluster covariance estimator. Record the finite-sample
correction and behavior when a group has too few clusters.

Apply the same definition consistently to the raw, CAPM, and FF4 published
returns. Check `compute_overall_summary()` and the duplicated discipline and
journal blocks as well as `compute_outperformance()`; changing only one path
would leave internally inconsistent rows.

### 2. Paired outperformance

The current code reports

`pub_oos - dm_oos`

and approximates its standard error as

`sqrt(pub_oos_se^2 + dm_oos_se^2)`.

Audit and, if consistent with the paper, replace this with the mean of the
row-level published-minus-data-mined return on observations where both sides
are available. Estimate its clustered standard error directly. Assert that the
reported point estimate agrees with the difference of means when the samples
are identical; if it does not, report why.

### 3. Discipline classification

`load_signal_mappings()` currently labels only `globalSettings$top3Accounting`
as Accounting. The intended discipline mapping appears to require
`globalSettings$acctlistAll`. Quantify the affected signals in every included
sample before changing the mapping, retain the separate top-three journal-rank
classification, and make the discipline and journal-rank rules visibly
distinct.

### 4. Pooled Any Model calculation

The final IA.10 export constructs Any Model by averaging the Stylized and
Dynamic/Quantitative subgroup point estimates and combining their standard
errors approximately. Earlier code already computes pooled Any Model summaries
using `model_binary`.

Use the pooled calculation for the final raw, CAPM, and FF4 Any Model row. Do
not average displayed subgroup estimates. Delete the approximate combination
only after an output diff proves which IA.10 cells change.

### 5. Samples and filters

Add assertions for the intended raw, CAPM, and FF4 samples rather than relying
on incidental joins. At minimum, record and test:

- published signals passing the raw `t > 2` screen;
- published signals passing each alpha screen and the raw intersection;
- data-mined signals passing the corresponding screen;
- signals and post-sample observations entering each reported group;
- uniqueness of the estimation rows and nonempty calendar-month and predictor
  clusters; and
- consistent pairing for the outperformance columns.

Assertions should fail with useful counts and identifiers, not merely say that
two objects differ.

## Implementation order

Make the corrections in separate, reviewable steps:

1. Add non-mutating sample and uniqueness diagnostics and capture the current
   counts.
2. Implement the common mean/clustered-SE estimator and use it for one table
   path. Compare before extending it to every row.
3. Switch outperformance to the paired estimator.
4. Correct discipline mapping.
5. Switch IA.10 to pooled Any Model estimates.
6. Only after the numerical changes are understood, consolidate duplicated
   summary blocks and remove dead phase-one scaffolding.

Prefer one commit per conceptual correction. Do not combine statistical
changes with broad renaming, formatting, or unrelated appendix cleanup.

## Validation and reporting

For every correction, produce a machine-readable comparison containing the
old value, new value, difference, old displayed value, and new displayed value
for every estimate and standard error in all three tables. Explain every
changed displayed cell.

The final validation must:

- preserve row order, headings, and the six-file output contract;
- confirm all TeX files remain complete `tabular` fragments;
- parse all R files and pass the Section 4 and appendix preflights;
- confirm `S4b_RVsDM_ByGroup.R` creates no plots, diagnostic exports, or
  temporary PDFs;
- run `S4b` from the same cache vintage used by the phase-one fixtures;
- retain the phase-one fixtures unchanged and add a clearly named phase-two
  expected-output fixture or test rather than rewriting history; and
- record runtime and the exact package/function used for clustered inference.

The audit CSV contract should be reconsidered during this work. The phase-one
CSVs contain formatted, rounded cells because they reproduce the legacy
outputs. If phase two adds unrounded numerical audit files, keep the paper
tabulars and human-review CSVs unambiguous and document which is authoritative.

## Scope boundaries

Do not edit the separate writing repository. It still receives these source
numbers by hand.

`Appendices/SA07_FullSampleRiskAdjustedResearchVsDMPlots.R` is a separate
full-sample workflow. Audit it for the same statistical issues, but do not
silently change Tables IA.6, IA.7, IA.11, or Figure IA.1 while fixing `S4b`.
The time-varying and full-sample helper files currently provide a useful scope
boundary; verify all callers before changing a shared helper.

Do not change normalization, factor adjustments, return units, screening
thresholds, or sample definitions unless the audit shows that the paper
specification requires it and the user approves the change explicitly.

## Completion condition

Phase two is complete when the intended estimands and clustering are stated in
code and documentation; Tables 6, 7, and IA.10 use those estimands consistently;
every numerical change from the preserved phase-one baseline is explained;
sample membership and pairing are asserted; the six-file output contract and
drivers still work; and the full-sample appendix workflow has not changed
without its own explicit audit.
