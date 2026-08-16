# Plan: one matched-and-excluding-correlated specification for Figure 2(c) and Tables 3–4

Timestamp: 2026-08-14 12:30 EDT

## Decision

The paper should show one version of Figure 2 and one version each of Tables 3
and 4. There should be no in-paper draft comparisons.

The common benchmark specification should be the one currently used by the
main Figure 2(c) line labeled “Matched and excluding correlated”:

- matched strategy t-stat within 10% of the published predictor;
- matched strategy mean return within 30% of the published predictor (Tol30);
- at least 60 in-sample months for each matched pair;
- pairwise signed-return correlation no greater than 10%;
- normalize each matched strategy by its own in-sample mean before averaging.

Table 3 should estimate the no-time-FE regressions on this benchmark. Table 4
should use the same data and regressions with time fixed effects.

## Current state

The official Tables 3–4 are not aligned with Figure 2(c). S3a currently starts
from `plotdat0$comp_matched`, the broad t-stat-greater-than-two match, then
reconstructs a correlation-filtered benchmark. Its relative matching
tolerances are effectively disabled and it does not apply Figure 2(c)'s
pair-level history floor.

The draft Tol30 Tables 3b/4b *are* aligned with the main Figure 2(c)
specification. Historical `4c22_MPStyleDecayTablesTolMatch.R` used the same
`MatchPub.RData` candidate returns, Tol30 pair set, 60-month floor, correlation
screen, and per-pair normalization as Figure 2(c). On the current data:

- main Figure 2(c) retains 152 published predictors;
- the Tol30 table regressions use 91,048 signal-month observations in every
  column;
- the checked-in Tol30 and Tol30Floor table files are byte-identical, showing
  that Tol30 was refreshed after the floor became the standing specification;
- the old Results and current Data/Processed pairwise-correlation caches are
  byte-identical.

The Figure and table code nevertheless reconstruct this benchmark separately.
They agree now, but the duplicated construction is what permits future drift.
The figure and regressions appropriately use different time windows for their
different purposes; alignment means the same matched pairs, filtering,
normalization, and surviving predictor universe, not identical plotted and
regression rows.

Main Figure 2(c) is Tol30. Its draft comparison is Tol10. The draft Table 3b
and 4b files are Tol30. Thus the desired change is to keep the main Figure
2(c), remove the Tol10 figure comparison, and promote the Tol30 table
specification into the official Tables 3–4.

## Pipeline plan

1. Build the matched-and-excluding-correlated benchmark once during Chapter 3.
   The reusable cache should contain enough information for both consumers:
   matched pair identifiers and diagnostics, `pubname`, event and calendar
   dates, published scaled/unscaled returns, matched scaled/unscaled returns,
   sample/publication dates, and the common surviving-predictor universe.

2. Make Figure 2(c) consume that canonical cache rather than reconstructing the
   pair set inside `3d_Fig2Data.R`. It should continue to render the published,
   matched, and matched-excluding-correlated lines, but only for Tol30.

3. Make `S3a_MPStyleDecayModels.R` consume the same cache. Remove its separate
   `plotdat0$comp_matched` correlation-filtered reconstruction. Estimate the
   six scaled and six unscaled specifications from the canonical panel, with
   published-return columns restricted to the same surviving predictors and
   observations as the benchmark/difference columns.

4. Keep `S3b_MPStyleDecayTables.R` as a render-only stage. It should write only
   the official presentation tables:

   - `Table_MPStyleRegsNoTimeFE.tex`;
   - `Table_MPStyleRegsTimeFE.tex`.

   Diagnostic component grids and the individual-DM appendix table may remain
   if still useful, but no Tol10/Tol30/Floor presentation variants should be
   emitted. R must continue to write under `../Results`, never directly into
   the writing repository.

5. Remove draft output generation from the Figure 2 pipeline: the `c10` panel,
   `Fig2c_MatchedExclCorr_Tol10.pdf`, and related draft-only comments/output
   declarations.

## Manuscript and exhibit cleanup

1. Remove the unnumbered draft Figure 2 comparison block from
   `sections/02_pubvs.tex`.

2. Replace the official Table 3 and Table 4 contents with newly generated
   results from the canonical Tol30 cache. Remove the unnumbered Table 3b and
   Table 4b blocks from `sections/03_learn.tex`.

3. Remove obsolete suffixed presentation files from the writing repository:
   Tol10, Tol30, Tol10Floor, and Tol30Floor. Retain only the official
   `Table_MPStyleRegsNoTimeFE.tex` and `Table_MPStyleRegsTimeFE.tex` files.

4. Update captions and surrounding prose so the official tables describe the
   Tol30 matching rule, history floor, correlation restriction, and common
   sample. Remove `DRAFT`, `tbc`, “Table 3b/4b,” and spec-decision language.

5. Update `docs/exhibit_map.md` to show one Figure 2 and two official MP-style
   tables, all in the MAIN.R chain. Recalculate the writing-repository exhibit
   counts after removing the obsolete files.

## Verification

- Add a focused invariant that Figure 2(c) and S3a read the same canonical
  cache rather than implementing parallel matching logic.
- Record pair count, predictor count, and a stable pair-set fingerprint in the
  cache metadata; assert that downstream consumers preserve them.
- Check that all six columns within each regression table use the same
  observation count and predictor universe.
- Generate tables and figures into temporary paths and compare the promoted
  tables with the current checked-in Tol30 versions before changing the
  writing repository.
- Compile the manuscript into a temporary output directory and confirm there
  are no references to Tol10/Tol30/Floor or draft exhibit files.
- Do not maintain older journal entries to reflect the new design; this entry
  is the current planning snapshot.
