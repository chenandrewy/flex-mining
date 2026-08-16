# Wiring all six S4b group tables into the paper

Snapshot updated 2026-08-14 after discovering that the initial three-table
plan omitted the related full-sample appendix tables. This is the implementation
plan for one end-to-end owner of all theory, model, discipline, and journal
group comparisons.

## Current gap

The default analysis chain rebuilds the sample-specific source artifacts for
Tables 6, 7, and IA.10:

```text
MAIN.R
  -> S4_Heterogeneity.R
    -> S4b_RVsDM_ByGroup.R
      -> ../Results/RiskAdjusted/TstatFilter/*.{csv,tex}
```

The paper originally contained those three tables as inline LaTeX. It inputs
three additional, manually maintained complete-table files for the same group
comparisons under full-sample CAPM/FF3 risk adjustment:

- IA.6: `Table_RiskAdjusted_FullSample_Appendix.tex`;
- IA.7: `Table_RiskAdjusted_FullSample_DisciplineJournal_Appendix.tex`; and
- IA.11: `Table_RiskAdjusted_FullSample_AnyModelVsNoModel_Appendix.tex`.

Those filenames exist only in the writing repository's `exhibits/` snapshot.
SA07 writes differently named files under
`RiskAdjusted/FullSampleTstatFilter/`, so the live
`\exhibitspath=../../Results` build cannot find IA.6, IA.7, or IA.11. The
manually maintained copies also differ from current generated output.

## Ownership and intended chain

S4b should be the single calculation and rendering owner for all six grouped
tables: Tables 6, 7, IA.6, IA.7, IA.10, and IA.11.

```text
S4b sample-specific and full-sample summaries
  -> detailed audit CSV/TeX outputs
  -> six paper-specific tabular fragments at ../Results/Table_*.tex
  -> \input{\exhibitspath/Table_*.tex}
  -> compiled paper
```

SA07 should cease owning the three full-sample tables. It continues to own
Figure IA.1 and should export its two paper-facing figure PDFs to top-level
`../Results/`, matching the manuscript paths.

## Analysis changes

Retain the existing sample-specific audit contract under
`RiskAdjusted/TstatFilter/`. Add or retain the equivalent corrected
full-sample audit contract under `RiskAdjusted/FullSampleTstatFilter/`.

S4b should write these six paper fragments at top-level `../Results/`:

1. `Table_RiskAdjusted_TimeVarying_ff4_t2.tex`;
2. `Table_RiskAdjusted_TimeVarying_DisciplineJournal_ff4_t2.tex`;
3. `Table_RiskAdjusted_TimeVarying_AnyModelVsNoModel_ff4_t2.tex`;
4. `Table_RiskAdjusted_FullSample_Appendix.tex`;
5. `Table_RiskAdjusted_FullSample_DisciplineJournal_Appendix.tex`; and
6. `Table_RiskAdjusted_FullSample_AnyModelVsNoModel_Appendix.tex`.

The full-sample CAPM/FF3 estimands currently computed in SA07 must move into
S4b or shared helpers called by S4b. They must follow the phase-two contract:

- predictor-month means;
- calendar-month and predictor clustered inference;
- paired published-minus-data-mined differences; and
- a pooled `Any Model` estimate, not an average of the displayed model
  subgroups.

Render from unrounded summaries, never from audit CSVs or rounded TeX. Preserve
the paper presentation: main-table headers and ordering; Agnostic, Mispricing,
Risk order; `Equilibrium Modeling`; the two-line Dynamic/Quantitative label;
bold Overall; discipline and journal order; `Raw`, `CAPM`, and `FF3` headers in
the full-sample appendix; `Theoretical Explanation` in IA.6; and no empty
category headings in IA.10 or IA.11.

Each generated paper file should contain exactly one complete `tabular`.
Captions, labels, notes, floats, font size, placement, and column spacing belong
to the writing repository.

## Writing changes

Replace the inline tabulars for Tables 6, 7, and IA.10 with the corresponding
inputs. For IA.6, IA.7, and IA.11, move the float-owned markup from the manual
input files into the manuscript and input only the generated tabular fragment.
Update prose ranges that no longer match corrected displayed values.

For an arXiv package, copy the same six top-level fragments into the writing
repository's `exhibits/` directory before changing `\exhibitspath` to
`exhibits`.

## Tests and validation

1. Add exact expected-output tests for all six paper fragments.
2. Preserve the sample-specific audit test and add an exact corrected
   full-sample audit contract.
3. Assert that every paper output has one `tabular` and no float, caption,
   label, or document wrapper.
4. Verify every manuscript `\exhibitspath` reference resolves at top-level
   `../Results/`.
5. Build with `pdflatex` and `biber` in a temporary directory, leaving the
   project PDF untouched.
6. Inspect Tables 6, 7, IA.6, IA.7, IA.10, and IA.11 for values, widths, line
   breaks, ordering, numbering, and page placement.
7. Verify the copied `exhibits/` path produces the same six tables.
8. Keep `docs/exhibit_map.md` explicit about calculation generation, paper
   artifact generation, and manuscript wiring.

## Main risks

The principal risks are dual ownership between S4b and SA07, retaining SA07's
obsolete subgroup-average `Any Model` calculation, and silently changing table
presentation while eliminating manual transcription. The full-sample move is
a statistical-consolidation task, not merely a filename copy.
