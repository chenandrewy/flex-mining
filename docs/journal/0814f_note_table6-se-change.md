# Note: Table 6 SE change from S4b clustered-inference fix

2026-08-14 14:41 America/New_York

Confirmed for the user that this branch (`fix/4c4_riskadj`) changed the
standard errors reported in Table 6 (`Table_RiskAdjusted_TimeVarying_ff4_t2.tex`,
§4). This comes from `d7a5020 Fix S4b clustered inference and pooled estimates`,
documented in `0814d_s4b_phase2_audit.md` / `0814d_s4b_phase2_comparison.csv`.

Point estimates in Table 6 are essentially unchanged. Standard errors roughly
doubled to tripled for the `ff4` column, e.g. (displayed, old → new):

| Row | pub_oos_se | outperform_se |
| --- | --- | --- |
| Overall | 3 → 7 | 3 → 8 |
| Risk | 6 → 14 | 7 → 12 |
| Mispricing | 4 → 8 | 4 → 9 |
| Agnostic | 7 → 22 | 8 → 24 |
| No Model | 3 → 8 | 4 → 9 |
| Stylized | 7 → 22 | 9 → 16 |
| Dynamic/Quantitative | 16 → 29 | 17 → 25 |

Cause: both the published-return mean and the paired published-minus-data-mined
outperformance are now estimated as an intercept from `stats::lm()` with variance
from `sandwich::vcovCL()`, two-way clustered on `date + pubname` (HC1,
`cadjust = TRUE`). This replaces the old iid standard errors and, for
outperformance, the old practice of adding marginal variances instead of
computing the paired variance directly — which is what the paper text already
promised (SEs clustered by calendar month and predictor) but the prior
implementation didn't actually do.

Net: this is a fix to match stated methodology, not a new choice. Same story
applies to Table 7 and Table IA.10 (also touched by S4b), per the same audit
doc — SEs move, point estimates don't (aside from the discipline
classification and Any-Model pooled-row changes noted separately in the
audit).
