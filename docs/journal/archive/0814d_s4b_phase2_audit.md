# S4b phase-two statistical audit

Snapshot written 2026-08-14 after running the `CZ-style-v8b` cache. This is the
implementation audit for Tables 6, 7, and IA.10.

## Estimand and panel shape

The observation entering every reported mean is one published-predictor by
calendar-month row after the data-mined candidates have been normalized and
averaged within `(actSignal, eventDate)`. Thus, individual data-mined candidates
are equally weighted when their benchmark is formed, but they are not separate
observations in the table regressions. The final mean pools predictor-months;
predictors with longer post-sample histories consequently receive more weight.

The raw, CAPM, and FF4 post-sample panels have, respectively, 45,442, 38,193,
and 32,816 rows. They contain 157, 136, and 120 published predictors and 624,
564, and 552 calendar months. All three panels are unique on `(pubname, date)`.
In this cache every post-sample row has both a published return and its
data-mined benchmark, so the paired counts equal the row counts. The code now
asserts uniqueness, nonempty pairing, and at least two clusters in each
dimension and reports offending keys and counts on failure.

## Inference and pairing

The paper notes promise standard errors clustered by calendar month and
predictor for both the published return and published-minus-data-mined
outperformance. Each mean is now an intercept from `stats::lm()`, and its
variance is computed by `sandwich::vcovCL()` with clusters `date + pubname`,
`type = "HC1"`, `cadjust = TRUE`, `multi0 = FALSE`, and `fix = FALSE`. Thus the
calculation uses HC1 observation scaling and `vcovCL`'s dimension-specific
`G/(G - 1)` cluster adjustment. A group with fewer than two months or two
predictors, or with a non-finite/negative multiway variance, fails explicitly
rather than silently reverting to an unclustered standard error.

Outperformance is the row-level paired difference on complete published and
data-mined observations. Its clustered variance is estimated directly; the
published and benchmark variances are not added. When the two marginal samples
are identical, the code asserts that the paired mean equals the difference of
means. They are identical for every current raw, CAPM, and FF4 panel.

## Screens and classifications

The columns intentionally use different screened signal sets. The raw
published `t > 2` screen has 157 signals. The CAPM alpha screen has 151, of
which 147 also pass the raw screen; 136 remain after requiring a screened
data-mined match. The FF4 alpha screen has 135, of which 132 also pass raw; 120
remain after the matched-benchmark join. The CAPM and FF4 data-mined alpha
screens retain 57,173 and 47,603 `(actSignal, candSignalname)` pairs before
monthly aggregation. `S4b` now records these counts and asserts the published
screen/intersection definitions.

Discipline now uses the complete accounting list, while journal rank continues
to use only AR/JAR/JAE. Eight raw and CAPM signals move from Finance to
Accounting: `EarningsConsistency` (BAR), `ChForecastAccrual` (RAS),
`EquityDuration` (RAS), `ExclExp` (RAS), `RDIPO` (JBFA), `MS` (RAS),
`OrderBacklog` (RAS), and `ChInv` (RAS). The FF4 sample contains six of these;
`EquityDuration` and `ExclExp` are absent. The top-three journal rows do not
change membership.

## IA.10 and numerical changes

The correct Any Model input is the existing `model_binary == "Any Model"`
summary. It estimates the intended pooled predictor-month panel and its
covariance directly. Equal-weighting the displayed Stylized and
Dynamic/Quantitative estimates gives those two unequally sized subgroups the
same influence and cannot recover their covariance, so the approximation has
been removed.

The unrounded old/new value, difference, and old/new displayed value for all
168 estimates and standard errors are in
[`0814d_s4b_phase2_comparison.csv`](0814d_s4b_phase2_comparison.csv). Point
estimates are unchanged except for the discipline rows affected by the broader
accounting classification and the pooled Any Model row (apart from floating
point noise below display precision). Standard-error changes throughout the
tables come from replacing iid standard errors and added marginal variances
with the promised two-way clustered inference on published returns and paired
differences.

That comparison CSV is the authoritative numerical change audit. The three
CSV files beside the paper tabulars remain deliberately rounded human-review
artifacts, matching the existing six-file publication-source contract.

The corrected run took 143.844 seconds. It produced only the retained three
review CSVs and three complete TeX `tabular` fragments; no plot, diagnostic
export, or temporary PDF was created. The frozen phase-one fixtures remain
unchanged, and `tests/test_s4b_phase2_expected.R` records the corrected
displayed output.
