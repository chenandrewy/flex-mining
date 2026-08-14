# Plan: refactor flex-mining before chasing run errors

Snapshot written 2026-08-13 20:34 EDT. Branch `cleanup/main.r`.
Supersedes the approach in `260813a,to-do,MAIN.md` (run first, patch failures).

## Why the change

Started the evening running MAIN.R end to end and patching whatever broke.
Two and a half hours in, AC called it: the repo is messy enough that it is
hard to tell what any given failure would even mean. Fixing errors in scripts
we cannot justify keeping is wasted work.

New order: refactor and clean -> commit -> then test and fix errors.

## The finding that makes this tractable

The writing repo is a precise contract. `../risk-vs-writing/latex-risk-vs/`
`\input`s or `\includegraphics`es exactly **54 exhibits**, and
`latex-risk-vs/exhibits/` holds exactly **54 files**. So "unused" is
mechanically decidable: keep every script on some path to one of those 54,
delete the rest.

Scope decision (AC): **paper only**. Not slides/, not jeff-response/. Noted
risk: a script feeding only slides/ will look unused and get deleted.

Related findings from tonight:

- MAIN.R sources **13 of ~40** `4c*` scripts.
- **39 of the 54** used exhibits are absent from `../Results`. Some are just
  downstream of where tonight's run has reached (`4c2` -> the
  `Fig_PublicationsVsDataMining_*` set, `4c5` -> `Fig_FullSampleRiskAdj_*`,
  `4d2` -> `inspect-*`, `8a` -> `theme_ez_*`). Others come from scripts in no
  chain at all: `Fig2a`-`Fig2d` (from `4c20b_Fig2Plots.R`), `DM_pca`,
  `quantilesCorDM`, `Table_RiskAdjusted_FullSample_*`.
- So **MAIN.R does not currently rebuild the paper.** That is the real bug,
  and the reason tonight's run felt pointless.
- `4c6` prefix collision: `4c6_AccountingOnlyPlots.R` (in the `4_` chain) and
  `4c6_MPStyleDecayTables.R` (in no chain). The latter produces
  `Table_MPStyleRegsMain.tex` / `...Unscaled.tex`, from which the paper's
  `HandTable_MPStyleRegsMain.tex` and `HandTable_MPStyleRegsTimeFE.tex` are
  hand-transcribed (columns 1/3/5 and 2/4/6 of the two). Comparing the hand
  table against the current generated one already shows drift: -36.7 vs -36.6,
  -4.29 vs -4.30. Might be a stale vintage rather than a typo; nothing exists
  that could tell the difference.
- No R script writes into the writing repo. Every `risk-vs-writing` /
  `exhibits` grep hit is a comment. Outputs go to `../Results` and `../Data`,
  and get copied to `exhibits/` by hand.

## Plan

1. **Map** script -> outputs -> exhibit, read-only. Grep each script for what
   it writes; cross-check against `../Results` timestamps and the run log.
   This is the artifact that makes the repo legible, and it must exist before
   anything is deleted. ✅
2. **Cut** scripts that reach none of the 54. ✅
3. **Split** MAIN.R into two stages rather than merely reordering:
   - Stage A, build: `2a`-`2d`, the expensive data construction, writing
     `../Data/Processed`. Hours. Rerun when data changes.
   - Stage B, exhibits: everything reading those files to emit the 54.
     Minutes. Rerun freely while iterating on a figure.
   AC's framing was "heavy computations first" — already roughly true, `2a`
   ate 2h02m of the first 2h38m. The property actually wanted is the cache
   boundary, so a table tweak does not cost a two-hour rebuild. ✅
4. **rationalize settings**: focus on settings for the long-running scripts.
5. **Commit** the refactor.
6. **Then** test and fix errors, which is where `260813a` started.

## Tonight's run

Still going at 20:34, PID 21804, started 17:48. Entered
`4b_DMCorrelationsPCASummary.R` at ~20:26 (commented "around an hour or so").
Completed so far: `2a` 17:50-19:52, `2b`-`2d` to 20:03, `3a`-`3g` in about two
minutes, `4a` 20:05-20:26.

Decision pending. Mapping is read-only so the two do not collide, and the run
supplies free evidence (log `source()` lines correlated with files appearing
in `../Results`) for paths built by `paste0()` that will not grep cleanly.
The moment editing starts it has to die, since `source()` reads each file when
execution reaches it and a rename would produce a fake failure.

Open: does `4b` produce any of the 54? It is spending an hour right now.
`DM_pca` and `quantilesCorDM` are used exhibits with no known producer and
`4b`/`4e` are the candidates. The map answers this early; if `4b` feeds
nothing, it is the first satisfying deletion.

## Process note

Committing the `4c6` to-dos as an edit to `260813a` (commit `a785cbf`) was
wrong. That entry is a 17:46 snapshot; guidance is that a journal entry is not
a document to maintain. Worse, AC had already reverted an earlier mid-run edit
to the same file. Those items live here instead. Proposed: revert `260813a` to
its as-written state — awaiting AC.
