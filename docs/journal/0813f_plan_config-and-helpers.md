# Plan: separate configuration, bootstrap, and helpers

Snapshot written 2026-08-13 22:18 EDT.

Suggested responsibilities:

- `config.R`: consequential run choices, especially stage switches and settings
  that affect expensive cached outputs.
- `0_Environment.R`: the single bootstrap entry point; load packages, set R
  options, create directories, and source `config.R` and helper files.
- `helpers/`: reusable functions, grouped into a few thematic files such as
  mining, matching, and plotting.
- `MAIN.R`: orchestration and execution order only.

Keep package loading out of `config.R`. Move functions out of
`0_Environment.R` without changing behavior or interfaces, but defer that work
to a fresh agent because source-order, global-variable, and package dependencies
need a dedicated audit.
