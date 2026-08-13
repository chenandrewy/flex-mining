# Writes the manuscript's combined 6-column MP-style decay table layout
# ((1)-(3) scaled pub/DM/diff, (4)-(6) unscaled) directly from six fixest
# models. Used by 4c6 (main Tables 3-4) and 4c22/4c23 (tolerance-spec drafts).

make_combined_table = function(fits, timeFE, file) {
  fmt  = function(x) formatC(signif(x, 3), format = 'g', digits = 3)
  fmt3 = function(x) formatC(x, format = 'f', digits = 3)
  cells = sapply(fits, function(f) {
    ct = fixest::coeftable(f)
    c(ps = fmt(ct['postSample', 'Estimate']), ps_se = paste0('(', fmt(ct['postSample', 'Std. Error']), ')'),
      pp = fmt(ct['postPub', 'Estimate']),    pp_se = paste0('(', fmt(ct['postPub', 'Std. Error']), ')'),
      n  = formatC(as.numeric(fixest::fitstat(f, 'n')$n), format = 'd', big.mark = ','),
      r2 = fmt3(as.numeric(fixest::fitstat(f, 'r2')$r2)),
      wr2 = fmt3(as.numeric(fixest::fitstat(f, 'wr2')$wr2)))
  })
  row = function(lab, key) paste0('   ', format(lab, width = 16), ' & ',
                                  paste(cells[key, ], collapse = ' & '), ' \\\\')
  hdr3 = function(x) paste0('                    & ', paste(x, collapse = '\n                    & '), ' \\\\')
  lines = c(
    '% GENERATED -- do not hand-edit (helpers/mp_table_helpers.R).',
    '\\begingroup', '\\setlength{\\tabcolsep}{0.55ex}%', '\\centering',
    '\\begin{tabular}{lcccccc}', '   \\toprule',
    '                    & \\multicolumn{3}{c}{(a) LHS = Scaled Return} & \\multicolumn{3}{c}{(b) LHS = Unscaled Return} \\\\',
    '   \\cmidrule(lr){2-4}\\cmidrule(lr){5-7}',
    hdr3(rep(c('Academic', 'Data Mining', 'Academic'), 2)),
    hdr3(rep(c('Signal', 'Benchmark', 'Minus'), 2)),
    hdr3(rep(c('Return', '(Uncorr)', 'Data Mining'), 2)),
    paste0('                    & ', paste0('(', 1:6, ')', collapse = ' & '), ' \\\\'),
    '   \\midrule',
    row('Post-Sample', 'ps'), row('SE', 'ps_se'),
    row("Post-Pub (Add'l)", 'pp'), row('SE', 'pp_se'),
    '   \\\\',
    row('Observations', 'n'), row('R$^2$', 'r2'), row('Within R$^2$', 'wr2'),
    '   \\\\',
    paste0('   Predictor F.E.   & ', paste(rep('Yes', 6), collapse = ' & '), ' \\\\'),
    paste0('   Time F.E.        & ', paste(rep(ifelse(timeFE, 'Yes', 'No'), 6), collapse = ' & '), ' \\\\'),
    '   \\bottomrule', '\\end{tabular}', '\\par\\endgroup'
  )
  writeLines(lines, file)
  cat('Wrote', file, '\n')
}
