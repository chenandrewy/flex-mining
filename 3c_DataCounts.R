# counts dropped predictors, journals, data summary stuff.
# also counts dm stuff

# Predictor Counts -------------------------------------------------------------------

source('0_Environment.R')

inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType, 
                                  topT = globalSettings$topT)

czsum = readRDS('../Data/Processed/czsum_allpredictors.RDS') %>% 
  select(signalname,rbar,nobs_postsamp,rbar_ok,n_ok,Keep) %>% 
  left_join(
  fread('../Data/Raw/SignalDoc.csv')[, 1:15] %>% 
  transmute(signalname = Acronym, Journal, Authors, Year, SampleStartYear, SampleEndYear)
  ) %>% 
  mutate(
    main_signal = if_else(signalname %in% inclSignals,'main', 'extra')
  )

czcat = fread('DataInput/SignalsTheoryChecked.csv') %>% 
  select(signalname, theory) %>% 
  filter(signalname %in% inclSignals)
  

# Write to md file -------------------------------------------------------------

sink('../Results/DataCounts.md')
print(paste0('Predictors in Chen-Zim data: ', nrow(czsum)))

print(paste0('We drop ', nrow(czsum %>% filter(!rbar_ok)), ' predictors for tiny rbar (distant from original papers)'))
print(czsum %>% filter(!rbar_ok) )

temp = czsum %>% filter(rbar_ok) %>% filter(!n_ok)
print(paste0('We drop ', nrow(temp), ' predictors for post-sample periods < 9 years'))
print(temp)

temp = czsum %>% filter(rbar_ok, n_ok, main_signal == 'main') 
print(paste0('We keep ', nrow(temp), ' published signals for the main analysis '))

sink()


# Journal counts -------------------------------------------------------------------


# for main paper (difficult to include automatically, copy and paste values please)
finlist = c('JF','RFS','JFE')
tab_risk_or_mispricing = czsum %>% 
  filter(rbar_ok, n_ok, main_signal == 'main') %>% 
  left_join(czcat, by = 'signalname') %>% 
  mutate(Top3 = ifelse(Journal %in% finlist, 'Top3', 'Other')) %>%
  group_by(Top3, theory) %>%
  count() %>% 
  pivot_wider(names_from = Top3, values_from = n, values_fill = 0) %>% 
  mutate(Any = Other + Top3)

# Create LaTeX table with actual values instead of \Sexpr commands
cat(sprintf('%% Risk vs Mispricing Table
\\begin{tabular}{lrrrl}
  \\toprule
    & \\multicolumn{2}{c}{Num Predictors} &   &  \\\\
  \\cmidrule{2-3}Category & \\multicolumn{1}{c}{Any} & \\multicolumn{1}{c}{\\multirow{2}[2]{*}{JF, JFE, RFS}} & \\multicolumn{1}{p{10.645em}}{Example Predictor} & \\multicolumn{1}{p{18.855em}}{Example  Passage} \\\\
    & \\multicolumn{1}{c}{Journal} &   &   &  \\\\
  \\midrule
  Risk & %d & %d & \\multicolumn{1}{p{10.645em}}{Real estate holdings \\newline{}(Tuzel 2010)} & \\multicolumn{1}{p{18.855em}}{Firms with high real estate holdings are more vulnerable to bad productivity shocks and hence are riskier and have higher expected returns.} \\\\
    &   &   &   &  \\\\
  Mispricing & %d & %d & \\multicolumn{1}{p{10.645em}}{Share repurchases \\newline{}(Ikenberry, Lakonishok, Vermaelen 1995)} & \\multicolumn{1}{p{18.855em}}{The market errs in its initial response and appears to ignore much of the information conveyed through repurchase announcements} \\\\
    &   &   &   &  \\\\
  Agnostic & %d & %d & \\multicolumn{1}{p{10.645em}}{Size \\newline{}(Banz 2981)} & \\multicolumn{1}{p{18.855em}}{To summarize, the size effect exists but it is not at all clear why it exists} \\\\
  \\midrule
  Total & %d & %d &   &  \\\\
  \\bottomrule
\\end{tabular}%%',
  filter(tab_risk_or_mispricing, theory == "risk")$Any,
  filter(tab_risk_or_mispricing, theory == "risk")$Top3,
  filter(tab_risk_or_mispricing, theory == "mispricing")$Any,
  filter(tab_risk_or_mispricing, theory == "mispricing")$Top3,
  filter(tab_risk_or_mispricing, theory == "agnostic")$Any,
  filter(tab_risk_or_mispricing, theory == "agnostic")$Top3,
  sum(tab_risk_or_mispricing$Any),
  sum(tab_risk_or_mispricing$Top3)
), file = '../Results/table_risk_vs_mispricing.tex')


# Detailed journal counts for appendix ------------------------------------------

finlist = c('JF','RFS','JFE','JFQA','MS')
acctlist = c('AR','RAS','JAR','JAE')

tabjournal = czsum %>% 
  filter(Keep) %>% 
  mutate(
    jcat = case_when(
      Journal %in% finlist ~ 'fin'
      , Journal %in% acctlist  ~ 'acct'
      , T ~ 'other'
    )
    , ntot = sum(Keep)
  ) %>% 
  group_by(jcat) %>% 
  summarize(n = n(), ntot = mean(ntot), pct = n/ntot*100)  %>% 
  mutate(
    labels = case_when(
      jcat == 'fin' ~ paste(finlist, collapse = ', ')
    )
  ) %>% 
  print()

# Count signals by journal and theory for appendix
czsum %>% 
  filter(rbar_ok, n_ok, main_signal == 'main') %>% 
  left_join(czcat, by = 'signalname') %>% 
  group_by(Journal, theory) %>% 
  count() %>% 
  pivot_wider(names_from = theory, values_from = n, values_fill = 0) %>% 
  xtable() %>% 
  print.xtable(include.rownames=FALSE, 
               include.colnames = FALSE,
               only.contents = TRUE,
               hline.after = NULL,
               file = '../Results/SignalsByTheoryAndJournal.tex', sep = ',')


# DM counts ---------------------------------------------------------------

source('0_Environment.R')

dmdat = readRDS(paste0('../Data/Processed/',
                       globalSettings$dataVersion, 
                       ' LongShort.RData'))

dmdat %>% names()


dmdat$ret %>% distinct(signalid) 

print(nrow(dmdat$signal_list))
n1 = dmdat$signal_list %>% distinct(v1) %>% nrow()
n2 = dmdat$signal_list %>% distinct(v2) %>% nrow()


print(n1 * n2 + (n1-n2)*n2 + choose(n2,2))
print(n1)
print(n2)


dmdat$user$signal

n1 * n2 *2
n1 * n2 *2 - nrow(dmdat$signal_list)

n2*n2
