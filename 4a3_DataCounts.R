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
  filter(signalname %in% inclSignals) %>% 
  mutate(modeltype = case_when(NoModel == 1 ~ 'No Model',
                               Stylized == 1 ~ 'Stylized',
                               Dynamic == 1 ~ 'Dynamic',
                               Quantitative == 1 ~ 'Quantitative')) %>%
  mutate(modeltype = factor(modeltype, levels = c('No Model', 'Stylized', 'Dynamic', 'Quantitative'))) %>%
  select(signalname, theory, modeltype) 



# Journal counts -------------------------------------------------------------------


## for main paper: Risk/Mispricing and Model counts 

### Top 3 finance
finlist = globalSettings$top3Finance
tab_risk_or_mispricingTop3 = czsum %>% 
  filter(rbar_ok, n_ok, main_signal == 'main') %>% 
  left_join(czcat, by = 'signalname') %>% 
  mutate(Top3 = ifelse(Journal %in% finlist, 'Top3', 'Other')) %>%
  group_by(Top3, theory) %>%
  count() %>% 
  pivot_wider(names_from = Top3, values_from = n, values_fill = 0) %>% 
  mutate(Any = Other + Top3) %>% 
  select(theory, Any, Top3) %>% 
  arrange(desc(theory))

### Top 3 accounting
acctlist = globalSettings$top3Accounting
tab_risk_or_mispricingTop3acct = czsum %>% 
  filter(rbar_ok, n_ok, main_signal == 'main') %>% 
  left_join(czcat, by = 'signalname') %>% 
  mutate(Top3Acct = ifelse(Journal %in% acctlist, 'Top3Acct', 'Other')) %>%
  group_by(Top3Acct, theory) %>%
  count() %>% 
  pivot_wider(names_from = Top3Acct, values_from = n, values_fill = 0) %>% 
  mutate(Any = Other + Top3Acct) %>% 
  select(theory, Top3Acct) %>% 
  arrange(desc(theory))

### Finance and accounting
tab_risk_or_mispricingJournal = czsum %>% 
  filter(rbar_ok, n_ok, main_signal == 'main') %>% 
  left_join(czcat, by = 'signalname') %>% 
  mutate(JournalCat = case_when(
    Journal %in% globalSettings$finlistAll  ~ 'Finance',
    Journal %in% globalSettings$acctlistAll ~ 'Accounting',
    TRUE                     ~ 'Other')) %>%
  group_by(JournalCat, theory) %>%
  count() %>% 
  pivot_wider(names_from = JournalCat, values_from = n, values_fill = 0) %>% 
  select(theory, Finance, Accounting) %>% 
  arrange(desc(theory))

## Combine the three tables and save as tex
tab_risk_or_mispricing = tab_risk_or_mispricingTop3 %>% 
  left_join(tab_risk_or_mispricingTop3acct, by = 'theory') %>% 
  left_join(tab_risk_or_mispricingJournal,  by = 'theory') %>% 
  select(theory, Any, Top3, Top3Acct, Finance, Accounting) %>% 
  arrange(desc(theory)) %>% 
  mutate(theory = str_to_title(theory))

tab_risk_or_mispricing %>% 
  xtable() %>% 
  print.xtable(
    include.rownames = FALSE, 
    include.colnames = FALSE,
    hline.after = NULL,
    only.contents = TRUE,
    file = '../Results/ApproachVsJournalsPart1.tex', sep = ',')

## Model type table

### Top 3 finance
tab_modeltypeTop3 = czsum %>% 
  filter(rbar_ok, n_ok, main_signal == 'main') %>% 
  left_join(czcat, by = 'signalname') %>% 
  mutate(Top3 = ifelse(Journal %in% finlist, 'Top3', 'Other')) %>%
  group_by(Top3, modeltype) %>%
  count() %>% 
  pivot_wider(names_from = Top3, values_from = n, values_fill = 0) %>% 
  mutate(Any = Other + Top3) %>% 
  select(modeltype, Any, Top3) %>% 
  arrange(modeltype)


### Top 3 accounting
tab_modeltypeTop3acct = czsum %>% 
  filter(rbar_ok, n_ok, main_signal == 'main') %>% 
  left_join(czcat, by = 'signalname') %>% 
  mutate(Top3acct = ifelse(Journal %in% acctlist, 'Top3acct', 'Other')) %>%
  group_by(Top3acct, modeltype) %>%
  count() %>% 
  pivot_wider(names_from = Top3acct, values_from = n, values_fill = 0) %>% 
  select(modeltype, Top3acct) %>% 
  arrange(modeltype)

### Finance and accounting
tab_modeltypeJournal = czsum %>% 
  filter(rbar_ok, n_ok, main_signal == 'main') %>% 
  left_join(czcat, by = 'signalname') %>% 
  mutate(JournalCat = case_when(
    Journal %in% globalSettings$finlistAll  ~ 'Finance',
    Journal %in% globalSettings$acctlistAll ~ 'Accounting',
    TRUE                     ~ 'Other')) %>%
  group_by(JournalCat, modeltype) %>%
  count() %>% 
  pivot_wider(names_from = JournalCat, values_from = n, values_fill = 0) %>% 
  select(modeltype, Finance, Accounting) %>% 
  arrange(modeltype)

## Combine the three tables and save as tex
tab_modeltype = tab_modeltypeTop3 %>% 
  left_join(tab_modeltypeTop3acct, by = 'modeltype') %>% 
  left_join(tab_modeltypeJournal, by = 'modeltype') %>% 
  select(modeltype, Any, Top3, Top3acct, Finance, Accounting) %>% 
  arrange(modeltype) 

tab_modeltype %>% 
  xtable() %>% 
  print.xtable(
    include.rownames = FALSE, 
    include.colnames = FALSE,
    hline.after = NULL,
    only.contents = TRUE,
    file = '../Results/ApproachVsJournalsPart2.tex', sep = ',')

## add totals
tab_sum = tab_modeltype %>%
  ungroup() %>% 
  select(-modeltype) %>%
  summarise(across(everything(), sum, na.rm = TRUE)) %>% 
  mutate(modeltype = '\\textbf{Total}') %>% 
  select(modeltype, everything())

tab_sum %>% 
  xtable() %>% 
  print.xtable(
    include.rownames = FALSE, 
    include.colnames = FALSE,
    hline.after = NULL,
    only.contents = TRUE,
    file = '../Results/ApproachVsJournalsPart3.tex', 
    sep = ',',
    sanitize.text.function = identity)

# Risk vs mispricing example quotes table ---------------------------------

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


# For DM counts ---------------------------------------------------------------

source('0_Environment.R')

# Define n1 and n2 from compnames for data counts
n1 <- length(compnames$yz.numer)
n2 <- length(compnames$yz.denom)

dmdat = readRDS(paste0('../Data/Processed/',
                       globalSettings$dataVersion, 
                       ' LongShort.RData'))


denom_ok = fread('DataIntermediate/freq_obs_1963.csv') %>% 
  filter(freq_obs_1963 > globalSettings$denom_min_fobs) 


# Write to md file -------------------------------------------------------------

sink('../Results/DataCounts.md')
print(paste0('Predictors in Chen-Zim data: ', nrow(czsum)))

print(paste0('We drop ', nrow(czsum %>% filter(!rbar_ok)), ' predictors for tiny rbar (distant from original papers)'))
print(czsum %>% filter(!rbar_ok) )

temp = czsum %>% filter(rbar_ok) %>% filter(!n_ok)
print(paste0('We drop ', nrow(temp), ' predictors for post-sample periods < 9 years'))
print(temp)

temp = czsum %>% filter(rbar_ok, n_ok, main_signal != 'main') 
print(paste0('We drop ', nrow(temp), ' predictors to limit each paper to two main signals'))
print(temp)

temp = czsum %>% filter(rbar_ok, n_ok, main_signal == 'main') 
print(paste0('We keep ', nrow(temp), ' published signals for the main analysis '))

print('\n')

print('Share of predictors that are risk or mispricing')
temp = tab_risk_or_mispricing %>% 
  ungroup() %>% 
  mutate(across(where(is.integer), ~ as.numeric(.))) %>% 
  mutate(
    pct_Top3 = Top3 / sum(Top3) * 100
    , pct_Any = Any / sum(Any) * 100
  ) %>% 
  select(theory, starts_with('pct')) %>% 
  print()

# DM counts
print('\n')
print('DM counts')
print(paste0('Total ratios: ',nrow(dmdat$signal_list)))
print(paste0('Numerators: ',n1))
print(paste0('Denominators: ',n2))
print(paste0('Number of ratios before removing redundant (n1*n2*2): ',n1*n2*2))
print(paste0('Number of ratios dropped (n1*n2*2 - nrow(dmdat$signal_list)): ',n1*n2*2 - nrow(dmdat$signal_list)))

print(paste0('Number of ratios where the numerator is also a valid denominator (n2*n2): ',n2*n2))
print(paste0('Number of distinct ratios where the numerator is also a valid denominator (n2 choose 2): ',choose(n2,2)))
print(paste0('Number of ratios dropped (n2*n2 - choose(n2,2)): ',n2*n2 - choose(n2,2)))

sink()


