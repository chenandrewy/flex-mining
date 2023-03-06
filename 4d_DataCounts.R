# counts dropped predictors, journals, data summary stuff.
# also counts dm stuff

# Predictor Counts -------------------------------------------------------------------

source('0_Environment.R')

czsum = readRDS('../Data/Processed/czsum_all207.RDS') %>% 
  select(signalname,rbar,nobs_postsamp,rbar_ok,n_ok,Keep) %>% 
  left_join(
  fread('../Data/Raw/SignalDoc.csv')[, 1:15] %>% 
  transmute(signalname = Acronym, Journal, Authors, Year, SampleStartYear, SampleEndYear)
  )


# pre-filtering counts
czsum  %>% 
  group_by(rbar_ok,n_ok) %>% 
  summarize(
    nsignal = n()
  )

# dropped signals
czsum %>% filter(!Keep | is.na(n_ok)) %>% 
  arrange(rbar_ok, n_ok)

# nobs info
czsum %>% 
  arrange(-SampleEndYear)

czsum %>% 
  filter(SampleEndYear <= 2012) 
204/207

czsum %>% 
  filter(Keep) %>% 
  summarize(
    mean(nobs_postsamp)/12
    , quantile(nobs_postsamp, c(0.25, 0.5))/12
  )


# journal counts
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

# pie chart: argh, this is hard to make in ggplot.
ggplot(tabjournal, aes(x='',y=pct,fill=jcat)) +
  geom_col() +
  geom_text(aes(label = pct),
            position = position_stack(vjust = 0.5),
            show.legend = FALSE) +
  coord_polar(theta = "y")



# DM counts ---------------------------------------------------------------

source('0_Environment.R')

dmdat = readRDS('../Data/Processed/CZ-style-v5 LongShort.RData')
        

dmdat %>% names()

dmdat$signal_list %>% distinct(v1)
dmdat$signal_list %>% pull(v2) %>% unique()

dmdat$ret %>% distinct(signalid) 
