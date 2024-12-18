
# Setup -------------------------------------------------------------------------
library(writexl)

# read matched strategies and get their documentation from stratdat
#   clean me up pls
matchdat = candidateReturns # loaded earlier

DMname = paste0('../Data/Processed/',
                globalSettings$dataVersion, 
                ' LongShort.RData')

stratdat = readRDS(DMname) # only really need the signal_list from here

# load pub stuff, add author info
inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType, 
                                  topT = globalSettings$topT)

czsum2 = czsum %>% left_join(
  fread('../Data/Raw/SignalDoc.csv') %>% 
    select(1:20) %>% 
    transmute(signalname = Acronym, Authors, Year, Journal, LongDescription, sign = Sign)  
) %>% 
  filter(signalname %in% inclSignals) %>% 
  setDT()

czret2 = czret %>% 
  filter(Keep == 1) %>% 
  left_join(
    czsum2 %>% select(signalname,sign) 
  ) %>% 
  filter(signalname %in% inclSignals) %>% 
  setDT()

# Merge dm and pub --------------------------------------------------------

# set up for merge
czret2[
  , ':=' (
    samptype = if_else(samptype == 'postpub','oos',samptype)
    , source = '1_pub'
    , candSignalname = NA
  )
]

matchdat[ , source := '2_dm']

# merge
allret = matchdat %>% rbind(
  czret2 %>% transmute(
    candSignalname, eventDate, ret = retOrig, samptype, source, actSignal = signalname, sign
  )
)

# Compare all -------------------------------------------------------------

# not used right now but may be useful for appendix

candsum = allret[
  !is.na(samptype) & !is.na(ret)
  , .(rbar = mean(ret), nmonth = .N, t = mean(ret)/sd(ret)*sqrt(.N))
  , by = c('source','actSignal','candSignalname','samptype')
]

pubsum = candsum[
  , .(rbar = mean(rbar), nmonth = mean(nmonth), t = mean(t), nstrat = .N)
  , by = c('actSignal','source','samptype')
] %>% 
  arrange(actSignal, source, samptype) %>% 
  select(source,actSignal, samptype, rbar, t, nstrat) %>% 
  pivot_wider(
    id_cols = c('actSignal', 'source')
    , names_from = samptype
    , values_from = c(rbar, t, nstrat)
  ) %>% 
  setDT()

pubsum2 = pubsum[ source == '1_pub'] %>% select(actSignal, starts_with('rbar_'), t_insamp) %>% 
  left_join(
    pubsum[ source == '2_dm'] %>% transmute(
      actSignal, rbar_insamp_dm = rbar_insamp, t_insamp_dm = t_insamp, rbar_oos_dm = rbar_oos, nmatch = nstrat_insamp
    )
  ) %>% 
  mutate(diff_rbar_oos = rbar_oos - rbar_oos_dm)

# Inspect select predictors -----------------------------------------------------------

# read compvars doc
compdoc = readxl::read_xlsx('DataInput/Yan-Zheng-Compustat-Vars.xlsx') %>% 
  transmute(
    acronym = tolower(acronym)
    , longname 
    , shortername 
  ) 


# make tables
namelist = c('Size','BMdec','Mom12m','realestate','OrgCap','Coskewness')

tabout = lapply(namelist, inspect_one_pub)
names(tabout) = namelist

tabout$allsignals = pubsum2 

# save to disk
write_xlsx(tabout, '../Results/InspectMatch.xlsx')

# Check diversity to console --------------------------------------------------------------------
# not sure how to put this in the paper =()

tabout$BMdec %>% distinct(v1long, signal_form) %>% print(n=Inf)

tabout$BMdec %>% distinct(v2long) %>% pull(v2long)

tabout$BMdec %>% 
  filter(!grepl('capx', v2)) %>%
  filter(!grepl('Market equity FYE', v2long)) %>%  
  select(signal, sign, signal_form, v1long, v2long, 5:6) %>% 
  distinct(v1long, signal_form, .keep_all = T) %>% 
  select(-ends_with('long'),-signal_form) %>% 
  as.data.frame() %>% 
  arrange(signal)

tabout$Mom12m %>% 
  filter(!grepl('capx', v2)) %>%
  filter(!grepl('Market equity FYE', v2long)) %>%  
  select(signal, sign, signal_form, v1long, v2long, 5:6) %>% 
  distinct(v1long, signal_form, .keep_all = T) %>% 
  select(-ends_with('long'),-signal_form) %>% 
  as.data.frame() %>% 
  arrange(signal)

# Make latex inputs -------------------------------------------------------

outpath = '../Results/'

write_tex_from_tab = function(
    tab, id1 = 1:10, id2 = 21:25, 
    signalnamelong = '12-Month Momentum (JT Style)',
    filename = 'inspect-Mom12m.tex'
){
  
  # measure and focus
  nsignal = tab %>% filter(source == '2_dm') %>% pull(id) %>% max
  id3 = (nsignal-4):nsignal
  tab2 = tab %>% select(source, id, signal, sign, starts_with('x')) %>% 
    filter(id %in% c(id1, id2, id3) | source != '2_dm') %>% 
    mutate(sign = if_else(source == '3_dm_mean', NA_real_, sign)) %>% 
    print() 
  ncol = dim(tab2)[2]
  
  # insert blanks
  blank1 = tibble(source = '2_dm', id = max(id1)+1,signal =  '. . .')
  blank2 = tibble(source = '2_dm', id = max(id2)+1,signal =  '. . .') 
  tab3 = tab2 %>% bind_rows(blank1) %>% bind_rows(blank2) %>%  
    arrange(source, id) %>% 
    mutate(id = if_else(signal == '. . .', NA_real_, id)) %>%     
    print()
  
  # format
  tab4 = tab3 %>% 
    mutate(across(starts_with('x'), ~ sprintf('%0.2f', .x) )) %>% 
    mutate(across(everything(), ~ as.character(.x))) %>% 
    mutate(across(everything(), ~ replace_na(.x,''))) %>% 
    mutate(
      sign = if_else(is.na(sign), '', sign)
    ) %>% 
    mutate_all(funs(str_replace(., "NA", ""))) %>%
    mutate(
      signal = if_else(source == '1_pub', signalnamelong, signal)
      , signal = if_else(source == '3_dm_mean', 'Mean Data-Mined', signal)
      , signal = str_replace_all(signal, '&', '\\\\&')
    ) %>% 
    print()
  
  # make top
  top = tab4[1, 2:ncol] %>% 
    xtable() %>% 
    print(
      include.rownames = FALSE, include.colnames = FALSE,
      hline.after = NULL, only.contents = TRUE, comment = TRUE,
      sanitize.text.function=function(x){x}
    ) %>% 
    paste0(
      '\\midrule \n'
      , '\\multicolumn{5}{l}{\\textit{Data-Mined}} \\\\ \n'
      ,'\\midrule \n' 
    )
  
  # make middle
  mid =  
    tab4[2:(nrow(tab4)-1), 2:ncol]  %>% 
    xtable() %>% 
    print(
      include.rownames = FALSE, include.colnames = FALSE,
      hline.after = NULL, only.contents = TRUE, comment = FALSE,
      sanitize.text.function=function(x){x}
    ) %>% 
    paste0(
      '\\midrule \n'
    )
  
  # make bottom
  bottom = tab4[nrow(tab4), 2:ncol]  %>% 
    xtable() %>% 
    print(
      include.rownames = FALSE, include.colnames = FALSE,
      hline.after = NULL, only.contents = TRUE, comment = FALSE,
      sanitize.text.function=function(x){x}
    )
  
  
  # concat and save
  tex = paste0(top,mid, bottom)
  cat(tex, file = paste0(outpath, filename))
  
}

# bm
tab = readxl::read_xlsx(paste0(outpath,'InspectMatch.xlsx'), sheet = 'BMdec') %>% 
  janitor::clean_names() %>% 
  as.data.frame()

write_tex_from_tab(tab, id1 = 1:10, id2 = 101:105, 
                   signalnamelong = 'Book / Market (Fama-French 1992)',
                   filename = 'inspect-BMdec.tex')

# momentum
tab = readxl::read_xlsx(paste0(outpath,'InspectMatch.xlsx'), sheet = 'Mom12m') %>% 
  janitor::clean_names() %>% 
  as.data.frame()

write_tex_from_tab(tab, id1 = 1:10, id2 = 21:25, 
                   signalnamelong = '12-Month Momentum (Jegadeesh-Titman 1993)',
                   filename = 'inspect-Mom12m.tex')

# size
tab = readxl::read_xlsx(paste0(outpath,'InspectMatch.xlsx'), sheet = 'Size') %>% 
  janitor::clean_names() %>% 
  as.data.frame()

write_tex_from_tab(tab, id1 = 1:10, id2 = 101:105, 
                   signalnamelong = 'Size (Banz 1981)',
                   filename = 'inspect-Size.tex')

# Tuzel
tab = readxl::read_xlsx(paste0(outpath,'InspectMatch.xlsx'), sheet = 'realestate') %>% 
  janitor::clean_names() %>% 
  as.data.frame()

write_tex_from_tab(tab, id1 = 1:10, id2 = 101:105, 
                   signalnamelong = 'Real Estate (Tuzel 2010)',
                   filename = 'inspect-realestate.tex')





