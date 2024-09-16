
# Pull DM and freq obs -------------------------------------------------------------------------
rm(list = ls())
source('0_Environment.R')

DMname = '../Data/Processed/CZ-style-v4 LongShort.RData' # for autofill convenience


lsdat = readRDS(DMname)



readxl::read_excel('DataInput/Yan-Zheng-Compustat-Vars.xlsx')

obs_1963 = fread('DataIntermediate/freq_obs_1963.csv')


# Compare lists -----------------------------------------------------------

temp = lsdat$signal_list %>% distinct(v2) %>% 
  transmute(name = v2, v2 = 1) %>% 
  full_join(obs_1963) 

temp %>% arrange(freq_obs_1963) %>% 
  filter(freq_obs_1963 > 0.25)


obs_1963 %>% filter(freq_obs_1963 > 0.25) %>% nrow


# Pull compustat -------------------------------------------------------------------------


user <- getPass('wrds username: ')
pass <- getPass('wrds password: ')

wrds <- dbConnect(Postgres(), 
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  sslmode='require',
                  dbname='wrds',
                  user=user,
                  password= pass)


qstring = paste0(
  'SELECT  gvkey, datadate, '
  , '*'
  , "  FROM comp.funda as a 
      WHERE a.consol = 'C'
      AND a.popsrc = 'D'
      AND a.datafmt = 'STD'
      AND a.curcd = 'USD'
      AND a.indfmt = 'INDL'
      AND (
        date_part('year', a.datadate) = 1963
        or 
        date_part('year', a.datadate) = 1985
      )
  "  
)  


compsmall = dbSendQuery(conn = wrds, statement = qstring) %>% 
  dbFetch(n = -1) %>% 
  as.data.table() %>% 
  janitor::clean_names()

setDT(compsmall)


# Count obs ---------------------------------------------------------------


complong = compsmall %>% 
  select(c('gvkey','datadate'), where(is.numeric)) %>% 
  pivot_longer(!c('gvkey','datadate')) %>% 
  filter(!is.na(value))  %>% 
  mutate(year = year(datadate))

compobs = complong %>% 
  group_by(year, name) %>% 
  summarize(nfirm = n())

  
compobs %>% 
  group_by(year) %>% 
  summarize(
    sum(nfirm > 200)
  )


compobs %>% 
  filter(year == 1985, nfirm > 200) %>% 
  arrange(nfirm)
  

