# demo for how to download and use Chen, Lopez-Lira, Zimmermann strategies

# Setup -------------------------------------------------------------------
rm(list = ls())
library(tidyverse)
library(data.table)
library(googledrive)

# temp folder for output and working space
dir.create('../temp/')

# gdrive path
dataurl = 'https://drive.google.com/drive/folders/16RqeHNyU5gcqjRUvqSeOfQCxu_B2mfcZ'

# look around
dataurl %>% drive_ls() 

# download doc
target_dribble = dataurl %>% drive_ls() %>% 
  filter(name == 'DataMinedSignalList.csv')

drive_download(
  as_id(target_dribble$id), path = '../temp/dmdoc.csv', overwrite = T
)


# download ew strats (600 mb)
target_dribble = dataurl %>% drive_ls() %>% 
  filter(name == 'DataMinedLongShortReturnsEW.csv')

drive_download(
  as_id(target_dribble$id), path = '../temp/dmret.csv', overwrite = T
)


# Chen-Zimmermann root of March 2022 release
pathRelease = 'https://drive.google.com/drive/folders/1O18scg9iBTiBaDiQFhoGxdn4FdsbMqGo'

target_dribble = pathRelease %>% drive_ls() %>% 
  filter(name=='Portfolios') %>%  drive_ls() %>% 
  filter(name=='Full Sets OP') %>% drive_ls() %>% 
  filter(name=='PredictorSummary.xlsx')
  
drive_download(
  as_id(target_dribble$id), path = '../temp/czsum.xlsx', overwrite = T
)

target_dribble = pathRelease %>% drive_ls() %>% 
  filter(name=='Portfolios') %>%  drive_ls() %>% 
  filter(name=='Full Sets OP') %>% drive_ls() %>% 
  filter(name=='PredictorPortsFull.csv')

drive_download(
  as_id(target_dribble$id), path = '../temp/czret.csv', overwrite = T
)


# read into memory -------------------------------------------------------------------------
library(tidyverse)
library(data.table)
library(googledrive)


dmret = fread('../temp/dmret.csv') %>% 
  mutate(time = year + month/12)
dmdoc = fread('../temp/dmdoc.csv')

czsum = readxl::read_excel('../temp/czsum.xlsx') %>% 
  select(signalname, starts_with('Sample'),  tstat, rbar, Authors, Year)

czret = fread('../temp/czret.csv') %>% 
  mutate(time = year(date) + month(date)/12)



# do stuff ----------------------------------------------------------------

# find summary stats that match FF92
bmsamp = czsum %>% filter(signalname == 'BMdec')

dmsamp = dmret[year >= bmsamp$SampleStartYear & year <= bmsamp$SampleEndYear][
  , .(tstat = mean(ret)/sd(ret)*sqrt(.N), rbar = mean(ret))
  , by = 'signalid'
] %>% 
  # sign 
  mutate(sign = sign(rbar)) %>% 
  mutate(
    tstat = tstat*sign, rbar = rbar*sign
  )


# find matches
matchlist = dmsamp %>% 
  filter(abs(tstat/bmsamp$tstat-1) < 0.1
         , abs(rbar/bmsamp$rbar-1) < 0.3) %>% 
  left_join(dmdoc, by = 'signalid')

# make matched returns, with sign
matchret = dmret %>% 
  inner_join(matchlist, by = 'signalid') %>% 
  mutate(ret = sign*ret)

# sanity check
matchret[year >= bmsamp$SampleStartYear & year <= bmsamp$SampleEndYear] %>% 
  group_by(signalid) %>% 
  summarize(tstat = mean(ret)/sd(ret)*sqrt(n()), rbar = mean(ret)) %>% 
  print(n=20)

matchret[year >= bmsamp$SampleEndYear + 1] %>% 
  group_by(signalid) %>% 
  summarize(tstat = mean(ret)/sd(ret)*sqrt(n()), rbar = mean(ret)) %>% 
  print(n=20) %>% 
  summarize(mean(rbar))
  
  
# compare returns
startyear = 1993

plotbm = czret %>% filter(signalname == 'BMdec', port == 'LS') %>% 
  filter(time > startyear) %>% 
  arrange(time) %>% 
  mutate(
    cret = cumprod(1+ret/100)
  )

plotdm = matchret %>% 
  filter(time >= startyear) %>% 
  arrange(signalid, time) %>% 
  group_by(signalid) %>% 
  mutate(
    cret = (cumprod(1+ret/100))
  )

plotdmmean = matchret %>% 
  filter(time >= startyear) %>% 
  group_by(time) %>% 
  summarize(ret = mean(ret)) %>% 
  mutate(
    cret = (cumprod(1+ret/100))
  )

colors = c(rgb(0,0.4470,0.7410), # MATBLUE
           rgb(0.8500, 0.3250, 0.0980), # MATRED
           rgb(0.9290, 0.6940, 0.1250) # MATYELLOW
)


ggplot(plotdm, aes(x=time, y=cret)) +
  geom_line(aes(group = signalid, color = 'a')) +
  geom_line(data=plotdmmean, aes(color = 'b'), size = 1.5) +  
  geom_line(data=plotbm, aes(color = 'c'), size = 1.5) +
  scale_color_manual(name = 'data source', 
                     values =c(
                       'a'='grey'
                         ,'b'=colors[1]
                         ,'c'=colors[2]                   
                     ), 
                     labels = c(
                       '190 Data-Mined Signals w/ t-stat Close to BM\'s'
                       ,'Mean Data-Mined'
                       ,'Book-to-Market'
                     ))  +
  ylab('Value of $1 invested in 1993')+
  theme_minimal(base_size = 18) +
  theme(
    legend.title = element_blank(), legend.position = c(30,85)/100
  )  +
  scale_x_continuous(breaks = seq(1900,2040,10)) +
  coord_cartesian(xlim = c(1990,2025), ylim = c(-10,40)) 



ggsave('../temp/BMdec_vs_DM.jpg')



