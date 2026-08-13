# Derive valid-denominator metadata from the raw Compustat pull.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript 1a_ValidDenoms.R
# (MAIN.R sources it; it also runs standalone.)
#
# This stage is pure computation off the existing raw data: it needs no WRDS
# connection, so it runs on both MAIN.R paths, including when the download
# stage (1_Download_and_Clean.R) is skipped. It was split out of that script
# for exactly that reason.
#
# Inputs:  ../Data/Raw/CompustatAnnual.RData, compnames/globalSettings from
#          0_Environment.R
# Outputs: DataIntermediate/freq_obs_1963.csv
#          DataIntermediate/validDenomsCombinations.csv

source('0_Environment.R')

# Save data for valid denominators ------------------------------------------------

comp0 = readRDS('../Data/Raw/CompustatAnnual.RData') 

# check some basic stuff to console
comp0 %>% 
  transmute(gvkey, year = year(datadate), permno, me_datadate, at) %>% 
  group_by(year) %>% 
  summarize(
    npermno = sum(!is.na(permno))
    , nme = sum(!is.na(me_datadate))
    , nat = sum(!is.na(at))
    , ngvkey = sum(!is.na(gvkey))
  ) %>% 
  print(n=30)

# count obs
fobs_list = comp0 %>% 
  filter(year(datadate)==1963, !is.na(permno)) %>% 
  arrange(gvkey, datadate) %>% 
  group_by(gvkey) %>% 
  filter(row_number() == 1) %>% 
  ungroup() %>% 
  summarise(across(everything(), function(x) sum(!is.na(x) & x!=0)/length(x)) ) %>% 
  pivot_longer(cols = everything()) %>% 
  transmute(
    name, freq_obs_1963 = value
  )

# keep only accounting vars + crsp me
tempnames = union(compnames$yz.numer, compnames$yz.denom) 
fobs_list = fobs_list %>% 
  filter(name %in% tempnames) %>% 
  arrange(-freq_obs_1963)

fwrite(fobs_list, 'DataIntermediate/freq_obs_1963.csv')



# Save data for valid denominators in bivariate comparisons ---------------

# count non-missing observations for each variable in each year
for (yy in 1963:max(year(comp0$datadate))) {
  
  print(yy)  
  temp = comp0 %>% 
    filter(year(datadate)==yy, !is.na(permno)) %>% 
    arrange(gvkey, datadate) %>% 
    group_by(gvkey) %>% 
    filter(row_number() == 1) %>% 
    ungroup() %>% 
    summarise(across(everything(), function(x) sum(!is.na(x) & x!=0)/length(x)) ) %>% 
    pivot_longer(cols = everything()) %>% 
    transmute(
      name, freq_obs_1963 = value
    )
  
  colnames(temp) = c('name', paste0('freq_obs_', yy))
  
  if (yy==1963) {
    fobs_list_annual = temp
  } else {
    fobs_list_annual = full_join(fobs_list_annual, temp, by='name')
  }
  
}

# Reshape longer
fobs_list_annual = fobs_list_annual %>% 
  pivot_longer(cols = -name, names_to = 'year', values_to = 'freq_obs') %>% 
  mutate(year = as.numeric(str_remove(year, 'freq_obs_'))) %>% 
  # Reduce
  filter(name %in% tempnames)

# For each pair of variables, figure out which one has more non-missing obs 
# in the first year in which they both appear
validDenoms = fobs_list_annual %>% 
  # Cross
  full_join(fobs_list_annual, by = 'year', relationship = 'many-to-many') %>% 
  # Remove self-join
  filter(name.x != name.y) %>%
  # Keep first year in which both are non-zero
  filter(freq_obs.x >0, freq_obs.y >0) %>% 
  group_by(name.x, name.y) %>%
  filter(row_number() == 1) %>% 
  ungroup() %>% 
  # paste names alphabetically
  mutate(combName = ifelse(name.x < name.y, paste(name.x, name.y, sep='|'), 
                           paste(name.y, name.x, sep='|')) %>% as.character()) %>% 
  # Denominator: Variable with more non-missing obs (break ties alphabetically)
  mutate(denom = ifelse(freq_obs.x > freq_obs.y | (freq_obs.x == freq_obs.y & name.x < name.y), 
                        name.x, 
                        name.y)) %>% 
  select(combName, year, denom) %>% 
  distinct()

fwrite(validDenoms, 'DataIntermediate/validDenomsCombinations.csv')


# manual checks -----------------------------------------------------------

fobs_list %>% 
  filter(name == 'me_datadate')

fobs_list %>% 
  filter(freq_obs_1963 > globalSettings$denom_min_fobs)

namecheck = fobs_list %>% 
  filter(freq_obs_1963 > globalSettings$denom_min_fobs) %>% 
  pull(name) 

# setdiff(namecheck, compnames$pos_in_1963)
# setdiff(compnames$pos_in_1963, namecheck)


