# Prepare YZ data
source('0_Environment.R')

library(readxl)
library(janitor)

# read in
yzraw = read_sas('../Data Yan-Zheng/Yan_Zheng_RFS_Data.sas7bdat')

yzcode = list(
  prefix = read_xlsx('../Data Yan-Zheng/yz_codes.xlsx', sheet = 'prefix') %>% clean_names()
  , suffix = read_xlsx('../Data Yan-Zheng/yz_codes.xlsx', sheet = 'suffix') %>% clean_names()
)


# Create function list from yzcode ----------------------------------------------------------


# first make all combinations
allcode = expand_grid(
  yzcode$prefix, suffix_yz = c(yzcode$suffix$alpha, yzcode$suffix$compyz, NA_character_)
)  %>% 
  as.data.frame()

# clean to get 76 YZ transformations
allcode = allcode %>% 
  filter(
    (suffix_type == 'alpha' & suffix_yz %in% yzcode$suffix$alpha)
    | (suffix_type == 'compustat' & suffix_yz %in% yzcode$suffix$compyz)
    | (suffix_type == 'none' & is.na(suffix_yz))
  ) %>% 
  mutate(
    transformation = if_else(!is.na(suffix_yz), paste0(prefix, suffix_yz), prefix)
  )

# translate
yz_fun_list = allcode %>% 
  left_join(
    yzcode$suffix %>% transmute(suffix_yz = alpha, v2a = compus)
  ) %>% 
  left_join(
    yzcode$suffix %>% transmute(suffix_yz = compyz, v2b = compus)
  ) %>%   
  mutate(
    v2 = if_else(!is.na(v2a), v2a, v2b)
  ) %>% 
  transmute(
    transformation, signal_form = translation, v2
  )



# Make signal list --------------------------------------------------------
# each of 18,113 (fsvariable, transformation) is mapped to (v1, form, v2) and signalid

# make signal list (v1, form, v2) combinations
yz_signal_list = yzraw %>% distinct(transformation,fsvariable) %>% 
  arrange(transformation, fsvariable) %>% 
  mutate(
    signalid = row_number(), v1 = fsvariable
  ) %>% 
  left_join(yz_fun_list, by = 'transformation') %>% 
  transmute(
    signalid, v1, signal_form, v2, fsvariable, transformation
  )


# Rearrange returns -------------------------------------------------------

yz = yzraw %>% 
  left_join(
    yz_signal_list %>% select(signalid, fsvariable, transformation)
  ) %>% 
  pivot_longer(cols = starts_with('ddiff'), names_to = 'sweight'
               , names_prefix = 'ddiff_', values_to = 'ret') %>% 
  mutate(
    yearm = as.yearmon(DATE), ret= ret*100
    , nstock = NA_integer_
  ) %>% 
  select(sweight, signalid, yearm, ret, nstock)


# Organize and output -----------------------------------------------------

stratyz = list(
  ret = yz
  , signal_list = yz_signal_list
  , name = 'Yan-Zheng'
)

saveRDS(stratyz, '../Data/LongShortPortfolios/yz_reorg_all.RData')


