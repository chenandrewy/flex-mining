# Setup -------------------------------------------------------------------
rm(list = ls())
source('0_Environment.R')



# Load and process data ---------------------------------------------------
czsum = readRDS('../Data/Processed/czsum_allpredictors.RDS')

czcat = fread('DataInput/SignalsTheoryChecked.csv') %>% 
  select(signalname, Year, theory)

czret = readRDS('../Data/Processed/czret_keeponly.RDS') %>% 
  left_join(czcat, by = 'signalname') %>% 
  mutate(
    retOrig = ret
    , ret = ret/rbar*100
  )

FamaFrenchFactors <- readRDS('../Data/Raw/FamaFrenchFactors.RData') %>%
  rename(date = yearm)

czret <- czret %>% left_join(FamaFrenchFactors, by  = c('date'))


# Compute factor adjustments ----------------------------------------------
czret %>% setDT()

czret[, risk_via_mkt :=  mkt_implied_category(.SD), by = signalname]
czret[, risk_via_ff3 :=  ff3_implied_category(.SD), by = signalname]
czret[, risk_via_ff5 :=  ff5_implied_category(.SD), by = signalname]

add_catID(czret, "risk_via_mkt", 3)
add_catID(czret, "risk_via_ff3", 3)
add_catID(czret, "risk_via_ff5", 3)

average_by_ntile <- czret[eventDate >0 ,
                          .(ret = mean(ret)/100,
                            catID_risk_via_mkt = first(catID_risk_via_mkt),
                            catID_risk_via_ff3 = first(catID_risk_via_ff3),
                            catID_risk_via_ff5 = first(catID_risk_via_ff5),
                            risk_via_mkt = first(risk_via_mkt),
                            risk_via_ff3 = first(risk_via_ff3),
                            risk_via_ff5 = first(risk_via_ff5)
                            ), by = signalname]


# Plot results ------------------------------------------------------------
ylab <- "[Post-Sample]/[In-Sample]"
xlab <- "[Predicted by Risk Model]/[In-Sample]"

# Create the individual plots
xax <- c(-.5, 1)
repelsize = 6
repelcolor = 'royalblue4'

# CAPM
reg_capm = lm(ret ~ risk_via_mkt, average_by_ntile) %>% 
  summary()

p1 <- ggplot(average_by_ntile, aes(x=risk_via_mkt, y=ret)) +
  geom_hline(yintercept = 0, color = 'gray', size =2) +
  geom_vline(xintercept =  0, color = 'gray', size = 1) +
  geom_hline(yintercept = 1, color = 'gray', size = 2) +
  geom_point(size = 2.5) +
  theme_light(base_size = 26) +
  theme(
    legend.position = c(80,85)/100
    , legend.spacing.y = unit(0, units = 'cm')
    , legend.background = element_rect(fill='transparent')) +
  labs(
    x=xlab,
    y=ylab) +
  xlim(xax) + ylim(c(-1, 2))    +
  geom_abline(
    aes(intercept = reg_capm$coefficients[1], slope = reg_capm$coefficients[2])
    , color = colors[1], size = 2
  ) 

p1

ggsave('../Results/Fig_Risk_via_CAPM.pdf', p1, width = 10, height = 8)


# FF3
reg_ff3 = lm(ret ~ risk_via_ff3, average_by_ntile) %>% 
  summary()

p2 <- ggplot(average_by_ntile, aes(x=risk_via_ff3, y=ret)) +
  geom_hline(yintercept = 0, color = 'gray', size = 2) +
  geom_vline(xintercept =  0, color = 'gray', size = 1) +
  geom_hline(yintercept = 1, color = 'gray', size = 2) +
  geom_point(size = 2.5) +
  theme_light(base_size = 26) +
  theme(
    legend.position = c(80,85)/100
    , legend.spacing.y = unit(0, units = 'cm')
    , legend.background = element_rect(fill='transparent')) +
  labs(
    x=xlab,
    y=ylab) +
  xlim(xax) + ylim(c(-1, 2))    +
  geom_abline(
    aes(intercept = reg_ff3$coefficients[1], slope = reg_ff3$coefficients[2])
    , color = colors[1], size = 2
  ) 

p2

ggsave('../Results/Fig_Risk_via_FF3.pdf', p2, width = 10, height = 8)

# FF5
reg_ff5 = lm(ret ~ risk_via_ff5, average_by_ntile) %>% 
  summary()

p3 <- ggplot(average_by_ntile, aes(x=risk_via_ff5, y=ret)) +
  geom_hline(yintercept = 0, color = 'gray', size = 2) +
  geom_vline(xintercept =  0, color = 'gray', size = 1) +
  geom_hline(yintercept = 1, color = 'gray', size = 2) +
  geom_point(size = 2.5) +
  theme_light(base_size = 26) +
  theme(
    legend.position = c(80,85)/100
    , legend.spacing.y = unit(0, units = 'cm')
    , legend.background = element_rect(fill='transparent')) +
  labs(
    x=xlab,
    y=ylab) +
  xlim(xax) + ylim(c(-1, 2))    +
  geom_abline(
    aes(intercept = reg_ff5$coefficients[1], slope = reg_ff5$coefficients[2])
    , color = colors[1], size = 2
  ) 

p3

ggsave('../Results/Fig_Risk_via_FF5.pdf', p3, width = 10, height = 8)
