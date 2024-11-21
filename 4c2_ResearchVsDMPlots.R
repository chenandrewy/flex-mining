# Data-mining comparisons 
# These are the new main plots as of 2024 04.

# Setup --------------------------------------------------------

rm(list = ls())

source("0_Environment.R")
library(doParallel)

## Load Global Data -------------------------------------------

# these are treated as globals (don't modify pls)
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(Keep) %>% 
  setDT()

czcat <- fread("DataInput/SignalsTheoryChecked.csv") %>%
  select(signalname, Year, theory)

czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  left_join(czcat, by = "signalname") %>%
  mutate(ret_scaled = ret / rbar * 100)

# Load pre-computed dm sumstats
dmcomp <- readRDS("../Data/Processed/dmcomp_sumstats.RDS")
dmtic <- readRDS("../Data/Processed/dmtic_sumstats.RDS")

# Specialized data prep function -------------------------------

make_ret_for_plotting <- function(plotdat, theory_filter = NULL){
  # takes dm selection criteria and returns data for an event time plot
  
  # # make event time returns for Compustat DM
  # temp = list()
  # temp$matched <- SelectDMStrats(dmcomp$insampsum, plotdat$matchset)
  
  # # tbc: see if make_DM_event_returns should called just once 
  # print("Making accounting event time returns")
  # print("Can take a few minutes...")
  # start_time <- Sys.time()
  # temp$event_time <- make_DM_event_returns(
  #   DMname = dmcomp$name, match_strats = temp$matched, npubmax = plotdat$npubmax, 
  #   czsum = czsum, use_sign_info = plotdat$use_sign_info
  # )
  # stop_time <- Sys.time()
  # print(stop_time - start_time)
  
  # plotdat$comp_matched <- temp$matched
  # plotdat$comp_event_time <- temp$event_time
  
  # join and reformat for plotting function
  # it wants columns:  (eventDate, ret, matchRet, matchRetAlt)
  # dm_mean returns are already scaled (see above)
  if(is.null(theory_filter)){
    ret_for_plotting <- czret %>%
      transmute(pubname = signalname, eventDate, ret = ret_scaled) %>%
      left_join(
        plotdat$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
      ) %>%
      select(eventDate, ret, matchRet, pubname) %>%
      # keep only rows where DM matchrets are observed
      filter(!is.na(matchRet) )
  }else{
    if(theory_filter == 'all'){
      ret_for_plotting <- czret %>%
        transmute(pubname = signalname, eventDate, ret = ret_scaled) %>%
        left_join(
          plotdat$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
        ) %>%
        select(eventDate, ret, matchRet, pubname) %>%
        # keep only rows where DM matchrets are observed
        filter(!is.na(matchRet) )
    }else{    
      ret_for_plotting <- czret %>%
        filter(theory == theory_filter) %>%
        transmute(pubname = signalname, eventDate, ret = ret_scaled) %>%
        left_join(
          plotdat$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
        ) %>%
        select(eventDate, ret, matchRet, pubname) %>%
        # keep only rows where DM matchrets are observed
        filter(!is.na(matchRet) )}
    
  }
  
  return(ret_for_plotting)
  
} # end make_ret_for_plotting

# Shared Settings --------------------------------------------------
plotdat0 <- list()

plotdat0$name <- "t_min_2"
plotdat0$npubmax = Inf
plotdat0$use_sign_info = TRUE

plotdat0$matchset <- list(
  # tolerance in levels
  t_tol = globalSettings$t_tol,
  r_tol = globalSettings$r_tol,
  # tolerance relative to op stat
  t_reltol = globalSettings$t_reltol,
  r_reltol = globalSettings$r_reltol,
  # alternative filtering
  t_min = globalSettings$t_min,
  t_max = globalSettings$t_max, 
  t_rankpct_min = globalSettings$t_rankpct_min,
  minNumStocks = globalSettings$minNumStocks
)

# aesthetic settings
fontsizeall = 28
legposall = c(30,15)/100
ylaball = 'Trailing 5-Year Return (bps pm)'
linesizeall = 1.5

# Shared data prep --------------------------------------------------

# make event time returns for Compustat DM
temp = list()
temp$matched <- SelectDMStrats(dmcomp$insampsum, plotdat0$matchset)

print("Making accounting event time returns")
print("Can take a few minutes...")
start_time <- Sys.time()
temp$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = temp$matched, npubmax = plotdat0$npubmax, 
  czsum = czsum, use_sign_info = plotdat0$use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

plotdat0$comp_matched <- temp$matched
plotdat0$comp_event_time <- temp$event_time
rm(temp)

ret_for_plot0 = czret %>%
  transmute(pubname = signalname, eventDate, ret = ret_scaled, theory) %>%
      left_join(
        plotdat0$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean),
        by = c("pubname", "eventDate")
      ) %>%
      select(eventDate, ret, matchRet, pubname, theory) 

# Intro Plot --------------------------------------------------

## plot --------------------------------------------------

tempsuffix = "t_min_2"

printme = ReturnPlotsWithDM(
  dt = ret_for_plot0 %>% filter(!is.na(matchRet)),
  basepath = "../Results/temp_",
  suffix = tempsuffix,
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  legendlabels =
    c(
      paste0("Published (and Peer Reviewed)"),
      paste0("Data-Mined for |t|>2.0 in Original Sample"),
      'N/A'
    ),
  legendpos = c(35,20)/100,
  fontsize = fontsizeall,
  yaxislab = ylaball,
  linesize = linesizeall
)

# custom edits 
(
  printme + theme(
  legend.background = element_rect(fill = "white", color = "black"
    , size = 0.3)
  # remove space where legend would be
  , legend.margin = margin(-1.0, 0.5, 0.5, 0.5, "cm")
  , legend.position  = c(44,15)/100
  # add space between legend items
  , legend.spacing.y = unit(0.2, "cm")
  ) +
  guides(color = guide_legend(byrow = TRUE))
) %>% 
  ggsave(filename = paste0("../Results/Fig_DM_", tempsuffix, '.pdf'), width = 10, height = 8)

file.remove("../Results/temp__t_min_2.pdf")

# numbers for intro
ret_for_plot0[eventDate>0 & eventDate <= Inf, .(mean(ret), mean(matchRet))]

ret_for_plot0 %>% distinct(pubname)
czret %>% distinct(signalname)

## Plot for slide animation --------------------------------------------------

tempname = "t_min_2_0"

printme = ReturnPlotsWithDM(
  dt = ret_for_plot0 %>% filter(!is.na(matchRet)) %>% select(-matchRet),
  basepath = "../Results/temp_Fig_DM",
  suffix = tempname,
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  legendlabels =
    c(
      paste0("Published (and Peer Reviewed)"),
      paste0("Data-Mined for |t|>2.0 in Original Sample"),
      'N/A'
    ),
  legendpos = c(35,20)/100,
  fontsize = fontsizeall,
  yaxislab = ylaball,
  linesize = linesizeall
)

# custom edits 
(
  printme + theme(
  legend.background = element_rect(fill = "white", color = "black"
    , size = 0.3)
  # remove space where legend would be
  , legend.margin = margin(-1.0, 0.5, 0.5, 0.5, "cm")
  , legend.position  = c(44,15)/100
  # add space between legend items
  , legend.spacing.y = unit(0.2, "cm")
  ) +
  guides(color = guide_legend(byrow = TRUE))
) %>% 
  ggsave(filename = paste0("../Results/Fig_DM_", tempname, '.pdf'), width = 10, height = 8)

file.remove("../Results/temp_Fig_DM_t_min_2_0.pdf")


# Plot by Theory Category -------------------------------------

# loop over theory categories
for (jj in unique(czret$theory)) {
  print(jj)
  tempname <- paste0("t_min_2_cat_", jj)

  # ret_for_plotting = make_ret_for_plotting(plotdat, theory_filter = jj)
  tempret = ret_for_plot0 %>% filter(theory == jj) %>% 
    filter(!is.na(matchRet))
  
  # Plot
  plt = ReturnPlotsWithDM(
    dt = tempret,  
    basepath = "../Results/Fig_DM",
    suffix = tempname,
    rollmonths = 60,
    colors = colors,
    labelmatch = FALSE,
    yl =-100, yh = 175,
    fig.width = 18,
    fontsize = 38,
    legendlabels =
      c(
        paste0("Published"),
        paste0("|t|>2.0 Mining Accounting"),
        'N/A'
      ),
    yaxislab = 'Trailing 5-Year Return',
    legendpos = c(25,20)/100,
  )  
  
}

## Plot with legend label for slides ------------------------------------

# Plot
plt = ReturnPlotsWithDM(
  dt = ret_for_plot0 %>% filter(theory == "risk") %>% filter(!is.na(matchRet)),
  basepath = "../Results/Fig_DM",
  suffix = "t_min_2_cat_risk_slides",
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl =-100, yh = 175,
  fig.width = 18,
  fontsize = 38,
  legendlabels =
    c(
      paste0("Published with Risk Explanation"),
      paste0("|t|>2.0 Mining Accounting"),
      'N/A'
    ),
  yaxislab = 'Trailing 5-Year Return',
  legendpos = c(25,20)/100,
  filetype = '.pdf'
)  



# Trailing N-year return plots --------------------------------------
## Prep top 5% ret data ====
# the rest of this file uses only t > 2 filter

tempplotdat = plotdat0
tempplotdat$matchset$t_min = 0 # turn off t_min filter
tempplotdat$matchset$t_rankpct_min = 5 # add top 5% t filter

# make event time returns for Compustat DM
temp = list()
temp$matched <- SelectDMStrats(dmcomp$insampsum, tempplotdat$matchset)

print("Making accounting event time returns")
print("Can take a few minutes...")
start_time <- Sys.time()
temp$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = temp$matched, npubmax = tempplotdat$npubmax, 
  czsum = czsum, use_sign_info = tempplotdat$use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

tempplotdat$comp_matched <- temp$matched
tempplotdat$comp_event_time <- temp$event_time
rm(temp)

ret_for_plot1 = ret_for_plot0 %>%
      left_join(
        tempplotdat$comp_event_time %>% transmute(pubname, eventDate, matchRetAlt = dm_mean),
        by = c("pubname", "eventDate")
      ) %>% 
      select(eventDate, ret, matchRet, matchRetAlt, pubname, theory)

## Plot ====


# loop over n_year_roll
n_year_list = c(1, 3, 5, 10)

for (n_year_roll in n_year_list) {
  vlabel = if (n_year_roll == 1){
    "1 year post-samp"
  }else{
    paste0(n_year_roll, " years post-samp")
  }
    (ReturnPlotsWithDM(
      dt = ret_for_plot1 %>% filter(!is.na(matchRet)),
      basepath = "../Results/temp_Fig_DM",
      suffix = "temp",
      rollmonths = n_year_roll * 12,
      colors = colors,
      labelmatch = FALSE,
      xl = -360, xh = 240,
      yl = -0, yh = 150,
      legendlabels =
        c(
          paste0("Published"),
          paste0("|t|>2.0 Mining Accounting"),
          paste0("Top 5% |t| Mining Accounting")
        ),
      legendpos = c(30,20)/100,
      fontsize = fontsizeall,
      yaxislab = paste0("Trailing ", n_year_roll, "-Year Return (bps pm)"),
      linesize = linesizeall
    ) +
      geom_vline(xintercept = n_year_roll * 12, linetype = "dashed",
                 color = "magenta") +
      annotate("text", x = n_year_roll * 12 + 6, y = 140, 
               label = vlabel,
               vjust = 1.5, hjust = 0, size = 6, color = "magenta")
    ) %>% 
      ggsave(filename = paste0("../Results/Fig_DM_Roll", n_year_roll, ".pdf"),
      width = 10, height = 8)
} # end loop over n_year_roll



# Accounting variables only plot  ----------------------------

# Compare to top 5% of t-stats (computed in previous chunk)

# Load info on accounting variables and filter
signaldoc =  data.table::fread('../Data/Raw/SignalDoc.csv') %>% 
  filter(Cat.Data == 'Accounting')


ret_for_plottingAnnualAccounting = ret_for_plot1 %>% 
  filter(pubname %in% unique(signaldoc$Acronym)) %>% 
  # Remove quarterly Compustat and a few others not constructed via annual CS
  filter(!(pubname %in% c("Cash", "ChTax", "EarningsSurprise", 
                          "NumEarnIncrease", "RevenueSurprise", "roaq",
                          'EarnSupBig',
                          'EarningsStreak',
                          'ShareIss1Y',
                          'ShareIss5Y'))) %>% 
  select(-matchRet) %>% 
  rename(matchRet = matchRetAlt) %>%
  filter(!is.na(matchRet))


printme = ReturnPlotsWithDM(
  dt = ret_for_plottingAnnualAccounting,
  basepath = "../Results/temp_Fig_DM",
  suffix = "t_top5Pct_AccountingOnly",
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  legendlabels =
    c(
      paste0("Published (and Peer Reviewed)"),
      paste0("Data-Mined for Top 5% |t| in Original Sample"),
      'N/A'
    ),
  legendpos = c(35,20)/100,
  fontsize = fontsizeall,
  yaxislab = ylaball,
  linesize = linesizeall
)

# custom edits 
printme2 = printme + theme(
  legend.background = element_rect(fill = "white", color = "black"
                                   , size = 0.3)
  # remove space where legend would be
  , legend.margin = margin(-0.7, 0.5, 0.5, 0.5, "cm")
  , legend.position  = c(44,15)/100
  # add space between legend items
  , legend.spacing.y = unit(0.2, "cm")
) +
  guides(color = guide_legend(byrow = TRUE))

# save
ggsave(paste0("../Results/Fig_DM_t_top5Pct_AccountingOnly.pdf"), width = 10, height = 8)

ret_for_plottingAnnualAccounting[eventDate>0 & eventDate <= Inf & pubname %in% unique(signaldoc$Acronym), .(mean(ret), mean(matchRet))]

ret_for_plottingAnnualAccounting %>% filter(pubname %in% unique(signaldoc$Acronym)) %>% 
  distinct(pubname)


