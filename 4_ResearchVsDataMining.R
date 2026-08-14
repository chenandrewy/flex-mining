# Chapter 4 driver: Research vs Data Mining.
#
# How to run: sourced by MAIN.R (working directory = flex-mining/). Can also be
#   run on its own once chapters 2 (../Data/Processed) and 3 exist.
# Inputs:  ../Data/Processed/*.RDS built by chapter 2 and by 4c1 below.
# Outputs: the chapter-4 exhibits under ../Results (see 260813c,map,exhibits.md).
#
# Every source() here is on a path to one of the paper's exhibits. Ordering:
# 4c1 builds the prep RDS (ret_for_plot0/1, plotdat0, dmcomp/dmtic_sumstats)
# that the later scripts read, and 4c20a builds fig2_panel_agg.RDS for 4c20b, so
# both prep steps precede their consumers.

source('4a_DataMiningSummary.R', echo = T)  # this can take several minutes
source('4b_DMCorrelationsPCASummary.R', echo = T)  # This might take around an hour or so
source('4c1_ResearchVsDMprep.R', echo = T)
source('4c2_ResearchVsDMPlots.R', echo = T)
source('4c3_ResearchVsAcctVsTicker.R', echo = T)

# Risk-adjusted exhibits used in the paper
source('4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R', echo = T)
source('4c5_FullSampleRiskAdjustedResearchVsDMPlots.R', echo = T)
source('4c6_AccountingOnlyPlots.R', echo = T)
source('4c7_AccountingOnlyAlphaPlots.R', echo = T)

# MP-style decay tables: Table_MPStyleRegsIndividualDM.tex, and the
# Table_MPStyleRegs{Main,Unscaled} tables the HandTable exhibits transcribe.
source('4c6_MPStyleDecayTables.R', echo = T)

source('4d_ResearchVsDMRobustnessCorrelationsEtc.R', echo = T)
source('4d2_InspectTables.R', echo = T)
source('4e_DM_Span_PCA.R', echo = T)

# Figure 2 (Fig2a-d and _CI variants). 4c20a writes fig2_panel_agg.RDS; 4c20b
# reads it and emits the plots.
source('4c20a_Fig2Data.R', echo = T)
source('4c20b_Fig2Plots.R', echo = T)
