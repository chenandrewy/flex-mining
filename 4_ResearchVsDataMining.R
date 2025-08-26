# Run Scripts 

source('4a_DataMiningSummary.R', echo = T)  # this can take several minutes
source('4b_DMCorrelationsPCASummary.R', echo = T)  # This might take around an hour or so
source('4c1_ResearchVsDMprep.R', echo = T)
source('4c2_ResearchVsDMPlots.R', echo = T)
source('4c3_ResearchVsAcctVsTicker.R', echo = T)
source('4c4_RunBothFilters.R', echo = T) # Risk-adjusted analysis with both filter types
source('4d_ResearchVsDMRobustnessCorrelationsEtc.R', echo = T)
source('4d2_InspectTables.R', echo = T)
source('4e_DM_Span_PCA.R', echo = T)
