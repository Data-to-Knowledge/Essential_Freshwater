# -*- coding: utf-8 -*-
"""
Python Script to generate indicator results for GW indicators

Created on Fri Jun 6 09:08:13 2021

@author: KurtV
"""

# import python modules
from hilltoppy import web_service as ws
import pandas as pd
import numpy as np
from Functions import hilltop_data,stacked_data,sample_freq,sort_censors

##############################################################################
'''
Set measurements of interest
'''

measurements = ['Chlorophyll a (planktonic)','Chlorophyll a (benthic)','Chlorophyll a (Ethanol)',
                'Total Nitrogen','Ammoniacal Nitrogen','Nitrate-N Nitrite-N',
                'Total Phosphorus','E. coli']

##############################################################################
'''
Choose Hilltop file
'''

# Set URL
base_url = 'http://wateruse.ecan.govt.nz'

# Set server hts file (Server hts name, not Hilltop01 hts name!)
# WQGroundwater.hts = \Hilltop01\Data\WQGroundwaterCombined.dsn
hts = 'SWQlongTerm.hts'

##############################################################################
'''
Set site list as sites within Hilltop file
'''

# Generate a list of all sites in the server file
sites = sorted(ws.site_list(base_url,hts).SiteName.tolist(),key=str.lower)

##############################################################################
'''
Create WQ table with format to match view in Hilltop Manager on site basis
'''

WQData_df = hilltop_data(base_url,hts,sites,measurements)

##############################################################################
'''
Set Measurement Units Dictionary
'''
units_dict = {}
for measurement in measurements:
    mcolumns = WQData_df[[measurement]].columns.get_level_values(1).to_list()
    units_dict[measurement]= [i for i in mcolumns if i.startswith('(')][0][1:-1]

##############################################################################
'''
Stack measurement results and create Hydro Year, censor, and numeric values
'''

# Take relevant data and append to StatsData_df
StatsData_df = stacked_data(WQData_df,measurements,units_dict)

'''
Consider removing negative values, 0 values, and values that seem to have unit
conversion issues (see Chlorophyl-a results)
'''

##############################################################################
'''
For each hydro year, determine number of samples, days sampled, months sampled,
and quarters sampled. From this, estimate a sampling frequency.
'''

Frequency_df = sample_freq(StatsData_df,semiannual=True)
# Use estimated frequency to create a table with one column for each year
Unstacked_df = Frequency_df['Frequency'].unstack(level=2)

##############################################################################
'''
Initiate Indicator Dataframe
'''

IndicatorResults_df = pd.DataFrame(columns=['Site','Measurement','Units','Indicator','HydroYear','Result','Censor','Numeric','GradeRange','Grade','SamplesOrIntervals','Frequency'])
IndicatorResults_df['HydroYear'] = IndicatorResults_df['HydroYear'].astype(int)
IndicatorResults_df['SamplesOrIntervals'] = IndicatorResults_df['SamplesOrIntervals'].astype(int)

##############################################################################
'''
Chlorophyll-a Annual Maximum indicator
'''

# Set measurement parameter
measurement = 'Chlorophyll a (planktonic)'
# Sort values from largest to smallest using censor and numeric components
indicator_df = sort_censors(StatsData_df[StatsData_df['Measurement']==measurement].copy(),'Censor','Numeric',ascending=False)
# Name indicator
indicator_df['Indicator'] = 'Chlorophyll-a Annual Max'
# Count number of samples collected in Hydroyear
indicator_df = pd.merge(indicator_df,indicator_df.groupby(['Site','HydroYear']).size().rename('SamplesOrIntervals'),on=['Site','HydroYear'],how='outer')
# Set sample frequency as 'All' to indicate all samples are used
indicator_df['Frequency'] = 'All'
# Keep maximum value for each hydro year
indicator_df = indicator_df.drop_duplicates(subset=['Site','HydroYear'],keep='first')
# Rename Observation column to be Result column and drop DateTime
indicator_df = indicator_df.rename(columns={'Observation':'Result'}).drop(columns=['DateTime'])
# Sort by Site and hydroyear
indicator_df = indicator_df.sort_values(by=['Site','HydroYear'],ascending=True)
# Set bins for the indicator grades and grade range
bins = [0,10,25,60,np.inf]
indicator_df['GradeRange'] = pd.cut(indicator_df['Numeric'],bins,labels=['0-10','>10-25','>25-60','>60'])
indicator_df['Grade'] = pd.cut(indicator_df['Numeric'],bins,labels=['A','B','C','D'])
# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(indicator_df)

##############################################################################
'''
Export the Results
'''

# Export results to Excel
with pd.ExcelWriter('SW-IndicatorResults.xlsx') as writer:  
    WQData_df.to_excel(writer, sheet_name='HilltopData',index=True)
    StatsData_df.to_excel(writer, sheet_name='CleanedData',index=False)
    Frequency_df.reset_index().to_excel(writer, sheet_name='SampleFrequency',index=False)
    Unstacked_df.reset_index().to_excel(writer, sheet_name='UnstackedFrequency',index=False)
    IndicatorResults_df.to_excel(writer, sheet_name='IndicatorResults',index=False)





