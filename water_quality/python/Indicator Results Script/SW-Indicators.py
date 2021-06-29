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
from Functions import hilltop_data,stacked_data,sample_freq,sort_censors,Hazen_percentile

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


StatsData_df = StatsData_df[StatsData_df['Numeric'] > 0.0]
StatsData_df = StatsData_df[(StatsData_df['Measurement'] == 'Chlorophyll a (planktonic)')&(StatsData_df['Numeric'] < 0.01)]


##############################################################################
'''
For each hydro year, determine number of samples, days sampled, months sampled,
and quarters sampled. From this, estimate a sampling frequency.
'''

Frequency_df = sample_freq(StatsData_df,semiannual=False)
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
Chlorophyll-a Annual Median indicator
'''

# Set measurement parameter
measurement = 'Chlorophyll a (planktonic)'
# Start by only considering the chlorophyll-a values
indicator_df = StatsData_df[(StatsData_df['Measurement'] == measurement)].copy()

# Duplicate samples are taken periodically and should not both be counted in
# the median, reduce multiple samples collected in a day to a single value.
# Additionally, no sites are regularly sampled more than monthly. Reduce multiple
# samples collected within a month to a single value.

indicator_df['Month'] = indicator_df['DateTime'].dt.month
indicator_df['Day'] = indicator_df['DateTime'].dt.month*31 + indicator_df['DateTime'].dt.day

# Obtain daily values by taking median of samples collected in a day
indicator_df = Hazen_percentile(indicator_df,50,['Site','HydroYear','Day'],'Censor','Numeric','DayCensor','DayNumeric')
# Drop unnecessary columns and duplicates
indicator_df = indicator_df.drop(columns=['DateTime','Observation','Censor','Numeric']).drop_duplicates()

# Obtain monthly values by taking median of samples collected within a month
indicator_df = Hazen_percentile(indicator_df,50,['Site','HydroYear','Month'],'DayCensor','DayNumeric','MonthCensor','MonthNumeric')
# Drop unnecessary columns and duplicates
indicator_df = indicator_df.drop(columns=['Day','DayCensor','DayNumeric']).drop_duplicates()

# Obtain annual values by taking median of monthly values collected within a year
indicator_df = Hazen_percentile(indicator_df,50,['Site','HydroYear'],'MonthCensor','MonthNumeric','AnnualCensor','AnnualNumeric')

indicator_df = pd.merge(indicator_df,indicator_df.groupby(['Site','HydroYear']).size().rename('Months'),on=['Site','HydroYear'],how='outer')
indicator_df = indicator_df.drop(columns=['Month','MonthCensor','MonthNumeric']).drop_duplicates()
indicator_df = indicator_df.rename(columns={'Months':'SamplesOrIntervals','AnnualNumeric':'Numeric','AnnualCensor':'Censor'})
indicator_df['Frequency'] = 'Monthly'

# Set bins for the indicator grades and add grade column
bins = [0,2,5,12,np.inf]
indicator_df['GradeRange'] = pd.cut(indicator_df['Numeric'],bins,labels=['0-2','>2-5','>5-12','>12'])
indicator_df['Grade'] = pd.cut(indicator_df['Numeric'],bins,labels=['A','B','C','D'])


# Add columns to complete information for appending to full indicator results
indicator_df['Indicator'] = 'Chlorophyll-a Annual Median'
indicator_df['Numeric'] = round(indicator_df['Numeric'],3)
indicator_df['Numeric'].mask(indicator_df['Numeric']>=0.2,round(indicator_df['Numeric'],2),inplace=True)
indicator_df['Numeric'].mask(indicator_df['Numeric']>=2,round(indicator_df['Numeric'],1),inplace=True)

# Convert result to a string
indicator_df['Result'] = indicator_df['Censor'].fillna('')+indicator_df['Numeric'].astype(str)

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





