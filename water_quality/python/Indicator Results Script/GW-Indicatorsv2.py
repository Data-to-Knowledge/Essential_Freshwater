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
import csv
from Functions import hilltop_data,stacked_data,sample_freq,sort_censors,Hazen_percentile,round_half_up

##############################################################################
'''
Set measurements of interest
'''

measurements = ['Nitrate Nitrogen','E. coli']

##############################################################################
'''
Import list of SoE sample project codes
'''
# Import set of SoE monitoring project codes
with open('GW-SoEProjectCodes.csv', 'r', encoding='utf-8-sig') as f:
    project_codes = [row[0] for row in csv.reader(f)]
    # Remove potential leading and tailing spaces
f.close()

# Import site list for Zella Smith missing project codes
with open('GW-ZellaSmithSoESites.csv', 'r', encoding='utf-8-sig') as f:
    ZS_sites = [row[0] for row in csv.reader(f)]
    # Remove potential leading and tailing spaces
f.close()

##############################################################################
'''
Choose Hilltop file
'''

# Set URL
base_url = 'http://wateruse.ecan.govt.nz'

# Set server hts file (Server hts name, not Hilltop01 hts name!)
# WQGroundwater.hts = \Hilltop01\Data\WQGroundwaterCombined.dsn
hts = 'WQGroundwater.hts'

##############################################################################
'''
Set site list as sites within Hilltop file
'''

# Generate a list of all sites in the server file
sites = sorted(ws.site_list(base_url,hts).SiteName.tolist(),key=str.lower)
# Only include sites that contain '/' in the site name
sites = [site for site in sites if '/' in site]


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

# Generate data that has been filter by project code
SoEData_df = WQData_df[(WQData_df['Sample Parameters','Project'].isin(project_codes))|((WQData_df.index.get_level_values('Site').isin(ZS_sites))&(WQData_df['Sample Parameters','Project'].isna())&(WQData_df['Sample Parameters','Field Technician']=='Zella Smith')&(WQData_df.index.get_level_values('DateTime').month.isin([9,10]))&(WQData_df.index.get_level_values('DateTime').year.isin([1999,2000,2001,2002])))]

# Take relevant data and append to StatsData_df
StatsData_df = stacked_data(SoEData_df,measurements,units_dict)
# Remove values of 0 and <0
StatsData_df = StatsData_df[StatsData_df['Observation'] != '0']
StatsData_df = StatsData_df[StatsData_df['Observation'] != '<0']

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

IndicatorResults_df = pd.DataFrame(columns=['FreshwaterBodyType','Measurement','Units','Indicator','Site','HydroYear','Result','Censor','Numeric','GradeRange','Grade','SamplesOrIntervals','Frequency','SpecialConsiderations'])
IndicatorResults_df['HydroYear'] = IndicatorResults_df['HydroYear'].astype(int)
IndicatorResults_df['SamplesOrIntervals'] = IndicatorResults_df['SamplesOrIntervals'].astype(int)

##############################################################################
'''
Nitrate Nitrogen Annual Maximum indicator
'''

# Set measurement parameter
measurement = 'Nitrate Nitrogen'
# Sort values from largest to smallest using censor and numeric components
indicator_df = sort_censors(StatsData_df[StatsData_df['Measurement']==measurement].copy(),'Censor','Numeric',ascending=False)
# Define waterbody, indicator, and special considerations
indicator_df['FreshwaterBodyType'] = 'Groundwater'
indicator_df['Indicator'] = 'Annual Maximum'
indicator_df['SpecialConsiderations'] = None
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
bins = [0,1,5.65,11.3,np.inf]
indicator_df['GradeRange'] = pd.cut(indicator_df['Numeric'],bins,labels=['0-1','>1-5.65','>5.65-11.3','>11.3'])
indicator_df['Grade'] = pd.cut(indicator_df['Numeric'],bins,labels=['A','B','C','D'])
# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(indicator_df)

##############################################################################
'''
E. coli 5-yr percent exceedances above drinking water standard for Groundwater
'''

# Set measurement parameter
measurement = 'E. coli'
# Start by only considering the E. coli values
indicator_df = StatsData_df[(StatsData_df['Measurement'] == 'E. coli')].copy()
# Drop any detection limits that don't work with drinking water standard
# (i.e., drop value of <2 from K38/0408) Detection limit larger than 1
indicator_df = indicator_df[~((indicator_df['Censor']=='<')&(indicator_df['Numeric']>1))]

# Duplicate samples largely have the same result and should not both be counted.
# However, when one sample is <1 and another is >=1, then count them both towards
# the percentage calculation

# Assign each sample an index for day of the year
indicator_df['Day'] = indicator_df['DateTime'].dt.month*31 + indicator_df['DateTime'].dt.day
# For each sample, indicate 0 if below detection (<1) and 1 if detected at 1+
indicator_df['Detection'] = np.where(indicator_df['Censor']=='<',0,1)

# Only allow a single detection and single non-detection count towards each day
indicator_df = indicator_df.drop_duplicates(subset=['Site','HydroYear','Day','Detection'],keep='first')
# Add columns counting the number of samples and detections each hydro year
indicator_df['AnnualSamples'] = indicator_df.groupby(['Site','HydroYear'])['Observation'].transform('count')
indicator_df['AnnualDetections'] = indicator_df.groupby(['Site','HydroYear'])['Detection'].transform('sum')
# Remove unneeded columns and drop duplicates
indicator_df = indicator_df.drop(columns=['DateTime','Observation','Censor','Numeric','Day','Detection']).drop_duplicates()
# Years without data can still have a result if the preceeding 4 years have
# results. For this reason, we fill the missing years at a site
indicator_df.HydroYear = pd.Categorical(indicator_df.HydroYear)
indicator_df = indicator_df.groupby(['Site','HydroYear']).sum()
# In years where no samples are collected, replace the 0 count with nan
indicator_df['AnnualSamples'] = np.where(indicator_df['AnnualSamples']==0,np.nan,indicator_df['AnnualSamples'])
# For each year, count the past 5 years of samples and exceedances
# Four values are required to generate results
indicator_df[['Samples5yr','Detections5yr']] = indicator_df.groupby(['Site']).rolling(window=5,min_periods=4).sum()[['AnnualSamples','AnnualDetections']].reset_index().rename(columns={'AnnualSamples':'Samples5yr','AnnualDetections':'Detections5yr'})[['Samples5yr','Detections5yr']].values
# Calculate the percentage by dividing the exceedances by the samples and multiply by 100.
indicator_df['Result'] = (indicator_df['Detections5yr']/indicator_df['Samples5yr']*100).apply(lambda x : round_half_up(x,2))
# Remove unneeded columns, reset index, and set hydro year data type to int
indicator_df = indicator_df.drop(columns=['AnnualSamples','AnnualDetections','Detections5yr']).rename(columns={'Samples5yr':'SamplesOrIntervals'})
indicator_df = indicator_df.dropna().reset_index()
indicator_df['HydroYear'] = indicator_df['HydroYear'].astype(int)
# Add columns to complete information for appending to full indicator results
indicator_df['FreshwaterBodyType'] = 'Groundwater'
indicator_df['Indicator'] = '5-yr Exceedance Percentage'
indicator_df['SpecialConsiderations'] = None
indicator_df['Numeric'] = indicator_df['Result']
indicator_df['Result'] = indicator_df['Result'].astype(str)
indicator_df['Measurement'] = measurement
indicator_df['Units'] = units_dict[measurement]
indicator_df['Frequency'] = 'Daily'
indicator_df['Censor'] = None
# Set bins for the indicator grades and add grade column
bins = [-0.01,5,25,50,100]
indicator_df['GradeRange'] = pd.cut(indicator_df['Numeric'],bins,labels=['0-5','>5-25','>25-50','>50'])
indicator_df['Grade'] = pd.cut(indicator_df['Numeric'],bins,labels=['A','B','C','D'])
# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(indicator_df)

##############################################################################
'''
Nitrate Nitrogen 5-yr median
'''
measurement = 'Nitrate Nitrogen'
# Start by only considering the nitrate nitrogen values
indicator_df = StatsData_df[(StatsData_df['Measurement'] == measurement)].copy()

# Duplicate samples are taken periodically and should not both be counted in
# the 5-yr median, reduce multiple samples collected in a day to a single value.
# Additionally, no sites are regularly sampled more than monthly. Reduce multiple
# samples collected within a month to a single value.

indicator_df['Semester'] = np.where(indicator_df['HydroYear']==indicator_df['DateTime'].dt.year,2,1)
indicator_df['Quarter'] = indicator_df['DateTime'].dt.quarter
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

# Obtain quarterly values by taking median of monthly values collected within a quarter
indicator_df = Hazen_percentile(indicator_df,50,['Site','HydroYear','Quarter'],'MonthCensor','MonthNumeric','QuarterCensor','QuarterNumeric')

# Obtain semi-annual values by taking median of monthly values collected within a half year
indicator_df = Hazen_percentile(indicator_df,50,['Site','HydroYear','Semester'],'MonthCensor','MonthNumeric','SemesterCensor','SemesterNumeric')

# Obtain annual values by taking median of monthly values collected within a year
indicator_df = Hazen_percentile(indicator_df,50,['Site','HydroYear'],'MonthCensor','MonthNumeric','AnnualCensor','AnnualNumeric')

# Save these results for use in calculations for each 5-year interval
tempdata = indicator_df

# Cycle through each hydroyear and collate the appropriate 5-yr data using the
# appropriate data frequency
indicator_df = pd.DataFrame()
for year in range(tempdata['HydroYear'].min(),tempdata['HydroYear'].max()+1):
    print(year)
    years = [year-i for i in range(0,5)]
    hydroyearstats = tempdata[tempdata['HydroYear'].isin(years)]
    # Determine number of months with results in 5-yr period and monthly median
    hydroyearstats = pd.merge(hydroyearstats,hydroyearstats.groupby('Site').size().rename('Months5yr'),on=['Site'],how='outer')
    hydroyearstats = Hazen_percentile(hydroyearstats,50,['Site'],'MonthCensor','MonthNumeric','Months5yrCensor','Months5yrNumeric')
    hydroyearstats = hydroyearstats.drop(columns=['Month','MonthCensor','MonthNumeric']).drop_duplicates()
    # Determine number of quarters with results in 5-yr period and quarterly median
    hydroyearstats = pd.merge(hydroyearstats,hydroyearstats.groupby('Site').size().rename('Quarters5yr'),on=['Site'],how='outer')
    hydroyearstats = Hazen_percentile(hydroyearstats,50,['Site'],'QuarterCensor','QuarterNumeric','Quarters5yrCensor','Quarters5yrNumeric')
    hydroyearstats = hydroyearstats.drop(columns=['Quarter','QuarterCensor','QuarterNumeric']).drop_duplicates()
    # Determine number of half-years with results in 5-yr period and semiannual median
    hydroyearstats = pd.merge(hydroyearstats,hydroyearstats.groupby('Site').size().rename('Semesters5yr'),on=['Site'],how='outer')
    hydroyearstats = Hazen_percentile(hydroyearstats,50,['Site'],'SemesterCensor','SemesterNumeric','Semesters5yrCensor','Semesters5yrNumeric')
    hydroyearstats = hydroyearstats.drop(columns=['Semester','SemesterCensor','SemesterNumeric']).drop_duplicates()
    # Determine number of years with results in 5-yr period and annual median
    hydroyearstats = pd.merge(hydroyearstats,hydroyearstats.groupby('Site').size().rename('Years5yr'),on=['Site'],how='outer')
    hydroyearstats = Hazen_percentile(hydroyearstats,50,['Site'],'AnnualCensor','AnnualNumeric','Years5yrCensor','Years5yrNumeric')
    hydroyearstats = hydroyearstats.drop(columns=['HydroYear','AnnualCensor','AnnualNumeric']).drop_duplicates()
    # Establish appropriate frequency for 5-yr median
    hydroyearstats['HydroYear'] = year
    hydroyearstats['Frequency'] = None
    hydroyearstats['Frequency'].mask(hydroyearstats['Years5yr'] >= 4, 'Annual', inplace=True)
    hydroyearstats['Frequency'].mask(hydroyearstats['Semesters5yr'] >= 8, 'Semi-annual', inplace=True)
    hydroyearstats['Frequency'].mask(hydroyearstats['Quarters5yr'] >= 16, 'Quarterly', inplace=True)
    hydroyearstats['Frequency'].mask(hydroyearstats['Months5yr'] >= 48, 'Monthly', inplace=True)
    # Set the 5-yr median censor component for set frequency
    hydroyearstats['Censor'] = None
    hydroyearstats['Censor'].mask(hydroyearstats['Years5yr'] >= 4, hydroyearstats['Years5yrCensor'], inplace=True)
    hydroyearstats['Censor'].mask(hydroyearstats['Semesters5yr'] >= 8, hydroyearstats['Semesters5yrCensor'], inplace=True)
    hydroyearstats['Censor'].mask(hydroyearstats['Quarters5yr'] >= 16, hydroyearstats['Quarters5yrCensor'], inplace=True)
    hydroyearstats['Censor'].mask(hydroyearstats['Months5yr'] >= 48, hydroyearstats['Months5yrCensor'], inplace=True)
    # Set the 5-yr median numeric component for set frequency
    hydroyearstats['Numeric'] = np.nan
    hydroyearstats['Numeric'].mask(hydroyearstats['Years5yr'] >= 4, hydroyearstats['Years5yrNumeric'], inplace=True)
    hydroyearstats['Numeric'].mask(hydroyearstats['Semesters5yr'] >= 8, hydroyearstats['Semesters5yrNumeric'], inplace=True)
    hydroyearstats['Numeric'].mask(hydroyearstats['Quarters5yr'] >= 16, hydroyearstats['Quarters5yrNumeric'], inplace=True)
    hydroyearstats['Numeric'].mask(hydroyearstats['Months5yr'] >= 48, hydroyearstats['Months5yrNumeric'], inplace=True)
    # Set the 5-yr median interval count for set frequency
    hydroyearstats['SamplesOrIntervals'] = np.nan
    hydroyearstats['SamplesOrIntervals'].mask(hydroyearstats['Years5yr'] >= 4, hydroyearstats['Years5yr'], inplace=True)
    hydroyearstats['SamplesOrIntervals'].mask(hydroyearstats['Semesters5yr'] >= 8, hydroyearstats['Semesters5yr'], inplace=True)
    hydroyearstats['SamplesOrIntervals'].mask(hydroyearstats['Quarters5yr'] >= 16, hydroyearstats['Quarters5yr'], inplace=True)
    hydroyearstats['SamplesOrIntervals'].mask(hydroyearstats['Months5yr'] >= 48, hydroyearstats['Months5yr'], inplace=True)
    # Reorder columns and drop columns that do not have a set frequency
    hydroyearstats = hydroyearstats[['Site','Measurement','Units','HydroYear','Frequency','Censor','Numeric','SamplesOrIntervals']]
    hydroyearstats = hydroyearstats.dropna(subset=['Frequency'])
    
    indicator_df = indicator_df.append(hydroyearstats)

# Add columns to complete information for appending to full indicator results
indicator_df['FreshwaterBodyType'] = 'Groundwater'
indicator_df['Indicator'] = '5-yr Median'
indicator_df['SpecialConsiderations'] = None
# Appropriately round results to match measurement precision in that range
# 5.65 should not be rounded since it is a grade cutoff
indicator_df['Numeric'] = indicator_df['Numeric'].apply(lambda x : round_half_up(x,3))
indicator_df['Numeric'].mask(indicator_df['Numeric']>=0.2,indicator_df['Numeric'].apply(lambda x : round_half_up(x,2)),inplace=True)
indicator_df['Numeric'].mask((indicator_df['Numeric']>=2)&(indicator_df['Numeric']!=5.65),indicator_df['Numeric'].apply(lambda x : round_half_up(x,1)),inplace=True)

# Set bins for the indicator grades and add grade column
bins = [0,1,5.65,11.3,np.inf]
indicator_df['GradeRange'] = pd.cut(indicator_df['Numeric'],bins,labels=['0-1','>1-5.65','>5.65-11.3','>11.3'])
indicator_df['Grade'] = pd.cut(indicator_df['Numeric'],bins,labels=['A','B','C','D'])

# Convert result to a string
indicator_df['Result'] = indicator_df['Censor'].fillna('')+indicator_df['Numeric'].astype(str)

# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(indicator_df)

##############################################################################
'''
Export the Results
'''

# Export results to Excel
with pd.ExcelWriter('GW-IndicatorResults.xlsx') as writer:  
    WQData_df.to_excel(writer, sheet_name='HilltopData',index=True)
    StatsData_df.to_excel(writer, sheet_name='CleanedData',index=False)
    Frequency_df.reset_index().to_excel(writer, sheet_name='SampleFrequency',index=False)
    Unstacked_df.reset_index().to_excel(writer, sheet_name='UnstackedFrequency',index=False)
    IndicatorResults_df.to_excel(writer, sheet_name='IndicatorResults',index=False)

##############################################################################
'''
Combine GW and SW indicator results
'''
GW_df = pd.read_excel('GW-IndicatorResults.xlsx',sheet_name='IndicatorResults')
SW_df = pd.read_excel('SW-IndicatorResults.xlsx',sheet_name='IndicatorResults')

df = pd.concat([GW_df,SW_df])
df.to_excel('IndicatorResults.xlsx')


