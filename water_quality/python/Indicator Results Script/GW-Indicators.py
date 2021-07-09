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
import os
from Functions import hilltop_data,stacked_data,sample_freq,round_half_up,annual_max,grades,grade_check,reduce_to_monthly,multiyear_percentile,trend_format,trends

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
# Save sample values in case result is censored value with detection limit
# in grade B or C range. Depending on the lower ranked detections, the result
# could be indetermined grade (i.e., A/B, A/B/C, or B/C)
sample_df = StatsData_df[StatsData_df['Measurement']==measurement][['Site','HydroYear','Censor','Numeric']].copy()
# Use annual_max function to generate annual max dataframe
indicator_df = annual_max(StatsData_df[StatsData_df['Measurement']==measurement].copy())
# Define waterbody, indicator, and special considerations
indicator_df['FreshwaterBodyType'] = 'Groundwater'
indicator_df['Indicator'] = 'Annual Maximum'
indicator_df['SpecialConsiderations'] = None
# Set sample frequency as 'All' to indicate all samples are used
indicator_df['Frequency'] = 'All'
# Set bins for grades
bins = [0,1,5.65,11.3,np.inf]
# Use grades function to set grades and grade range
indicator_df = grades(indicator_df,bins)
# Use  grade_check to adjust grade results for detected values
indicator_df = grade_check(indicator_df,sample_df,bins,'All')
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
indicator_df = indicator_df.sort_values(by=['Site','HydroYear'],ascending=True)
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
# Set bins for grades
bins = [0,5,25,50,100]
# Use grades function to set grades and grade range
indicator_df = grades(indicator_df,bins)
# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(indicator_df)

##############################################################################
'''
Nitrate Nitrogen 5-yr median
'''

# Set measurement parameter
measurement = 'Nitrate Nitrogen'
# Use reduce_to_monthly function to generate monthly values dataframe
indicator_df = reduce_to_monthly(StatsData_df[(StatsData_df['Measurement'] == measurement)].copy())
# Use multi_year function to generate 5-yr medians
indicator_df = multiyear_percentile(indicator_df,50,5,['Monthly','Quarterly','Semi-annual','Annual'],[48,16,8,4])
# Define waterbody, indicator, and special considerations
indicator_df['FreshwaterBodyType'] = 'Groundwater'
indicator_df['Indicator'] = '5-yr Median'
indicator_df['SpecialConsiderations'] = None
# Appropriately round results to match measurement precision in that range
# 5.65 should not be rounded since it is a grade cutoff
indicator_df['Numeric'] = indicator_df['Numeric'].apply(lambda x : round_half_up(x,3))
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>=0.2,indicator_df['Numeric'].apply(lambda x : round_half_up(x,2)),indicator_df['Numeric'])
indicator_df['Numeric'] = np.where((indicator_df['Numeric']>=2)&(indicator_df['Numeric']!=5.65),indicator_df['Numeric'].apply(lambda x : round_half_up(x,1)),indicator_df['Numeric'])
# Convert result to a string
indicator_df['Result'] = indicator_df['Censor'].fillna('')+indicator_df['Numeric'].astype(str)
# Set bins for grades
bins = [0,1,5.65,11.3,np.inf]
# Use grades function to set grades and grade range
indicator_df = grades(indicator_df,bins)
# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(indicator_df)

##############################################################################
'''
Initiate Indicator Dataframe
'''

TrendData_df = pd.DataFrame(columns=['Site','Measurement','Units','HydroYear','Frequency','Interval','Censor','Numeric','Result'])
TrendResults_df = pd.DataFrame(columns=['Site','Measurement','Units','HydroYear','TrendLength','DataFrequency','Intervals','MaxDetectionLimit','MinQuantLimit','Seasonal_pvalue','Seasonality','MK_pvalue','MK_Zscore','MK_Tau','MK_S','MK_VarS','DecreasingLikelihood','TrendCategory','TrendLineStartDate','TrendLineStartValue','Slope','TrendLineEndDate','TrendLineEndValue'])

##############################################################################
'''
Nitrate Nitrogen Trends
'''

# Set measurement parameter
measurement = 'Nitrate Nitrogen'
# Use reduce_to_monthly function to generate monthly values dataframe
indicator_df = reduce_to_monthly(StatsData_df[(StatsData_df['Measurement'] == measurement)].copy())
# Use trend_format function to generate data format for trend analyses
# using specified data frequency options
trend_data_df = trend_format(indicator_df,['Annual','Quarterly','Monthly'])
# Save units
units = indicator_df['Units'][0]
# Run trends() function for selected trend periods(5 to 30), hydroyears (2021), and data requirement (80%)
trend_results_df = trends(trend_data_df,[i for i in range(5,31)],[2021],0.80)
# Add measurement and units column in trend results table
trend_results_df['Measurement'] = measurement
trend_results_df['Units'] = units
# Append the trend data and trend results to respective tables
TrendData_df = TrendData_df.append(trend_data_df)      
TrendResults_df = TrendResults_df.append(trend_results_df)

##############################################################################
'''
Export the Results
'''

# Export results to Excel
with pd.ExcelWriter('GW-Results.xlsx') as writer:
    WQData_df.to_excel(writer, sheet_name='HilltopData',index=True)
    StatsData_df.to_excel(writer, sheet_name='CleanedData',index=False)
    Frequency_df.reset_index().to_excel(writer, sheet_name='SampleFrequency',index=False)
    Unstacked_df.reset_index().to_excel(writer, sheet_name='UnstackedFrequency',index=False)
    IndicatorResults_df.to_excel(writer, sheet_name='IndicatorResults',index=False)
    TrendData_df.to_excel(writer, sheet_name='TrendData',index=False)
    TrendResults_df.to_excel(writer, sheet_name='TrendResults',index=False)

##############################################################################
'''
Combine GW and SW indicator results
'''

# Check if SW results exist otherwise don't merge
if os.path.isfile('SW-Results.xlsx'):
    with pd.ExcelWriter('Results.xlsx') as writer:
        # Only merge primary results in output
        for sheet in ['IndicatorResults','TrendData','TrendResults']:
            try:
                GW_df = pd.read_excel('GW-Results.xlsx',sheet_name=sheet)
                SW_df = pd.read_excel('SW-Results.xlsx',sheet_name=sheet)
                df = pd.concat([GW_df,SW_df])
                df.to_excel(writer,sheet_name=sheet,index=False)
            except NameError:
                continue
            except ValueError:
                continue