# -*- coding: utf-8 -*-
"""
Python Script to generate indicator results for SW indicators

Created on Fri Jun 6 09:08:13 2021

@author: KurtV
"""

# import python modules
from hilltoppy import web_service as ws
import pandas as pd
import numpy as np
import os
from Functions import hilltop_data,stacked_data,sample_freq,round_half_up,annual_max,grades,reduce_to_monthly,annual_percentile,grade_check,multiyear_percentile

##############################################################################
'''
Set measurements of interest
'''

measurements = ['Chlorophyll a (planktonic)','Chlorophyll a (benthic)','Chlorophyll a (Ethanol)',
                'Total Nitrogen','Ammoniacal Nitrogen','Nitrate-N Nitrite-N',
                'Total Phosphorus','Dissolved Reactive Phosphorus','E. coli']

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
StatsData_df = StatsData_df[~((StatsData_df['Measurement'] == 'Chlorophyll a (planktonic)')&(StatsData_df['Numeric'] < 0.01))]


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

IndicatorResults_df = pd.DataFrame(columns=['FreshwaterBodyType','Measurement','Units','Indicator','Site','HydroYear','Result','Censor','Numeric','GradeRange','Grade','SamplesOrIntervals','Frequency','SpecialConsiderations'])
IndicatorResults_df['HydroYear'] = IndicatorResults_df['HydroYear'].astype(int)
IndicatorResults_df['SamplesOrIntervals'] = IndicatorResults_df['SamplesOrIntervals'].astype(int)

##############################################################################
'''
Chlorophyll-a Annual Maximum indicator
'''

# Set measurement parameter
measurement = 'Chlorophyll a (planktonic)'
# Save sample values in case result is censored value with detection limit
# in grade B or C range. Depending on the lower ranked detections, the result
# could be indetermined grade (i.e., A/B, A/B/C, or B/C)
sample_df = StatsData_df[StatsData_df['Measurement']==measurement][['Site','HydroYear','Censor','Numeric']].copy()
# Use annual_max function to generate annual max dataframe
indicator_df = annual_max(StatsData_df[StatsData_df['Measurement']==measurement].copy())
# Define waterbody, indicator, and special considerations
indicator_df['FreshwaterBodyType'] = 'Lakes'
indicator_df['Indicator'] = 'Annual Maximum'
indicator_df['SpecialConsiderations'] = 'Separate analysis for open/closed to sea periods'
# Set sample frequency as 'All' to indicate all samples are used
indicator_df['Frequency'] = 'All'
# Set bins for grades
bins = [0,10,25,60,np.inf]
# Use grades function to set grades and grade range
indicator_df = grades(indicator_df,bins)
# Use  grade_check to adjust grade results for detected values
indicator_df = grade_check(indicator_df,sample_df,bins,'All')
# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(indicator_df)

##############################################################################
'''
Chlorophyll-a Annual Median indicator
'''

# Set measurement parameter
measurement = 'Chlorophyll a (planktonic)'
# Use reduce_to_monthly function to generate monthly values dataframe
indicator_df = reduce_to_monthly(StatsData_df[(StatsData_df['Measurement'] == measurement)].copy())
# Save monthly values in case result is censored value with detection limit
# in grade B or C range. Depending on the lower ranked detections, the result
# could be indetermined grade (i.e., A/B, A/B/C, or B/C)
monthly_df = indicator_df[['Site','HydroYear','MonthCensor','MonthNumeric']].copy()
# Use annual_percetnile function to generate annual medians from monthly data
indicator_df = annual_percentile(indicator_df,50)
# Define waterbody, indicator, and special considerations
indicator_df['FreshwaterBodyType'] = 'Lakes'
indicator_df['Indicator'] = 'Annual Median'
indicator_df['SpecialConsiderations'] = 'Separate analysis for open/closed to sea periods'
# Set sample frequency used to calculate median
indicator_df['Frequency'] = 'Monthly'
# Round median numeric result to nearest 0.1
indicator_df['Numeric'] = indicator_df['Numeric'].apply(lambda x : round_half_up(x,1))
# Convert result to a string
indicator_df['Result'] = indicator_df['Censor'].fillna('')+indicator_df['Numeric'].astype(str)
# Set bins for grades
bins = [0,2,5,12,np.inf]
# Use grades function to set grades and grade range
indicator_df = grades(indicator_df,bins)
# Use  grade_check to adjust grade results for detected values
indicator_df = grade_check(indicator_df,monthly_df,bins,'Monthly')
# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(indicator_df)

##############################################################################
'''
Total Nitrogen Annual Median indicator
'''

# Set measurement parameter
measurement = 'Total Nitrogen'
# Use reduce_to_monthly function to generate monthly values dataframe
indicator_df = reduce_to_monthly(StatsData_df[(StatsData_df['Measurement'] == measurement)].copy())
# Save monthly values in case result is censored value with detection limit
# in grade B or C range. Depending on the lower ranked detections, the result
# could be indetermined grade (i.e., A/B, A/B/C, or B/C)
monthly_df = indicator_df[['Site','HydroYear','MonthCensor','MonthNumeric']].copy()
# Use annual_percentile function to generate annual medians from monthly data
indicator_df = annual_percentile(indicator_df,50)
# Define waterbody, indicator, and special considerations
indicator_df['FreshwaterBodyType'] = 'Lakes'
indicator_df['Indicator'] = 'Annual Median'
#indicator_df['SpecialConsiderations'] = 'Separate analysis for open/closed to sea periods'
# Set sample frequency used to calculate median
indicator_df['Frequency'] = 'Monthly'
# Round median numeric result to nearest 0.001, reduced precision for higher concentrations
indicator_df['Numeric'] = indicator_df['Numeric'].apply(lambda x : round_half_up(x,3))
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>0.2,indicator_df['Numeric'].apply(lambda x : round_half_up(x,2)),indicator_df['Numeric'])
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>2.0,indicator_df['Numeric'].apply(lambda x : round_half_up(x,1)),indicator_df['Numeric'])
# Convert result to a string
indicator_df['Result'] = indicator_df['Censor'].fillna('')+indicator_df['Numeric'].astype(str)
# Generate results for Polymictic vs seasonally stratified and brackish sites
# Copy data
indicator_copy_df = indicator_df.copy()
# Start with Seasonally stratified & brackish
indicator_df['SpecialConsiderations'] = 'Seasonally stratified and brackish, separate analysis for open/closed to sea periods'
# Set bins for grades
bins = [0,0.160,0.350,0.750,np.inf]
# Use grades function to set grades and grade range
indicator_df = grades(indicator_df,bins)
# Use  grade_check to adjust grade results for detected values
indicator_df = grade_check(indicator_df,monthly_df,bins,'Monthly')
# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(indicator_df)
# Repeat for Polymictic
indicator_df = indicator_copy_df.copy()
indicator_df['SpecialConsiderations'] = 'Polymictic, separate analysis for open/closed to sea periods'
# Set bins for grades
bins = [0,0.300,0.500,0.800,np.inf]
# Use grades function to set grades and grade range
indicator_df = grades(indicator_df,bins)
# Use  grade_check to adjust grade results for detected values
indicator_df = grade_check(indicator_df,monthly_df,bins,'Monthly')
# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(indicator_df)

##############################################################################
'''
Total Phosphorus Annual Median indicator
'''

# Set measurement parameter
measurement = 'Total Phosphorus'
# Use reduce_to_monthly function to generate monthly values dataframe
indicator_df = reduce_to_monthly(StatsData_df[(StatsData_df['Measurement'] == measurement)].copy())
# Save monthly values in case result is censored value with detection limit
# in grade B or C range. Depending on the lower ranked detections, the result
# could be indetermined grade (i.e., A/B, A/B/C, or B/C)
monthly_df = indicator_df[['Site','HydroYear','MonthCensor','MonthNumeric']].copy()
# Use annual_percentile function to generate annual medians from monthly data
indicator_df = annual_percentile(indicator_df,50)
# Define waterbody, indicator, and special considerations
indicator_df['FreshwaterBodyType'] = 'Lakes'
indicator_df['Indicator'] = 'Annual Median'
indicator_df['SpecialConsiderations'] = 'Separate analysis for open/closed to sea periods'
# Set sample frequency used to calculate median
indicator_df['Frequency'] = 'Monthly'
# Round median numeric result to nearest 0.001, reduced precision for higher concentrations
indicator_df['Numeric'] = indicator_df['Numeric'].apply(lambda x : round_half_up(x,3))
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>0.2,indicator_df['Numeric'].apply(lambda x : round_half_up(x,2)),indicator_df['Numeric'])
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>2.0,indicator_df['Numeric'].apply(lambda x : round_half_up(x,1)),indicator_df['Numeric'])
# Convert result to a string
indicator_df['Result'] = indicator_df['Censor'].fillna('')+indicator_df['Numeric'].astype(str)
# Set bins for grades
bins = [0,0.010,0.020,0.050,np.inf]
# Use grades function to set grades and grade range
indicator_df = grades(indicator_df,bins)
# Use  grade_check to adjust grade results for detected values
indicator_df = grade_check(indicator_df,monthly_df,bins,'Monthly')
# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(indicator_df)

##############################################################################
'''
Ammonia Annual Maximum indicator
'''

# Set measurement parameter
measurement = 'Ammoniacal Nitrogen'
# Save sample values in case result is censored value with detection limit
# in grade B or C range. Depending on the lower ranked detections, the result
# could be indetermined grade (i.e., A/B, A/B/C, or B/C)
sample_df = StatsData_df[StatsData_df['Measurement']==measurement][['Site','HydroYear','Censor','Numeric']].copy()
# Use annual_max function to generate annual max dataframe
indicator_df = annual_max(StatsData_df[StatsData_df['Measurement']==measurement].copy())
# Define indicator and special considerations
indicator_df['FreshwaterBodyType'] = 'Rivers'
indicator_df['Indicator'] = 'Annual Maximum'
indicator_df['SpecialConsiderations'] = 'pH 8 and 20C temp adjustment'
# Set sample frequency as 'All' to indicate all samples are used
indicator_df['Frequency'] = 'All'
# Set bins for grades
bins = [0,0.05,0.40,2.20,np.inf]
# Use grades function to set grades and grade range
indicator_df = grades(indicator_df,bins)
# Use  grade_check to adjust grade results for detected values
indicator_df = grade_check(indicator_df,sample_df,bins,'All')
# Append twice for Lakes and Rivers
for FWType in ['Rivers','Lakes']:
    # Set freshwater type
    indicator_df['FreshwaterBodyType'] = FWType
    # Append to indicator results table
    IndicatorResults_df = IndicatorResults_df.append(indicator_df)

##############################################################################
'''
Ammonia Annual Median indicator
'''

# Set measurement parameter
measurement = 'Ammoniacal Nitrogen'
# Use reduce_to_monthly function to generate monthly values dataframe
indicator_df = reduce_to_monthly(StatsData_df[(StatsData_df['Measurement'] == measurement)].copy())
# Save monthly values in case result is censored value with detection limit
# in grade B or C range. Depending on the lower ranked detections, the result
# could be indetermined grade (i.e., A/B, A/B/C, or B/C)
monthly_df = indicator_df[['Site','HydroYear','MonthCensor','MonthNumeric']].copy()
# Use annual_percentile function to generate annual medians from monthly data
indicator_df = annual_percentile(indicator_df,50)
# Define indicator and special considerations
indicator_df['Indicator'] = 'Annual Median'
indicator_df['SpecialConsiderations'] = 'pH 8 and 20C temp adjustment'
# Set sample frequency used to calculate median
indicator_df['Frequency'] = 'Monthly'
# Round median numeric result to nearest 0.001, reduced precision for higher concentrations
indicator_df['Numeric'] = indicator_df['Numeric'].apply(lambda x : round_half_up(x,3))
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>0.2,indicator_df['Numeric'].apply(lambda x : round_half_up(x,2)),indicator_df['Numeric'])
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>2.0,indicator_df['Numeric'].apply(lambda x : round_half_up(x,1)),indicator_df['Numeric'])
# Convert result to a string
indicator_df['Result'] = indicator_df['Censor'].fillna('')+indicator_df['Numeric'].astype(str)
# Set bins for grades
bins = [0,0.05,0.40,2.20,np.inf]
# Use grades function to set grades and grade range
indicator_df = grades(indicator_df,bins)
# Use  grade_check to adjust grade results for detected values
indicator_df = grade_check(indicator_df,monthly_df,bins,'Monthly')
# Append twice for Lakes and Rivers
for FWType in ['Rivers','Lakes']:
    # Set freshwater type
    indicator_df['FreshwaterBodyType'] = FWType
    # Append to indicator results table
    IndicatorResults_df = IndicatorResults_df.append(indicator_df)

##############################################################################
'''
Nitrate Annual Median indicator
'''

# Set measurement parameter
measurement = 'Nitrate-N Nitrite-N'
# Use reduce_to_monthly function to generate monthly values dataframe
indicator_df = reduce_to_monthly(StatsData_df[(StatsData_df['Measurement'] == measurement)].copy())
# Save monthly values in case result is censored value with detection limit
# in grade B or C range. Depending on the lower ranked detections, the result
# could be indetermined grade (i.e., A/B, A/B/C, or B/C)
monthly_df = indicator_df[['Site','HydroYear','MonthCensor','MonthNumeric']].copy()
# Use annual_percentile function to generate annual medians from monthly data
indicator_df = annual_percentile(indicator_df,50)
# Define waterbody, indicator, and special considerations
indicator_df['FreshwaterBodyType'] = 'Rivers'
indicator_df['Indicator'] = 'Annual Median'
indicator_df['SpecialConsiderations'] = 'Using NNN for NO3 indicator'
# Set sample frequency used to calculate median
indicator_df['Frequency'] = 'Monthly'
# Round median numeric result to nearest 0.0001, reduced precision for higher concentrations
indicator_df['Numeric'] = indicator_df['Numeric'].apply(lambda x : round_half_up(x,4))
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>0.02,indicator_df['Numeric'].apply(lambda x : round_half_up(x,3)),indicator_df['Numeric'])
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>0.2,indicator_df['Numeric'].apply(lambda x : round_half_up(x,2)),indicator_df['Numeric'])
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>2.0,indicator_df['Numeric'].apply(lambda x : round_half_up(x,1)),indicator_df['Numeric'])
# Convert result to a string
indicator_df['Result'] = indicator_df['Censor'].fillna('')+indicator_df['Numeric'].astype(str)
# Set bins for grades
bins = [0,1.0,2.4,6.9,np.inf]
# Use grades function to set grades and grade range
indicator_df = grades(indicator_df,bins)
# Use  grade_check to adjust grade results for detected values
indicator_df = grade_check(indicator_df,monthly_df,bins,'Monthly')
# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(indicator_df)

##############################################################################
'''
Nitrate 95th percentile indicator
'''

# Set measurement parameter
measurement = 'Nitrate-N Nitrite-N'
# Use reduce_to_monthly function to generate monthly values dataframe
indicator_df = reduce_to_monthly(StatsData_df[(StatsData_df['Measurement'] == measurement)].copy())
# Save monthly values in case result is censored value with detection limit
# in grade B or C range. Depending on the lower ranked detections, the result
# could be indetermined grade (i.e., A/B, A/B/C, or B/C)
monthly_df = indicator_df[['Site','HydroYear','MonthCensor','MonthNumeric']].copy()
# Use annual_percentile function to generate annual medians from monthly data
indicator_df = annual_percentile(indicator_df,95)
# Define waterbody, indicator, and special considerations
indicator_df['FreshwaterBodyType'] = 'Rivers'
indicator_df['Indicator'] = '95th Percentile'
indicator_df['SpecialConsiderations'] = 'Using NNN for NO3 indicator'
# Set sample frequency used to calculate median
indicator_df['Frequency'] = 'Monthly'
# Round median numeric result to nearest 0.0001, reduced precision for higher concentrations
indicator_df['Numeric'] = indicator_df['Numeric'].apply(lambda x : round_half_up(x,4))
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>0.02,indicator_df['Numeric'].apply(lambda x : round_half_up(x,3)),indicator_df['Numeric'])
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>0.2,indicator_df['Numeric'].apply(lambda x : round_half_up(x,2)),indicator_df['Numeric'])
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>2.0,indicator_df['Numeric'].apply(lambda x : round_half_up(x,1)),indicator_df['Numeric'])
# Convert result to a string
indicator_df['Result'] = indicator_df['Censor'].fillna('')+indicator_df['Numeric'].astype(str)
# Set bins for grades
bins = [0,1.5,3.5,9.8,np.inf]
# Use grades function to set grades and grade range
indicator_df = grades(indicator_df,bins)
# Use  grade_check to adjust grade results for detected values
indicator_df = grade_check(indicator_df,monthly_df,bins,'Monthly')
# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(indicator_df)

##############################################################################
'''
DRP 5-yr median
'''

# Set measurement parameter
measurement = 'Dissolved Reactive Phosphorus'
# Use reduce_to_monthly function to generate monthly values dataframe
indicator_df = reduce_to_monthly(StatsData_df[(StatsData_df['Measurement'] == measurement)].copy())
# Save monthly values in case result is censored value with detection limit
# in grade B or C range. Depending on the lower ranked detections, the result
# could be indetermined grade (i.e., A/B, A/B/C, or B/C)
monthly_df = indicator_df[['Site','HydroYear','MonthCensor','MonthNumeric']].copy()
# Use multi_year function to generate 5-yr medians
indicator_df = multiyear_percentile(indicator_df,50,5,['Monthly'],[48])
# Define waterbody, indicator, and special considerations
indicator_df['FreshwaterBodyType'] = 'Rivers'
indicator_df['Indicator'] = '5-yr Median'
indicator_df['SpecialConsiderations'] = None
# Round median numeric result to nearest 0.0001, reduced precision for higher concentrations
indicator_df['Numeric'] = indicator_df['Numeric'].apply(lambda x : round_half_up(x,4))
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>0.02,indicator_df['Numeric'].apply(lambda x : round_half_up(x,3)),indicator_df['Numeric'])
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>0.2,indicator_df['Numeric'].apply(lambda x : round_half_up(x,2)),indicator_df['Numeric'])
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>2.0,indicator_df['Numeric'].apply(lambda x : round_half_up(x,1)),indicator_df['Numeric'])
# Convert result to a string
indicator_df['Result'] = indicator_df['Censor'].fillna('')+indicator_df['Numeric'].astype(str)
# Set bins for grades
bins = [0,0.006,0.010,0.018,np.inf]
# Use grades function to set grades and grade range
indicator_df = grades(indicator_df,bins)
# Use  grade_check to adjust grade results for detected values
indicator_df = grade_check(indicator_df,monthly_df,bins,'Monthly')
# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(indicator_df)

##############################################################################
'''
DRP 5-yr 95th Percentile
'''

# Set measurement parameter
measurement = 'Dissolved Reactive Phosphorus'
# Use reduce_to_monthly function to generate monthly values dataframe
indicator_df = reduce_to_monthly(StatsData_df[(StatsData_df['Measurement'] == measurement)].copy())
# Save monthly values in case result is censored value with detection limit
# in grade B or C range. Depending on the lower ranked detections, the result
# could be indetermined grade (i.e., A/B, A/B/C, or B/C)
monthly_df = indicator_df[['Site','HydroYear','MonthCensor','MonthNumeric']].copy()
# Use multi_year function to generate 5-yr medians
indicator_df = multiyear_percentile(indicator_df,95,5,['Monthly'],[48])
# Define waterbody, indicator, and special considerations
indicator_df['FreshwaterBodyType'] = 'Rivers'
indicator_df['Indicator'] = '95th percentile'
indicator_df['SpecialConsiderations'] = None
# Round median numeric result to nearest 0.0001, reduced precision for higher concentrations
indicator_df['Numeric'] = indicator_df['Numeric'].apply(lambda x : round_half_up(x,4))
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>0.02,indicator_df['Numeric'].apply(lambda x : round_half_up(x,3)),indicator_df['Numeric'])
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>0.2,indicator_df['Numeric'].apply(lambda x : round_half_up(x,2)),indicator_df['Numeric'])
indicator_df['Numeric'] = np.where(indicator_df['Numeric']>2.0,indicator_df['Numeric'].apply(lambda x : round_half_up(x,1)),indicator_df['Numeric'])
# Convert result to a string
indicator_df['Result'] = indicator_df['Censor'].fillna('')+indicator_df['Numeric'].astype(str)
# Set bins for grades
bins = [0,0.021,0.030,0.054,np.inf]
# Use grades function to set grades and grade range
indicator_df = grades(indicator_df,bins)
# Use  grade_check to adjust grade results for detected values
indicator_df = grade_check(indicator_df,monthly_df,bins,'Monthly')
# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(indicator_df)

##############################################################################
'''
Export the Results
'''

# Export results to Excel
with pd.ExcelWriter('SW-Results.xlsx') as writer:  
    WQData_df.to_excel(writer, sheet_name='HilltopData',index=True)
    StatsData_df.to_excel(writer, sheet_name='CleanedData',index=False)
    Frequency_df.reset_index().to_excel(writer, sheet_name='SampleFrequency',index=False)
    Unstacked_df.reset_index().to_excel(writer, sheet_name='UnstackedFrequency',index=False)
    IndicatorResults_df.to_excel(writer, sheet_name='IndicatorResults',index=False)

##############################################################################
'''
Combine GW and SW indicator results
'''

# Check if GW results exist otherwise don't merge
if os.path.isfile('GW-Results.xlsx'):
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
