# -*- coding: utf-8 -*-
"""
Python Script to generate indicator results for GW Nitrate-nitrogen Annual Max

Created on Fri Jun 6 09:08:13 2021

@author: KurtV
"""

# import python modules
from hilltoppy import web_service as ws
import csv
import pandas as pd
import numpy as np

measurements = ['Nitrate Nitrogen','E. coli']

# Set URL
base_url = 'http://wateruse.ecan.govt.nz'

# Set server hts file (Server hts name, not Hilltop01 hts name!)
# WQGroundwater.hts = \Hilltop01\Data\WQGroundwaterCombined.dsn
hts = 'WQGroundwater.hts'

##############################################################################

# Generate a list of all sites in the server file
hts_sites_list = sorted(ws.site_list(base_url,hts).SiteName.tolist(),key=str.lower)
# Only include sites that contain '/' in the site name
hts_sites_list = [site for site in hts_sites_list if '/' in site]

##############################################################################

# Import set of SoE monitoring project codes
with open('GWSoEProjectCodes.csv', 'r', encoding='utf-8-sig') as f:
    project_codes = [row[0] for row in csv.reader(f)]
    # Remove potential leading and tailing spaces
f.close()

##############################################################################

# Initiate empty list of dataframes
WQData = []
#Extract Nitrate Nitrogen and E.coli data with measurement and sample parameters
for site in hts_sites_list:
    print(site)
    try:
        # Obtain the sample parameter metadata
        sample_data = ws.get_data(base_url,hts,site,'WQ Sample',from_date='1001-01-01',to_date='9999-01-01').unstack('Parameter')
        sample_data.columns = sample_data.columns.droplevel()
    except ValueError:
        sample_data = pd.DataFrame()
    # Create list of sample parameters
    sample_parameters = sample_data.columns
    # Rearrange dataframe
    sample_data = pd.concat([sample_data],axis=1,keys=['Sample Parameters'])
    # Create dataframe for units
    units_df = ws.measurement_list(base_url,hts,site)
    
    # Check if there is any measurement data
    if units_df.empty:
        pass
    else:
        # Obtain the desired measurement results
        for measurement in measurements:
            # Check if site has measurement
            if measurement in units_df.index.get_level_values(1):
                # Obtain measurement data from Hilltop
                data = ws.get_data(base_url,hts,site,measurement,from_date='1001-01-01',to_date='9999-01-01',parameters=True,quality_codes=True)
                # Format measurement results, measurement metadata and join to the sample metadata
                meta_data = data[1].unstack('Parameter').droplevel(1)
                meta_data.columns = meta_data.columns.droplevel()
                meta_data = meta_data.drop([x for x in sample_parameters if x in meta_data.columns],axis=1)
                data = data[0].droplevel(1)
                data.columns = ['({})'.format(units_df['Units'].loc[site,measurement])]
                data = pd.concat([data,meta_data],axis=1)
                data = pd.concat([data],axis=1,keys=[measurement])
                sample_data = pd.concat([sample_data,data],axis=1)
    WQData.append(sample_data)

# Create WQ times series dataframe from list of site dataframes
WQData_df = pd.concat(WQData,sort = False)
WQData_df = WQData_df.reindex(['Sample Parameters']+measurements,axis=1,level=0)

##############################################################################

# Generate data that has been filter by project code
SoEData_df = WQData_df[WQData_df['Sample Parameters','Project'].isin(project_codes)].drop('Sample Parameters',axis=1)
# Take relevant data and append to StatsData_df
StatsData_df = pd.DataFrame(columns=['Site','Measurement','Units','HydroYear','DateTime','Observation','Censor','Numeric','Curated'])
    
# Consider Nitrate Nitrogen data
measurement == 'Nitrate Nitrogen'
MeasurementData = SoEData_df['Nitrate Nitrogen','(mg/L)']
MeasurementData.name = 'Observation'
# Remove * values and nan values
MeasurementData = MeasurementData[MeasurementData != '*'].dropna()
# Remove values where result is 0 or <0
MeasurementData = MeasurementData[MeasurementData != '0']
MeasurementData = MeasurementData[MeasurementData != '<0']
# Create dataframe
MeasurementData_df = MeasurementData.to_frame().reset_index()
# Insert name of measurement and units
MeasurementData_df['Measurement'] = 'Nitrate Nitrogen'
MeasurementData_df['Units'] = 'mg/L'
StatsData_df = StatsData_df.append(MeasurementData_df)


# Review E. coli data
measurement == 'E. coli'
MeasurementData = SoEData_df['E. coli','(MPN/100mL)']
MeasurementData.name = 'Observation'
# Remove * values and nan values
MeasurementData = MeasurementData[MeasurementData != '*'].dropna()
# Create dataframe
MeasurementData_df = MeasurementData.to_frame().reset_index()
# Insert name of measurement and units
MeasurementData_df['Measurement'] = 'E. coli'
MeasurementData_df['Units'] = 'MPN/100mL'
StatsData_df = StatsData_df.append(MeasurementData_df)

# Create HydroYear column from year and month
StatsData_df['HydroYear'] = np.where(StatsData_df.DateTime.dt.month <= 6,
                                                 StatsData_df.DateTime.dt.year,
                                                 StatsData_df.DateTime.dt.year+1)

# Split censor component from numeric component of observation
# and calculate curated value
StatsData_df['Censor'] = np.where(StatsData_df['Observation'].str.startswith(tuple(['<','>'])),StatsData_df['Observation'].str[0],np.nan)
StatsData_df['Numeric'] = StatsData_df['Observation'].map(lambda x: x.replace('<','').replace('>',''))
StatsData_df['Numeric'] = pd.to_numeric(StatsData_df['Numeric'])
StatsData_df['Curated'] = StatsData_df['Numeric']
StatsData_df['Curated'] = np.where(StatsData_df['Censor']=='<',0.5*StatsData_df['Numeric'],StatsData_df['Curated'])
StatsData_df['Curated'] = np.where(StatsData_df['Censor']=='>',1.1*StatsData_df['Numeric'],StatsData_df['Curated'])


##############################################################################

# Initiate indicator results dataframe
IndicatorResults_df = pd.DataFrame(columns=['Site','Measurement','Units','Indicator','HydroYear','Result','Censor','Numeric','Grade'])
IndicatorResults_df['HydroYear'] = IndicatorResults_df['HydroYear'].astype(int)

##############################################################################
'''
Nitrate Nitrogen Annual Max for Groundwater
'''
# Start by only considering the maximum curated nitrate nitrogen values when grouped by site and hydro year
NitrateMax_df = StatsData_df[(StatsData_df['Measurement'] == 'Nitrate Nitrogen')].groupby(['Site','HydroYear'])['Curated'].max().reset_index()
NitrateMax_df['Measurement'] = 'Nitrate Nitrogen'
# Rename the curated column as the maximum
NitrateMax_df.rename(columns={'Curated':'Max'}, inplace=True)
# Merge to the original table to determine if the result needs to be expressed as a censored value
NitrateMax_df = pd.merge(NitrateMax_df,StatsData_df,on=['Site','Measurement','HydroYear'],how='left')
# Only consider rows where the curated value is the maximum
NitrateMax_df = NitrateMax_df[NitrateMax_df['Max']==NitrateMax_df['Curated']]
# Rank the censored values such that if the curated value occurs as both a
# numerically observed result and as a censored value, the indicator result is
# the more appropriate (i.e., if 0.05 and <0.1 are both in the time series,
# both have a curated value of 0.05. Use censor ranking to choose which should
# be returned as the maximum.)
NitrateMax_df['CensorRank'] = NitrateMax_df['Censor'].map({'>':1,np.nan:2,'<':3})
# Sort values based on the censor ranking
NitrateMax_df = NitrateMax_df.sort_values(by='CensorRank',ascending=True)
# Drop duplicates keeping the highest censor rank
NitrateMax_df = NitrateMax_df.drop_duplicates(subset=['Site','HydroYear'])
# Set bins for the indicator grades and add grade column
bins = [0,1,5.65,11.3,np.inf]
NitrateMax_df['Grade'] = pd.cut(NitrateMax_df['Max'],bins,labels=['A','B','C','D'])
# Remove unneeded columns
NitrateMax_df = NitrateMax_df.drop(columns=['Max','DateTime','CensorRank','Curated'])
# Rename the observation as the indicator result
NitrateMax_df.rename(columns={'Observation':'Result'}, inplace=True)
# Reorder based on site name and hydro year
NitrateMax_df = NitrateMax_df.sort_values(by=['Site','HydroYear'],ascending=True)
# Add columns to complete information for appending to full indicator results
NitrateMax_df['Indicator'] = 'Nitrate Nitrogen Annual Max'
# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(NitrateMax_df)

##############################################################################
'''
Nitrate Nitrogen 5-yr median
'''
# Start by only considering the nitrate nitrogen values
NitrateMed_df = StatsData_df[(StatsData_df['Measurement'] == 'Nitrate Nitrogen')]

# The 5-yr median should have all years evenly weighted, all quarters within a
# year evenly weighted, all months within a quarter evenly weighted, and all days
# within a month evenly weighted. This prevents changes in data frequency from
# having significant impacts on the result

# Add quarter, month, and day columns
NitrateMed_df.loc[:,'Quarter'] = NitrateMed_df['DateTime'].dt.quarter
NitrateMed_df.loc[:,'Month'] = NitrateMed_df['DateTime'].dt.month
NitrateMed_df.loc[:,'Day'] = NitrateMed_df['DateTime'].dt.dayofyear

# Take median of values taken on the same day at the same site
Median = NitrateMed_df.groupby(['Site','HydroYear','Day'])['Curated'].median().reset_index().rename(columns={'Curated':'DayMedian'})
# Join to original table and remove duplicates
NitrateMed_df = pd.merge(NitrateMed_df,Median,on=['Site','HydroYear','Day'],how='left').drop(columns=['DateTime','Observation','Censor','Numeric','Curated'])
# Drop duplicate days
NitrateMed_df = NitrateMed_df.drop_duplicates(subset=['Site','HydroYear','Day'])

# Take median of values taken in the same month at the same site
Median = NitrateMed_df.groupby(['Site','HydroYear','Month'])['DayMedian'].median().reset_index().rename(columns={'DayMedian':'MonthMedian'})
# Join to original table and remove duplicates
NitrateMed_df = pd.merge(NitrateMed_df,Median,on=['Site','HydroYear','Month'],how='left').drop(columns=['DayMedian','Day'])
# Drop duplicate months
NitrateMed_df = NitrateMed_df.drop_duplicates(subset=['Site','HydroYear','Month'])

# Take median of values taken in the same quarter at the same site
Median = NitrateMed_df.groupby(['Site','HydroYear','Quarter'])['MonthMedian'].median().reset_index().rename(columns={'MonthMedian':'QuarterMedian'})
# Join to original table and remove duplicates
NitrateMed_df = pd.merge(NitrateMed_df,Median,on=['Site','HydroYear','Quarter'],how='left').drop(columns=['MonthMedian','Month'])
# Drop duplicate quarters
NitrateMed_df = NitrateMed_df.drop_duplicates(subset=['Site','HydroYear','Quarter'])

# Take median of values taken in the same year at the same site
Median = NitrateMed_df.groupby(['Site','HydroYear'])['QuarterMedian'].median().reset_index().rename(columns={'QuarterMedian':'AnnualMedian'})
# Join to original table and remove duplicates
NitrateMed_df = pd.merge(NitrateMed_df,Median,on=['Site','HydroYear'],how='left').drop(columns=['QuarterMedian','Quarter'])
# Drop duplicate quarters
NitrateMed_df = NitrateMed_df.drop_duplicates(subset=['Site','HydroYear'])

# 5-yr medians require 4 years of results so a year can have a median result 
# despite no data collection that year so missing years need to be filled in
NitrateMed_df.HydroYear = pd.Categorical(NitrateMed_df.HydroYear)
NitrateMed_df = NitrateMed_df.groupby(['Site','HydroYear']).sum()
# In years where no samples are collected, replace the 0 count with nan
NitrateMed_df['AnnualMedian'] = np.where(NitrateMed_df['AnnualMedian']==0,np.nan,NitrateMed_df['AnnualMedian'])
# For each year, take the median of the past 5 years
# Four values are required to generate results
NitrateMed_df['Numeric'] = NitrateMed_df.groupby(['Site']).rolling(window=5,min_periods=4).median()['AnnualMedian'].reset_index().rename(columns={'AnnualMedian':'Median5yr'})['Median5yr'].values
# Remove unneeded columns, reset index, and set hydro year data type to int
NitrateMed_df = NitrateMed_df.drop(columns=['AnnualMedian'])
NitrateMed_df = NitrateMed_df.dropna().reset_index()
NitrateMed_df['HydroYear'] = NitrateMed_df['HydroYear'].astype(int)
# Set bins for the indicator grades and add grade column
bins = [0,1,5.65,11.3,np.inf]
NitrateMed_df['Grade'] = pd.cut(NitrateMed_df['Numeric'],bins,labels=['A','B','C','D'])
# Add columns to complete information for appending to full indicator results
NitrateMed_df['Measurement'] = 'Nitrate Nitrogen'
NitrateMed_df['Units'] = 'mg/L'
NitrateMed_df['Indicator'] = 'Nitrate Nitrogen 5-yr Median'
# For median values below 0.1, report result as <0.1
# Set censor component of values <0.1 as <
NitrateMed_df['Censor'] = np.where(NitrateMed_df['Numeric'] < 0.1,'<',np.nan)
# Set numeric component of values <0.1 as 0.1 and round others to two decimals
NitrateMed_df['Numeric'] = np.where(NitrateMed_df['Censor']=='<',0.1,round(NitrateMed_df['Numeric'],2))
# For values greater than 2 mg/L, round to a single decimal
NitrateMed_df['Numeric'] = np.where(NitrateMed_df['Numeric']>=2,round(NitrateMed_df['Numeric'],1),NitrateMed_df['Numeric'])
# Convert result to a string
NitrateMed_df['Result'] = NitrateMed_df['Numeric'].astype(str)
# Where the result is censored, change the text result to <0.1
NitrateMed_df['Result'] = np.where(NitrateMed_df['Censor']=='<','<0.1',NitrateMed_df['Result'])

# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(NitrateMed_df)

##############################################################################
'''
E. coli 5-yr percent exceedances above drinking water standard for Groundwater
'''
# Start by only considering the E. coli values
Ecoli5yrEx_df = StatsData_df[(StatsData_df['Measurement'] == 'E. coli')]
# Drop any detection limits that don't work with drinking water standard
# (i.e., drop value of <2 from K38/0408)
Ecoli5yrEx_df = Ecoli5yrEx_df[~((Ecoli5yrEx_df['Censor']=='<')&(Ecoli5yrEx_df['Numeric']!=1))]
# Count the number of samples and exceedances (detections) found each hydro year
Samples = Ecoli5yrEx_df.groupby(['Site','HydroYear']).size().reset_index().rename(columns={0:'Samples'})
Exceedances = Ecoli5yrEx_df[Ecoli5yrEx_df['Censor']!='<'].groupby(['Site','HydroYear']).size().reset_index().rename(columns={0:'Exceedances'})
# Merge these results to the data
Ecoli5yrEx_df = pd.merge(Ecoli5yrEx_df,Samples,on=['Site','HydroYear'],how='left')
Ecoli5yrEx_df = pd.merge(Ecoli5yrEx_df,Exceedances,on=['Site','HydroYear'],how='left')
# Where no exceedances occur in a given year, fill with 0
Ecoli5yrEx_df['Exceedances'] = Ecoli5yrEx_df['Exceedances'].fillna(0)
# Remove unneeded columns
Ecoli5yrEx_df = Ecoli5yrEx_df.drop(columns=['DateTime','Observation','Censor','Numeric','Curated'])
# Drop duplicates within a hydro year at a site
Ecoli5yrEx_df = Ecoli5yrEx_df.drop_duplicates(subset=['Site','HydroYear'])
# Years without data can still have a result if the preceeding 4 years have
# results. For this reason, we fill the missing years at a site
Ecoli5yrEx_df.HydroYear = pd.Categorical(Ecoli5yrEx_df.HydroYear)
Ecoli5yrEx_df = Ecoli5yrEx_df.groupby(['Site','HydroYear']).sum()
# In years where no samples are collected, replace the 0 count with nan
Ecoli5yrEx_df['Samples'] = np.where(Ecoli5yrEx_df['Samples']==0,np.nan,Ecoli5yrEx_df['Samples'])
# For each year, count the past 5 years of samples and exceedances
# Four values are required to generate results
Ecoli5yrEx_df[['Samples5yr','Exceedances5yr']] = Ecoli5yrEx_df.groupby(['Site']).rolling(window=5,min_periods=4).sum()[['Samples','Exceedances']].reset_index().rename(columns={'Samples':'Samples5yr','Exceedances':'Exceedances5yr'})[['Samples5yr','Exceedances5yr']].values
# Calculate the percentage by dividing the exceedances by the samples and multiply by 100.
Ecoli5yrEx_df['Result'] = round(Ecoli5yrEx_df['Exceedances5yr']/Ecoli5yrEx_df['Samples5yr']*100,2)
# Remove unneeded columns, reset index, and set hydro year data type to int
Ecoli5yrEx_df = Ecoli5yrEx_df.drop(columns=['Samples','Exceedances','Samples5yr','Exceedances5yr'])
Ecoli5yrEx_df = Ecoli5yrEx_df.dropna().reset_index()
Ecoli5yrEx_df['HydroYear'] = Ecoli5yrEx_df['HydroYear'].astype(int)
# Set bins for the indicator grades and add grade column
bins = [-0.01,5,25,50,100]
Ecoli5yrEx_df['Grade'] = pd.cut(Ecoli5yrEx_df['Result'],bins,labels=['A','B','C','D'])
# Add columns to complete information for appending to full indicator results
Ecoli5yrEx_df['Measurement'] = 'E. coli'
Ecoli5yrEx_df['Units'] = 'MPN/100mL'
Ecoli5yrEx_df['Indicator'] = 'E. coli 5-yr percent exceedances'
Ecoli5yrEx_df['Censor'] = np.nan
Ecoli5yrEx_df['Numeric'] = Ecoli5yrEx_df['Result']
Ecoli5yrEx_df['Result'] = Ecoli5yrEx_df['Result'].astype(str)
# Append to indicator results table
IndicatorResults_df = IndicatorResults_df.append(Ecoli5yrEx_df)

##############################################################################
'''
Export the Results
'''

# Export results to Excel
with pd.ExcelWriter('IndicatorResults.xlsx') as writer:  
    WQData_df.to_excel(writer, sheet_name='WQData',index=True)
    StatsData_df.to_excel(writer, sheet_name='CuratedData',index=False)
    IndicatorResults_df.to_excel(writer, sheet_name='IndicatorResults',index=False)





