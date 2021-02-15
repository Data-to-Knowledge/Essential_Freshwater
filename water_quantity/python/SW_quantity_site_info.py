# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from pdsql import mssql

"""
Queries the ECan databases (USM and Hydro) for USM sites that measure surface water quantity related data:
    Rivers:
        - Flow
        - Water level
        - Temperature
        - Abstraction
    Atmosphere:
        - Precipitation
    Lake:
        - Water level
    Aquifer:
        - Abstraction
"""

pd.options.display.max_columns = 100

#-Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Environment Canterbury'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ ='February 2021'
############################################################################################

# CSV file to write output to
outF = r'C:\Active\Projects\NPS-FM current state\data\USM\USM_site_data_summary.csv'

### Server and database for the USM (contains all our unique sites)
USM_server = 'sql02prod'
USM_db = 'USM'
# USM table containing all the sites
USM_table = 'Site'
# USM table containing source systems
USM_src_table = 'SourceSystem'
# USM site table identifying unique site number
USM_site_id_field = 'UpstreamSiteID'
# Columns that are needed from this table (Active column is not copied because all sites seem to be active which is weird)
USM_site_columns = [USM_site_id_field, 'Name', 'NZTMX', 'NZTMY', 'SourceSystem']

### Server and hydro database for data that belongs to the sites. Contains both water quantity and quality data
site_data_server = 'edwprod01'
site_data_db = 'Hydro'

## Surface water quantity
# Water quantity data summary table
sw_quantity_summ = 'TSDataNumericSumm'
# Water quantity hourly data summary table
sw_quantity_summ_hour = 'TSDataNumericHourlySumm'
# Water quantity daily data summary table
sw_quantity_summ_day = 'TSDataNumericDailySumm'
# Water quantity dataset types table and view thereof
sw_quantity_dt = 'DatasetType'
sw_quantity_dt_view = 'vDatasetTypeNamesAll'
# Water quantity measurement types table
sw_quantity_mt = 'MeasurementType'
# Flow restrictions table
sw_flow_restrict = 'LowFlowRestrSite'



### for water quantity ###

## TSDataNumericSumm
# ExtSiteID
# DatasetTypeID
# FromDate
# ToDate

## vDatasetTypeNamesAll
# DatasetTypeID
# Feature: e.g. river, lake, atmosphere
# MeasurementType (MTypeID): e.g. abstraction, flow, precipitation, et actual
# CollectionType: Manual Field, Manual Lab, Recorder
# DataCode: Correlated, Primary, RAW, Restriction, Synthetic
# DataProvider: e.g., NIWA, ECan, Aqualinc
     
## MeasurementType
# MTypeID
# MeasurementType
# Units

#### ---> processes below

## Get all the USM site details
USM_site_df = mssql.rd_sql(USM_server, USM_db, USM_table, col_names = USM_site_columns)
USM_site_df.loc[USM_site_df.Name == 'Missing Name', 'Name'] = np.nan  # some names are missing and are classified as Missing Name. These are set to NaN.
df = mssql.rd_sql(USM_server, USM_db, USM_src_table)
USM_site_df = pd.merge(USM_site_df, df, how = 'left', left_on = 'SourceSystem', right_on = 'ID').drop(['SourceSystem', 'ID'], axis=1)
df = None
# Drop the Hilltop sites because this is covered by other team members
USM_site_df = USM_site_df.loc[USM_site_df.SystemName != 'Hilltop']


## Process surface water quantity data

# Get the water quantity summary data
sw_quantity_summ_data1 = mssql.rd_sql(site_data_server, site_data_db, sw_quantity_summ, col_names = ['ExtSiteID', 'DatasetTypeID', 'FromDate', 'ToDate'])
sw_quantity_summ_data2 = mssql.rd_sql(site_data_server, site_data_db, sw_quantity_summ_hour, col_names = ['ExtSiteID', 'DatasetTypeID', 'FromDate', 'ToDate'])
sw_quantity_summ_data3 = mssql.rd_sql(site_data_server, site_data_db, sw_quantity_summ_day, col_names = ['ExtSiteID', 'DatasetTypeID', 'FromDate', 'ToDate'])
sw_quantity_summ_data3['FromDate'] = pd.to_datetime(sw_quantity_summ_data3['FromDate']) 
sw_quantity_summ_data3['ToDate'] = pd.to_datetime(sw_quantity_summ_data3['ToDate'])
sw_quantity_summ_data = pd.concat([sw_quantity_summ_data1, sw_quantity_summ_data2, sw_quantity_summ_data3])
sw_quantity_summ_data = sw_quantity_summ_data.groupby(['ExtSiteID', 'DatasetTypeID']).first().reset_index()
sw_quantity_summ_data1 = None; sw_quantity_summ_data2 = None; sw_quantity_summ_data3 = None;

# Merge USM sites with surface water quantity summary
df = pd.merge(USM_site_df, sw_quantity_summ_data, how='left', left_on='UpstreamSiteID', right_on='ExtSiteID').drop('ExtSiteID', axis=1)
# Split in two dataframes, one for missing datasettypes and one with datasettypes
df1 = df.loc[pd.isna(df.DatasetTypeID)]
df2 = df.loc[pd.notna(df.DatasetTypeID)]

# Get datasettypes information
df2_dtypes = mssql.rd_sql(site_data_server, site_data_db, sw_quantity_dt_view, col_names = ['DatasetTypeID', 'Feature', 'MeasurementType', 'CollectionType', 'DataCode', 'DataProvider'], where_in = {'DatasetTypeID': pd.unique(df2.DatasetTypeID).tolist()})
df2_mtypes1 = mssql.rd_sql(site_data_server, site_data_db, sw_quantity_dt, col_names = ['DatasetTypeID', 'MTypeID'], where_in = {'DatasetTypeID': pd.unique(df2.DatasetTypeID).tolist()})
df2_mtypes2 = mssql.rd_sql(site_data_server, site_data_db, sw_quantity_mt, col_names = ['MTypeID', 'Units'], where_in = {'MTypeID': pd.unique(df2_mtypes1.MTypeID).tolist()})
df2_mtypes3 = pd.merge(df2_mtypes1, df2_mtypes2, how = 'left', on='MTypeID').drop('MTypeID', axis=1) 
df2_dtypes = pd.merge(df2_dtypes, df2_mtypes3, how='left', on='DatasetTypeID')
df2 = pd.merge(df2, df2_dtypes, how='left', on = 'DatasetTypeID')
df2_dtypes = None; df2_mtypes1 = None; df2_mtypes2 = None; df2_mtypes3 = None
df_final = pd.concat([df2, df1]).sort_values('UpstreamSiteID')
df = None; df1 = None; df2 = None;

# Kick out acuifer water level and temperature as this is covered by Phil / Kurt
df_final = df_final.loc[~((df_final.Feature == 'Aquifer') & (df_final.MeasurementType == 'Water Level'))]
df_final = df_final.loc[~((df_final.Feature == 'Aquifer') & (df_final.MeasurementType == 'Temperature'))]

# # get site IDs that could potentially be a lowflow restriction site
# siteIDs = pd.unique(df_final.loc[(df_final.SystemName == 'Hydstra'), 'UpstreamSiteID']).tolist()
# # get the flow restriction site data and merge it with the final_df
# df = mssql.rd_sql(site_data_server, site_data_db, sw_flow_restrict, col_names = ['site', 'date', 'site_type', 'flow_method'], 
#                   where_in = {'site': siteIDs}).rename(columns={'site': 'UpstreamSiteID', 'site_type': 'MeasurementType', 'flow_method': 'CollectionType'})
# df.date = pd.to_datetime(df.date)
# df = df.groupby(['UpstreamSiteID'])
# df_mindate = df.min().reset_index().rename(columns={'date': 'FromDate'})
# df_maxdate = df.max().reset_index().rename(columns={'date': 'ToDate'})
# df = pd.merge(df_mindate[['UpstreamSiteID', 'FromDate']], df_maxdate, how='left', on='UpstreamSiteID')
# df_final = pd.merge(df_final, df, how='left', on='UpstreamSiteID')
# df = None
# # restructure some columns and names
# df_final.rename(columns = {'FromDate_x': 'FromDate', 'ToDate_x': 'ToDate', 'MeasurementType_x': 'MeasurementType', 'CollectionType_x': 'CollectionType'}, inplace=True)
# df_final.loc[(df_final.SystemName == 'Hydstra') & pd.isna(df_final.DatasetTypeID) & pd.notna(df_final.FromDate_y) & pd.notna(df_final.ToDate_y), 'FromDate'] = df_final['FromDate_y']
# df_final.loc[(df_final.SystemName == 'Hydstra') & pd.isna(df_final.DatasetTypeID) & pd.notna(df_final.FromDate_y) & pd.notna(df_final.ToDate_y), 'ToDate'] = df_final['ToDate_y']
# df_final.loc[(df_final.SystemName == 'Hydstra') & pd.isna(df_final.DatasetTypeID) & pd.notna(df_final.FromDate_y) & pd.notna(df_final.ToDate_y), 'Feature'] = 'River'
# df_final.loc[(df_final.SystemName == 'Hydstra') & pd.isna(df_final.DatasetTypeID) & pd.notna(df_final.FromDate_y) & pd.notna(df_final.ToDate_y), 'MeasurementType'] = df_final['MeasurementType_y']
# df_final.loc[(df_final.SystemName == 'Hydstra') & pd.isna(df_final.DatasetTypeID) & pd.notna(df_final.FromDate_y) & pd.notna(df_final.ToDate_y), 'CollectionType'] = df_final['CollectionType_y']
# df_final.loc[(df_final.SystemName == 'Hydstra') & pd.isna(df_final.DatasetTypeID) & pd.notna(df_final.FromDate_y) & pd.notna(df_final.ToDate_y), 'Units'] = 'm**3/s'
# df_final.loc[(df_final.SystemName == 'Hydstra') & pd.isna(df_final.DatasetTypeID) & pd.notna(df_final.FromDate) & pd.notna(df_final.ToDate), ['FromDate_y', 'ToDate_y', 'MeasurementType_y', 'CollectionType_y']] = np.nan
# df = df_final[['UpstreamSiteID', 'Name', 'NZTMX', 'NZTMY', 'SystemName', 'FromDate_y', 'ToDate_y', 'MeasurementType_y', 'CollectionType_y']].copy()
# df = df.loc[pd.notna(df.FromDate_y)].drop_duplicates().rename(columns = {'FromDate_y': 'FromDate', 'ToDate_y': 'ToDate', 'MeasurementType_y': 'MeasurementType', 'CollectionType_y': 'CollectionType'})
# df['Units'] = 'm**3/s'
# df['Feature'] = 'River'
# df_final.drop(['FromDate_y', 'ToDate_y', 'MeasurementType_y', 'CollectionType_y'], axis=1, inplace=True)
# df_final = pd.concat([df_final, df]).sort_values('UpstreamSiteID')
# df = None

# Drop records with DataCode = RAW and MeasurementType not equals Abstraction. We only want good quality data, but abstraction data is only in DataCode = Raw
df_final = df_final.loc[~((df_final.MeasurementType != 'Abstraction') & (df_final.DataCode == 'RAW'))]

# Drop records that are manual water levels and not lake
df_final = df_final.loc[~((df_final.MeasurementType == 'Water Level') & (df_final.Feature == 'River') & (df_final.CollectionType == 'Manual Field'))]

# Drop records that are manual river temperature
df_final = df_final.loc[~((df_final.MeasurementType == 'Temperature') & (df_final.Feature == 'River') & (df_final.CollectionType == 'Manual Field'))]

# Drop records with recorded river water level if it has recorded flows
df1 = df_final.loc[(df_final.Feature == 'River') & ((df_final.MeasurementType == 'Flow') | (df_final.MeasurementType == 'Water Level')) & (df_final.CollectionType == 'Recorder') & (df_final.DataCode == 'Primary')]
df2 = df_final.loc[~((df_final.Feature == 'River') & ((df_final.MeasurementType == 'Flow') | (df_final.MeasurementType == 'Water Level')) & (df_final.CollectionType == 'Recorder') & (df_final.DataCode == 'Primary'))]
water_level_and_flow_df = df1.groupby(['UpstreamSiteID', 'MeasurementType']).first().reset_index()
water_level_and_flow_sites = []
unique_sites = pd.unique(water_level_and_flow_df.UpstreamSiteID).tolist()
for s in unique_sites:
    df = water_level_and_flow_df.loc[water_level_and_flow_df.UpstreamSiteID == s]
    if len(df) > 1:
        water_level_and_flow_sites.append(s)
df1.loc[((df1.UpstreamSiteID.isin(water_level_and_flow_sites)) & (df1.MeasurementType == 'Water Level'))] = np.nan
df1.dropna(how='all', inplace=True)
df_final = pd.concat([df1, df2])
df1 = None; df2 = None; df = None; unique_sites = None

# Drop records for river recorder flow sites that have restriction as DataCode if there is also a primary datacode for that site
df1 = df_final.loc[((df_final.Feature == 'River') & (df_final.MeasurementType == 'Flow') & (df_final.CollectionType == 'Recorder') & ((df_final.DataCode == 'Primary') | (df_final.DataCode == 'Restriction')))] 
df2 = df_final.loc[~((df_final.Feature == 'River') & (df_final.MeasurementType == 'Flow') & (df_final.CollectionType == 'Recorder') & ((df_final.DataCode == 'Primary') | (df_final.DataCode == 'Restriction')))]
primary_and_restriction_df = df1.groupby(['UpstreamSiteID', 'DataCode']).first().reset_index()
primary_and_restriction_sites = []
unique_sites = pd.unique(primary_and_restriction_df.UpstreamSiteID).tolist()
for s in unique_sites:
    df = primary_and_restriction_df.loc[primary_and_restriction_df.UpstreamSiteID == s]
    if len(df) > 1:
        primary_and_restriction_sites.append(s)
df1.loc[((df1.UpstreamSiteID.isin(primary_and_restriction_sites)) & (df1.DataCode == 'Restriction'))] = np.nan
df1.dropna(how='all', inplace=True)
df_final = pd.concat([df1, df2])
df1 = None; df2 = None; df = None; unique_sites = None      

# Dop records for manual river flow sites if they also have a recorder
df1 = df_final.loc[((df_final.Feature == 'River') & (df_final.MeasurementType == 'Flow') & ((df_final.CollectionType == 'Recorder') | (df_final.CollectionType == 'Manual Field')))]
df2 = df_final.loc[~((df_final.Feature == 'River') & (df_final.MeasurementType == 'Flow') & ((df_final.CollectionType == 'Recorder') | (df_final.CollectionType == 'Manual Field')))]
recorder_and_manual_df = df1.groupby(['UpstreamSiteID', 'CollectionType']).first().reset_index()
recorder_and_manual_df_sites = []
unique_sites = pd.unique(recorder_and_manual_df.UpstreamSiteID).tolist()
for s in unique_sites:
    df = recorder_and_manual_df.loc[recorder_and_manual_df.UpstreamSiteID == s]
    if len(df) > 1:
        recorder_and_manual_df_sites.append(s)
df1.loc[((df1.UpstreamSiteID.isin(recorder_and_manual_df_sites)) & (df1.CollectionType == 'Manual Field'))] = np.nan
df1.dropna(how='all', inplace=True)
df_final = pd.concat([df1, df2])
df1 = None; df2 = None; df = None; unique_sites = None  

# Finally drop all records where we do not have any data associated with the site
df_final = df_final.loc[pd.notna(df_final.MeasurementType)]

# Drop the DatasetTypeID column because it is not needed in final results
df_final.drop('DatasetTypeID', axis=1, inplace=True)

# Add other label columns to it: Data prior to 2020, Data for 2020 and later
df_final[['Data prior to 2020', 'Date for 2020 and later']] = np.nan
df_final.loc[df_final.ToDate < pd.Timestamp(2020,1,1), 'Data prior to 2020'] = 'YES' 
df_final.loc[df_final.FromDate >= pd.Timestamp(2020,1,1), 'Date for 2020 and later'] = 'YES'
df_final['Data prior to 2020, 2020, and later'] = np.nan
df_final.loc[(df_final.ToDate >= pd.Timestamp(2020,1,1)) & (df_final.FromDate < pd.Timestamp(2020,1,1)), 'Data prior to 2020, 2020, and later'] = 'YES'

# Write final result to csv-file
df_final.drop_duplicates(inplace=True)
df_final.to_csv(outF, index=False)

