# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from pdsql import mssql

pd.options.display.max_columns = 100

#-Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Wilco Terink'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ ='January 2021'
############################################################################################



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



## Surface water quality
# Water quality data summary table
sw_quality_summ = 'WQDataSumm'
# Water quality dataset types table
sw_quality_dt = 'WQMeasurement'

#TODO: Add lowflow site details....this is currently missing

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

### for water quality ###

## WQDataSumm
# ExtSiteID
# MeasurementID
# DataType: SimpleTimeSeries, WQData, WQSample
# SensorGroup: e.g. Chemicals, Nutrients, Isotopes, Sediment
# Units
# FromDate
# ToDate

## WQMeasurement
# MeasurementID
# Measurement


#### ---> processes below

## Get all the USM site details
USM_site_df = mssql.rd_sql(USM_server, USM_db, USM_table, col_names = USM_site_columns)
USM_site_df.loc[USM_site_df.Name == 'Missing Name', 'Name'] = np.nan  # some names are missing and are classified as Missing Name. These are set to NaN.
df = mssql.rd_sql(USM_server, USM_db, USM_src_table)
USM_site_df = pd.merge(USM_site_df, df, how = 'left', left_on = 'SourceSystem', right_on = 'ID').drop(['SourceSystem', 'ID'], axis=1)
df = None
USM_site_df.to_csv(r'C:\Active\Projects\NPS-FM current state\data\USM\USM_sites.csv', index=False)

## Then process surface water quantity data

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

# get site IDs that could potentially be a lowflow restriction site
siteIDs = pd.unique(df_final.loc[(df_final.SystemName == 'Hydstra'), 'UpstreamSiteID']).tolist()
# get the flow restriction site data and merge it with the final_df
df = mssql.rd_sql(site_data_server, site_data_db, sw_flow_restrict, col_names = ['site', 'date', 'site_type', 'flow_method'], 
                  where_in = {'site': siteIDs}).rename(columns={'site': 'UpstreamSiteID', 'site_type': 'MeasurementType', 'flow_method': 'CollectionType'})
df.date = pd.to_datetime(df.date)
df = df.groupby(['UpstreamSiteID'])
df_mindate = df.min().reset_index().rename(columns={'date': 'FromDate'})
df_maxdate = df.max().reset_index().rename(columns={'date': 'ToDate'})
df = pd.merge(df_mindate[['UpstreamSiteID', 'FromDate']], df_maxdate, how='left', on='UpstreamSiteID')
df_final = pd.merge(df_final, df, how='left', on='UpstreamSiteID')
df = None
# restructure some columns and names
df_final.rename(columns = {'FromDate_x': 'FromDate', 'ToDate_x': 'ToDate', 'MeasurementType_x': 'MeasurementType', 'CollectionType_x': 'CollectionType'}, inplace=True)
df_final.loc[(df_final.SystemName == 'Hydstra') & pd.isna(df_final.DatasetTypeID) & pd.notna(df_final.FromDate_y) & pd.notna(df_final.ToDate_y), 'FromDate'] = df_final['FromDate_y']
df_final.loc[(df_final.SystemName == 'Hydstra') & pd.isna(df_final.DatasetTypeID) & pd.notna(df_final.FromDate_y) & pd.notna(df_final.ToDate_y), 'ToDate'] = df_final['ToDate_y']
df_final.loc[(df_final.SystemName == 'Hydstra') & pd.isna(df_final.DatasetTypeID) & pd.notna(df_final.FromDate_y) & pd.notna(df_final.ToDate_y), 'Feature'] = 'River'
df_final.loc[(df_final.SystemName == 'Hydstra') & pd.isna(df_final.DatasetTypeID) & pd.notna(df_final.FromDate_y) & pd.notna(df_final.ToDate_y), 'MeasurementType'] = df_final['MeasurementType_y']
df_final.loc[(df_final.SystemName == 'Hydstra') & pd.isna(df_final.DatasetTypeID) & pd.notna(df_final.FromDate_y) & pd.notna(df_final.ToDate_y), 'CollectionType'] = df_final['CollectionType_y']
df_final.loc[(df_final.SystemName == 'Hydstra') & pd.isna(df_final.DatasetTypeID) & pd.notna(df_final.FromDate_y) & pd.notna(df_final.ToDate_y), 'Units'] = 'm**3/s'
df_final.loc[(df_final.SystemName == 'Hydstra') & pd.isna(df_final.DatasetTypeID) & pd.notna(df_final.FromDate) & pd.notna(df_final.ToDate), ['FromDate_y', 'ToDate_y', 'MeasurementType_y', 'CollectionType_y']] = np.nan
df = df_final[['UpstreamSiteID', 'Name', 'NZTMX', 'NZTMY', 'SystemName', 'FromDate_y', 'ToDate_y', 'MeasurementType_y', 'CollectionType_y']].copy()
df = df.loc[pd.notna(df.FromDate_y)].drop_duplicates().rename(columns = {'FromDate_y': 'FromDate', 'ToDate_y': 'ToDate', 'MeasurementType_y': 'MeasurementType', 'CollectionType_y': 'CollectionType'})
df['Units'] = 'm**3/s'
df['Feature'] = 'River'
df_final.drop(['FromDate_y', 'ToDate_y', 'MeasurementType_y', 'CollectionType_y'], axis=1, inplace=True)
df_final = pd.concat([df_final, df]).sort_values('UpstreamSiteID')
df = None
#df_final.to_csv(r'C:\Active\Projects\NPS-FM current state\data\USM\test2.csv', index=False)

## Then process the surface water quality data
sw_quality_summ_data = mssql.rd_sql(site_data_server, site_data_db, sw_quality_summ, col_names = ['ExtSiteID', 'MeasurementID', 'DataType', 'SensorGroup', 'Units', 'FromDate', 'ToDate'])

df_final = pd.merge(df_final, sw_quality_summ_data, how='left', left_on='UpstreamSiteID', right_on='ExtSiteID').drop('ExtSiteID', axis=1)
df_final.rename(columns={'FromDate_x': 'FromDate', 'ToDate_x': 'ToDate', 'Units_x': 'Units'}, inplace=True)
sw_quality_summ_data = None

# Create a sub dataframe for the water quality data that has data
wq_df = df_final[['UpstreamSiteID', 'Name', 'NZTMX', 'NZTMY', 'SystemName','MeasurementID','DataType', 'SensorGroup', 'Units_y', 'FromDate_y', 'ToDate_y']].copy()
wq_df = wq_df.loc[pd.notna(wq_df.MeasurementID)]
wq_df.rename(columns={'Units_y': 'Units', 'FromDate_y': 'FromDate', 'ToDate_y': 'ToDate'}, inplace=True)

# Drop the records in final_df where we do not have a datasettypeid, but we do have a measurement ID.
flag = pd.isna(df_final.DatasetTypeID) & pd.notna(df_final.MeasurementID)
df_final.loc[flag] = np.nan
df_final.dropna(inplace=True, how='all')
df_final.drop(['MeasurementID','DataType', 'SensorGroup', 'Units_y', 'FromDate_y', 'ToDate_y'], axis=1, inplace=True) # these water quality columns are dropped because the wq_df is concatted below

# Concat the water quality df
df_final = pd.concat([df_final, wq_df]).sort_values('UpstreamSiteID')
meastypes = pd.unique(df_final['MeasurementID'])
meastypes = meastypes[~np.isnan(meastypes)]
wq_df = None

# Get WQ measurement details and merge
WQMeasurement_df = mssql.rd_sql(site_data_server, site_data_db, sw_quality_dt, col_names = ['MeasurementID', 'Measurement'], where_in = {'MeasurementID': list(meastypes)})
df_final = pd.merge(df_final, WQMeasurement_df, how='left', on='MeasurementID')
WQMeasurement_df = None

# Write final result to csv-file
df_final.drop_duplicates(inplace=True)
df_final.to_csv(r'C:\Active\Projects\NPS-FM current state\data\USM\USM_site_data_summary.csv', index=False)
