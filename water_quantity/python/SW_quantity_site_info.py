# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from pdsql import mssql

"""
************************************************************************************************************
Queries the ECan databases (USM and Hydro) for USM sites that measure surface water quantity related data.

The following data is extracted from Hydstra (from a csv-file):
    Rivers:
        - Flow
        - Water level
    Lakes:
        - Water level
    Atmosphere:
        - Precipitation

The following data is extracted from the Hydro database:
    Rivers:
        - Abstraction
    Aquifer:
        - Abstraction
************************************************************************************************************

Hydstra site specific info:

The sites down here that were put in as low flow indicators are:
66 Burkes Creek
2312 Crows Drain
2314 Harris Drain
2316 Oakdale Drain
1693065 Oakdale Drain
These should not be used for anything, i.e. they are removed from the record

Northern sites below should only be used for water level (flow records are dropped for the four sites below)
66214 Ashley RTB - only rated for high flows - low flows meaningless
66404 Waimakariri Esk - stage only (one rating in Hydstra in 1995 which was a very crude guide to predict flows at OHB using Esk stage - no gaugings at this site)
68003 Selwyn Ridgens Road - stage only (one rating in Hydstra in 2004 filed for some NIWA research)
68502 Rakaia Gorge - stage only  (historically rated until 1979 then again for a period in 2000/2001 when NIWA rated the site after their recorder at Fighting Hill was destroyed in a landslide)

Southern Sites:
1697011 Ring Drain – stage only   --> no flow record present, so ok.
69641 Opihi at Waipopo (flood flows only) --> no flow record present, so ok
69802 Washdyke Creek at SH1 – initially flood flows only but now fully rated (from 2020) --> put in secondary site if it has "flow recorder" records and add comment
69614 Opuha at Skipton – not rated between 01/07/1999 and 11/05/2011 (flood flows only) --> put in secondary site if it has "flow recorder" records and add comment
69616 South Opuha (ECS) – low flow only – not accurate at all about 10m3/s --> put in "secondary" site if it has "flow recorder" records and add comment
69661 Opuha at downstream weir (ECS) – low flow only --> put in "secondary" site if it has "flow recorder" records and add comment
69650 Opihi at Saleyards (ECS) – low flow only – not accurate about 20 m3/s --> put in "secondary" site if it has "flow recorder" records and add comment
************************************************************************************************************

Sites are eventually labeled as primary, secondary, and other. They are classified using the conditions below under each label.

Primary:
    - Flow / water level sites only
    - Recorder data >= the year 2020
    - >= 5 years of record
Secondary:
    - Flow / water level sites only
    - Recorder data < the year 2020 & >= 5 years of record
    - Recorder data >= the year 2020 & <= 5 years of record
    - Manual
    - All sites in hydstra_secondary_flow_sites
Other:
    - All rainfall sites
    - All abstraction sites



"""

pd.options.display.max_columns = 100

# Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Environment Canterbury'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ = 'May 2021'
############################################################################################

# CSV file to write output to
outF = r'C:\Active\Projects\Essential_Freshwater\data\USM\USM_site_data_summary_v2.csv'

# CSV file with Hydstra site summary to read
hydstra_csv = r'C:\Active\Projects\Essential_Freshwater\data\USM\hydstra_site_summary_filtered.csv'

# Server and database for the USM (contains all our unique sites)
USM_server = 'sql02prod'
USM_db = 'USM'
# USM table containing all the sites
USM_table = 'Site'
# USM table containing source systems
USM_src_table = 'SourceSystem'
# # USM site table identifying unique site number
# USM_site_id_field = 'UpstreamSiteID'
# # Columns that are needed from this table (Active column is not copied because all sites seem to be active which is weird)
# USM_site_columns = [USM_site_id_field, 'Name', 'NZTMX', 'NZTMY', 'SourceSystem']

# Server, hydro database and tables for the abstraction data
site_data_server = 'edwprod01'
site_data_db = 'Hydro'
# Water quantity daily data summary table
sw_quantity_summ_day = 'TSDataNumericDailySumm'
# Water quantity dataset types table and view thereof
sw_quantity_dt = 'DatasetType'
sw_quantity_dt_view = 'vDatasetTypeNamesAll'
# Water quantity measurement types table
sw_quantity_mt = 'MeasurementType'


# Hydstra sites to drop because they only used for low flow indicators and should therefore not be used for anything.
# This list also includes sites that should be dropped because they have been replaced with a different recorder number containing the entire period (e.g. 164606).
hydstra_drop_sites = [66, 2312, 2314, 2316, 1693065, 164606]
# Sites that should only be used as water level sites. I.e. flow records are dropped for these sites
hydstra_wl_only_sites = [66214, 66404, 68003, 68502, 1697011, 69641]
# Sites that should be marked as a secondary site because the flow records are unreliable under certain conditions. A comment is added to the results.
hydstra_secondary_flow_sites = {69802: 'Initially only used for flood flows. Fully rated since 2020',
                                69614: 'Flood flows only. Not rated between 01/07/1999 and 11/05/2011',
                                69616: 'Low flow only. Not accurate at all about 10 m3/s',
                                69661: 'Low flow only',
                                69650: 'Low flow only. Not accurate about 20 m3/s'
                                }

# Processing is done below

# Get all the USM site details
USM_site_df = mssql.rd_sql(USM_server, USM_db, USM_table, col_names=['UpstreamSiteID', 'SourceSystem'])
df = mssql.rd_sql(USM_server, USM_db, USM_src_table)
USM_site_df = pd.merge(USM_site_df, df, how='left', left_on='SourceSystem', right_on='ID').drop(['SourceSystem', 'ID'], axis=1)
df = None
# Drop the Hilltop sites because this is covered by other team members, and drop Hydstra stuff as that is read in from csv-file. So only abstraction (SW or GW) sites are kept.
USM_site_df = USM_site_df.loc[(USM_site_df.SystemName != 'Hilltop') & (USM_site_df.SystemName != 'Hydstra')]

# Get the water abstraction summary data
sw_quantity_summ_data = mssql.rd_sql(site_data_server, site_data_db, sw_quantity_summ_day, col_names=['ExtSiteID', 'DatasetTypeID', 'FromDate', 'ToDate'], where_in={'DatasetTypeID': [9, 12]})
sw_quantity_summ_data = sw_quantity_summ_data.loc[sw_quantity_summ_data.ExtSiteID.isin(pd.unique(USM_site_df.UpstreamSiteID).tolist())]
sw_quantity_summ_data['FromDate'] = pd.to_datetime(sw_quantity_summ_data['FromDate'])
sw_quantity_summ_data['ToDate'] = pd.to_datetime(sw_quantity_summ_data['ToDate'])

# Merge USM sites with surface water quantity summary
df = pd.merge(USM_site_df, sw_quantity_summ_data, how='left', left_on='UpstreamSiteID', right_on='ExtSiteID').drop('ExtSiteID', axis=1)
# Keep only the sites that actually have abstraction data
df = df.loc[pd.notna(df.DatasetTypeID)]
sw_quantity_summ_data = None; del sw_quantity_summ_data

# Get datasettypes information
df_dtypes = mssql.rd_sql(site_data_server, site_data_db, sw_quantity_dt_view, col_names=['DatasetTypeID', 'Feature', 'MeasurementType', 'CollectionType', 'DataCode', 'DataProvider'], where_in={'DatasetTypeID': pd.unique(df.DatasetTypeID).tolist()})
df_mtypes1 = mssql.rd_sql(site_data_server, site_data_db, sw_quantity_dt, col_names=['DatasetTypeID', 'MTypeID'], where_in={'DatasetTypeID': pd.unique(df.DatasetTypeID).tolist()})
df_mtypes2 = mssql.rd_sql(site_data_server, site_data_db, sw_quantity_mt, col_names=['MTypeID', 'Units'], where_in={'MTypeID': pd.unique(df_mtypes1.MTypeID).tolist()})
df_mtypes3 = pd.merge(df_mtypes1, df_mtypes2, how='left', on='MTypeID').drop('MTypeID', axis=1)
df_dtypes = pd.merge(df_dtypes, df_mtypes3, how='left', on='DatasetTypeID')
df = pd.merge(df, df_dtypes, how='left', on='DatasetTypeID')
df_dtypes = None; df_mtypes1 = None; df_mtypes2 = None; df_mtypes3 = None
df_final = df.copy().sort_values('UpstreamSiteID')
df = None; del df
df_final.drop(['DatasetTypeID', 'DataCode', 'SystemName'], axis=1, inplace=True)
df_final.loc[df_final.Units == 'm**3', 'Units'] = 'm3'
df_final.rename(columns={'UpstreamSiteID': 'Site'}, inplace=True)
df_final['Feature'] = df_final['Feature'].str.lower()
df_final['MeasurementType'] = df_final['MeasurementType'].str.lower()
df_final['CollectionType'] = df_final['CollectionType'].str.lower()

# Read the csv-file with the hydstra data
hydstra_df = pd.read_csv(hydstra_csv, parse_dates=[1, 2], dayfirst=True)

# Drop sites
hydstra_df = hydstra_df.loc[~hydstra_df.Site.isin(hydstra_drop_sites)]
hydstra_df = hydstra_df.loc[~((hydstra_df.Site.isin(hydstra_wl_only_sites)) & (hydstra_df.MeasurementType == 'flow') & (hydstra_df.CollectionType == 'recorder'))]
# Drop records with recorded river water level if it has recorded flows and the site is not part of the list hydstra_secondary_flow_sites
df1 = hydstra_df.loc[(hydstra_df.Feature == 'river') & ((hydstra_df.MeasurementType == 'flow') | (hydstra_df.MeasurementType == 'water level')) & (hydstra_df.CollectionType == 'recorder') & (~hydstra_df.Site.isin(list(hydstra_secondary_flow_sites.keys())))]
df2 = hydstra_df.loc[~((hydstra_df.Feature == 'river') & ((hydstra_df.MeasurementType == 'flow') | (hydstra_df.MeasurementType == 'water level')) & (hydstra_df.CollectionType == 'recorder') & (~hydstra_df.Site.isin(list(hydstra_secondary_flow_sites.keys()))))]
df1.sort_values(by=['Site', 'MeasurementType'], inplace=True)
df1 = df1.groupby(['Site']).first().reset_index()
hydstra_df = pd.concat([df1, df2])
df1 = None; df2 = None; del df1, df2

# Kick out manual water level for rivers
hydstra_df = hydstra_df.loc[~((hydstra_df.Feature == 'river') & (hydstra_df.MeasurementType == 'water level') & (hydstra_df.CollectionType == 'manual'))]

# Drop records for manual river flow sites if they also have a recorder
df1 = hydstra_df.loc[(hydstra_df.Feature == 'river') & (hydstra_df.MeasurementType == 'flow') & ((hydstra_df.CollectionType == 'recorder') | (hydstra_df.CollectionType == 'manual'))]
df2 = hydstra_df.loc[~((hydstra_df.Feature == 'river') & (hydstra_df.MeasurementType == 'flow') & ((hydstra_df.CollectionType == 'recorder') | (hydstra_df.CollectionType == 'manual')))]
df1.sort_values(by=['Site', 'CollectionType'], ascending=[True, True], inplace=True)
# keep recorder if it has two entries
df1 = df1.groupby(['Site']).last().reset_index()
hydstra_df = pd.concat([df1, df2])
df1 = None; df2 = None; del df1, df2

# Add comment column to mark hydstra sites
hydstra_df['Comment'] = np.nan
for k in hydstra_secondary_flow_sites.keys():
    v = hydstra_secondary_flow_sites[k]
    hydstra_df.loc[(hydstra_df.Site == k) & (hydstra_df.MeasurementType == 'flow') & (hydstra_df.CollectionType == 'recorder'), 'Comment'] = v

# Concat hydstra data df with abstraction data df
df_final['Comment'] = np.nan
df_final = pd.concat([df_final, hydstra_df])

# Add site names
df_final.Site = df_final.Site.astype(str)
s = pd.unique(df_final.Site).tolist()
USM_site_df = mssql.rd_sql(USM_server, USM_db, USM_table, col_names=['UpstreamSiteID', 'Name'])
USM_site_df.rename(columns={'UpstreamSiteID': 'Site'}, inplace=True)
USM_site_df = USM_site_df.loc[USM_site_df.Site.isin(s)]
USM_site_df.loc[USM_site_df.Name == 'Missing Name', 'Name'] = np.nan
df_final = pd.merge(df_final, USM_site_df, how='left', on='Site')
df_final.insert(1, 'name', df_final.Name)
df_final.drop('Name', axis=1, inplace=True)
df_final.rename(columns={'name': 'Name'}, inplace=True)
df_final.to_csv(outF, index=False)

# Remove duplicate sites for abstractions. Keep river sites if one site has both aquifer as well as river. Technically there can only be one.
df1 = df_final.loc[df_final.MeasurementType == 'abstraction']
df2 = df_final.loc[df_final.MeasurementType != 'abstraction']
df_new = pd.DataFrame(columns=df1.columns)
for j in pd.unique(df1.Site).tolist():
    df = df1.loc[df1.Site == j]
    if len(df) > 1:
        # Keep min of both dates and max of both dates to capture the full length of record for the site
        fromDate = df.FromDate.min()
        toDate = df.ToDate.max()
        df['FromDate'] = fromDate
        df['ToDate'] = toDate
        # if it has both river and aquifer, then keep river as the only record for that site. Otherwise it is aquifer.
        try:
            df = df.loc[df.Feature == 'river']
        except:
            df = df.loc[df.Feature == 'aquifer']
        df.drop_duplicates(inplace=True)

    df_new = pd.concat([df_new, df])
df_final = pd.concat([df_new, df2])
df_new = None; df1 = None; df2 = None; del df1, df2, df_new


# Label the sites to primary, secondary, and other and write to csv-file
df_final['Primary, Secondary, or Other'] = np.nan
df_final['Rec length'] = (df_final['ToDate'] - df_final['FromDate']) / np.timedelta64(1, 'Y')
df_final.loc[df_final.MeasurementType.isin(['precipitation', 'abstraction']), 'Primary, Secondary, or Other'] = 'other'
df_final.loc[(df_final.MeasurementType.isin(['flow', 'water level'])) & (df_final['Rec length'] >= 5) & (df_final.ToDate.dt.year >= 2020) & (df_final.CollectionType == 'recorder'), 'Primary, Secondary, or Other'] = 'primary'
df_final.loc[pd.isna(df_final['Primary, Secondary, or Other']), 'Primary, Secondary, or Other'] = 'secondary'
df_final.loc[df_final.Site.isin(list(hydstra_secondary_flow_sites.keys())), 'Primary, Secondary, or Other'] = 'secondary'
df_final.to_csv(outF, index=False)

