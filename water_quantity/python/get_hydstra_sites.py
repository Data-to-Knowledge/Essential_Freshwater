# -*- coding: utf-8 -*-

from pyhydllp import hyd
import pandas as pd
from pdsql import mssql
import numpy as np


pd.options.display.max_columns = 100

# Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Environment Canterbury'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ = 'May 2021'
############################################################################################

"""
Extracts Hydstra site info for all flow sites, water level sites (rivers and lakes), and rainfall sites.
Both recorder as well as manual (non-recorder) sites are extracted, and only quality codes 10, 18, and 20 are extracted.

Filtering is done for longest period of record, and best quality codes; e.g. if a flow site with quality code 10 has a longer
record than the same flow site with quality code 20, then the records with code 20 are kicked out. Similarly, 18
is compared with 20, and 10 with 18.

FromDate and ToDate are adjusted to the best record; e.g. if a recorder flow site runs from 1 January 2000 through 31 March 2021,
and there are manual flow gaugings for the same site spanning the period 6 June 2003 through 30 November 2019, then those records
are kicked out as the recorder has a longer dataset of a higher frequency. Similarly, if the manual flow gaugings start before
the recorder starts, then the ToDate for the manual gaugings is set to the day before the recorder data starts, etc.

*************************************************************************************************************************
Info about Hydstra VARFROM and VARTO data and DATASOURCE

VAR:
100    = water level [m]
140    = stream discharge [m3/s]
143    = stream discharge [l/s]
130    = lake level [m]
10     = rainfall [mm]

DATASOURCE:
A        = archive
TELEM    = telemetry
GF       = gauged flow
GH       = gauged water level

Possible combinations of VAR and DATASOURCE:

VARFROM    VARTO    DATASOURCE    MEANING
100        100      A             Recorded archived water level [m]
100        140      A             Calculated flow using recorded archived water level [m3/s]
143        143      A             Archived NIWA flow data [l/s]
140        140      A             Archived NIWA flow data [m3/s]
100        100      GH            Spot gauged water level [m]
140        140      GF            Spot gauged flow [m3/s]

10         10       A             Recorded archived rainfall [mm]
130        130      A             Recorded archived lake level [m]
**************************************************************************************************************************
"""

# csv file to write summary table of all hydstra flow, water level (lakes & rivers), and rainfall sites to. Only qualitycodes 10, 18, and 20 are selected.
hydstra_site_summary_csv = r'C:\Active\Projects\Essential_Freshwater\data\USM\hydstra_site_summary.csv'
# csv file to write filtered summary table of table above to. Filtered means highest qualitycodes are kept if they have overlapping periods.
# Also, if recorder data period is longer than manual data period, only recorder period is kept.
hydstra_site_summary_filtered_csv = r'C:\Active\Projects\Essential_Freshwater\data\USM\hydstra_site_summary_filtered.csv'

# Only filter these quality codes
qual_codes = [10, 20, 18]

# Hydstra configuration settings
ini_path = r'\\hydstraprod01\hydsys\hydstra\prod\hyd'
dll_path = r'\\hydstraprod01\hydsys\hydstra\prod\hyd\sys\run'
username = 'SQLJOB'    # replace this with your own username
password = 'Mar2021'   # replace this with your own password
hydllp_filename = 'hydllp.dll'
hyaccess_filename = 'Hyaccess.ini'
hyconfig_filename = 'HYCONFIG.INI'

# Server and database settings for the USM (contains all our unique sites)
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


##### PROCESSING BELOW

# Instance of hyd class
hyd1 = hyd(ini_path, dll_path, hydllp_filename=hydllp_filename,
           hyaccess_filename=hyaccess_filename, hyconfig_filename=hyconfig_filename,
           username=username, password=password)

# Dictionary with unique Hydstra combinations to pull out. Key values are [VARFROM, VARTO, DATASOURCE, DATATYPE, FEATURE, MEASUREMENTTYPE, COLLECTIONTYPE, DATAPROVIDER, UNITS]
hyd_dict = {'river_wl_record_ecan': [100, 100, 'A', 'mean', 'river', 'water level', 'recorder', 'ECan', 'm'],
            'river_flow_record_ecan': [100, 140, 'A', 'mean', 'river', 'flow', 'recorder', 'ECan', 'm3/s'],
            'river_flow_record_niwa': [140, 140, 'A', 'mean', 'river', 'flow', 'recorder', 'NIWA', 'm3/s'],
            'river_flow_record_niwa_ls': [143, 143, 'A', 'mean', 'river', 'flow', 'recorder', 'NIWA', 'l/s'],
            'river_wl_manual_ecan': [100, 100, 'GH', 'point', 'river', 'water level', 'manual', 'ECan', 'm'],
            'river_flow_manual_ecan': [140, 140, 'GF', 'point', 'river', 'flow', 'manual', 'ECan', 'm3/s'],
            'rain_record': [10, 10, 'A', 'mean', 'atmosphere', 'precipitation', 'recorder', 'ECan', 'mm'],
            'lake_wl_record': [130, 130, 'A', 'mean', 'lake', 'water level', 'recorder', 'ECan', 'm']
            }

# Get Hydstra site numbers
USM_site_df = mssql.rd_sql(USM_server, USM_db, USM_table, col_names = USM_site_columns)
USM_site_df.loc[USM_site_df.Name == 'Missing Name', 'Name'] = np.nan  # some names are missing and are classified as Missing Name. These are set to NaN.
df = mssql.rd_sql(USM_server, USM_db, USM_src_table)
USM_site_df = pd.merge(USM_site_df, df, how = 'left', left_on = 'SourceSystem', right_on = 'ID').drop(['SourceSystem', 'ID'], axis=1)
# Only keep the Hydstra sites
USM_site_df = USM_site_df.loc[USM_site_df.SystemName == 'Hydstra']
df = None

# unique Hydstra sites
sites = pd.unique(USM_site_df.UpstreamSiteID).tolist()

# Empty dataframe to fill with the site info
df_final = pd.DataFrame(columns=['Site', 'FromDate', 'ToDate', 'Feature', 'MeasurementType', 'CollectionType', 'DataProvider', 'Units', 'QualityCode'])
i = 0
for s in sites:
    for k in hyd_dict.keys():
        print(s, k)
        try:
            # try whether the requested data exists for the site
            df = hyd1.get_ts_data(sites=[s], varfrom=hyd_dict[k][0], varto=hyd_dict[k][1], datasource=hyd_dict[k][2], data_type=hyd_dict[k][3], qual_codes=qual_codes).reset_index()
            minDate = df.time.min()
            maxDate = df.time.max()
            unique_qual_codes = pd.unique(df.qual_code)
            for c in unique_qual_codes:
                i += 1
                df_sel = df.loc[df.qual_code == c]
                df_final.loc[i, 'Site'] = s
                df_final.loc[i, 'FromDate'] = df_sel.time.dt.date.min()
                df_final.loc[i, 'ToDate'] = df_sel.time.dt.date.max()
                df_final.loc[i, 'Feature'] = hyd_dict[k][4]
                df_final.loc[i, 'MeasurementType'] = hyd_dict[k][5]
                df_final.loc[i, 'CollectionType'] = hyd_dict[k][6]
                df_final.loc[i, 'DataProvider'] = hyd_dict[k][7]
                df_final.loc[i, 'Units'] = hyd_dict[k][8]
                df_final.loc[i, 'QualityCode'] = c
        except:
            pass

df_final.to_csv(hydstra_site_summary_csv, index=False)

# Get rid of lower quality codes if the higher quality codes spans an overlapping longer period
df_final.sort_values(by=['Site', 'Feature', 'MeasurementType', 'CollectionType', 'DataProvider', 'Units', 'QualityCode'],
                     ascending=True, inplace=True)
df_new = pd.DataFrame(columns=df_final.columns)
# Create unique records to loop over and check for quality codes and period of length
df_unique = df_final.drop_duplicates(subset=['Site', 'Feature', 'MeasurementType', 'DataProvider', 'Units'])
for index, col in df_unique.iterrows():
    df_sel = df_final.loc[(df_final.Site == col['Site']) & (df_final.Feature == col['Feature']) & (df_final.MeasurementType == col['MeasurementType'])
                & (df_final.DataProvider == col['DataProvider']) & (df_final.Units == col['Units'])].copy()
    # If length > 1, then it has multiple quality codes for the same site and type of data, and/or there is both recorder as well as manual data.
    # In this case we only want to keep the best quality codes, and recorder data instead of manual data if possible.
    if len(df_sel) > 1:
        # Check if there's a recorder, a manual gauging, or both
        ctypes = pd.unique(df_sel.CollectionType).tolist()
        for c in ctypes:
            # check the qcodes for each collectiontype and keep the best code for each collection type for the longest record
            df_sel2 = df_sel.loc[df_sel.CollectionType == c]
            df = df_sel2.copy()
            # check if there's more than one quality code
            qcodes = pd.unique(df_sel2.QualityCode).tolist()
            if (10 in qcodes) and (20 in qcodes) and (18 in qcodes):
                fromDate_10 = df_sel2.loc[df_sel2.QualityCode == 10, 'FromDate'].iloc[0]
                toDate_10 = df_sel2.loc[df_sel2.QualityCode == 10, 'ToDate'].iloc[0]
                fromDate_20 = df_sel2.loc[df_sel2.QualityCode == 20, 'FromDate'].iloc[0]
                toDate_20 = df_sel2.loc[df_sel2.QualityCode == 20, 'ToDate'].iloc[0]
                fromDate_18 = df_sel2.loc[df_sel2.QualityCode == 18, 'FromDate'].iloc[0]
                toDate_18 = df_sel2.loc[df_sel2.QualityCode == 18, 'ToDate'].iloc[0]
                # Compare 10 vs 20
                # If higher quality record is longer and starts before and ends after lower quality code record, then kick out lower quality record
                if (fromDate_10 <= fromDate_20) & (toDate_10 >= toDate_20):
                    df = df.loc[df.QualityCode != 20]
                # If lower quality record starts before higher quality record, but higher quality record runs till later date, then set todate of lower quality record to fromdate - 1 of higher quality record
                elif (fromDate_10 > fromDate_20) & (toDate_10 >= toDate_20):
                    df.loc[df.QualityCode == 20, 'ToDate'] = np.minimum(df.loc[df.QualityCode == 20, 'ToDate'].iloc[0],
                                                                        df.loc[df.QualityCode == 10, 'FromDate'].iloc[0] - pd.Timedelta(days=1))

                # if lower quality record runs till later date as higher quality record, but higher quality record starts earlier, then set start date of lower quality record to todate + 1 of higher quality record
                elif (fromDate_10 <= fromDate_20) & (toDate_10 < toDate_20):
                    df.loc[df.QualityCode == 20, 'FromDate'] = np.maximum(df.loc[df.QualityCode == 20, 'FromDate'].iloc[0],
                                                                          df.loc[df.QualityCode == 10, 'ToDate'].iloc[0] + pd.Timedelta(days=1))
                # Compare 10 vs 18
                # If higher quality record is longer and starts before and ends after lower quality code record, then kick out lower quality record
                if (fromDate_10 <= fromDate_18) & (toDate_10 >= toDate_18):
                    df = df.loc[df.QualityCode != 18]
                # If lower quality record starts before higher quality record, but higher quality record runs till later date, then set todate of lower quality record to fromdate - 1 of higher quality record
                elif (fromDate_10 > fromDate_18) & (toDate_10 >= toDate_18):
                    df.loc[df.QualityCode == 18, 'ToDate'] = np.minimum(df.loc[df.QualityCode == 18, 'ToDate'].iloc[0],
                                                                        df.loc[df.QualityCode == 10, 'FromDate'].iloc[0] - pd.Timedelta(days=1))
                # if lower quality record runs till later date as higher quality record, but higher quality record starts earlier, then set start date of lower quality record to todate + 1 of higher quality record
                elif (fromDate_10 <= fromDate_18) & (toDate_10 < toDate_18):
                    df.loc[df.QualityCode == 18, 'FromDate'] = np.maximum(df.loc[df.QualityCode == 18, 'FromDate'].iloc[0],
                                                                          df.loc[df.QualityCode == 10, 'ToDate'].iloc[0] + pd.Timedelta(days=1))
                # Compare 20 vs 18. A try/except is needed here because the qcode 20 or 18 might have been kicked out above and can therefore not be found anymore
                try:
                    # If higher quality record is longer and starts before and ends after lower quality code record, then kick out lower quality record
                    if (fromDate_18 <= fromDate_20) & (toDate_18 >= toDate_20):
                        df = df.loc[df.QualityCode != 20]
                    # If lower quality record starts before higher quality record, but higher quality record runs till later date, then set todate of lower quality record to fromdate - 1 of higher quality record
                    elif (fromDate_18 > fromDate_20) & (toDate_18 >= toDate_20):
                        df.loc[df.QualityCode == 20, 'ToDate'] = np.minimum(df.loc[df.QualityCode == 20, 'ToDate'].iloc[0],
                                                                            df.loc[df.QualityCode == 18, 'FromDate'].iloc[0] - pd.Timedelta(days=1))
                    # if lower quality record runs till later date as higher quality record, but higher quality record starts earlier, then set start date of lower quality record to todate + 1 of higher quality record
                    elif (fromDate_18 <= fromDate_20) & (toDate_18 < toDate_20):
                        df.loc[df.QualityCode == 20, 'FromDate'] = np.maximum(df.loc[df.QualityCode == 20, 'FromDate'].iloc[0],
                                                                              df.loc[df.QualityCode == 18, 'ToDate'].iloc[0] + pd.Timedelta(days=1))
                except:
                    pass

            elif (10 in qcodes) and (20 in qcodes):
                fromDate_10 = df_sel2.loc[df_sel2.QualityCode == 10, 'FromDate'].iloc[0]
                toDate_10 = df_sel2.loc[df_sel2.QualityCode == 10, 'ToDate'].iloc[0]
                fromDate_20 = df_sel2.loc[df_sel2.QualityCode == 20, 'FromDate'].iloc[0]
                toDate_20 = df_sel2.loc[df_sel2.QualityCode == 20, 'ToDate'].iloc[0]
                # Compare 10 vs 20
                # If higher quality record is longer and starts before and ends after lower quality code record, then kick out lower quality record
                if (fromDate_10 <= fromDate_20) & (toDate_10 >= toDate_20):
                    df = df.loc[df.QualityCode != 20]
                # If lower quality record starts before higher quality record, but higher quality record runs till later date, then set todate of lower quality record to fromdate - 1 of higher quality record
                elif (fromDate_10 > fromDate_20) & (toDate_10 >= toDate_20):
                    df.loc[df.QualityCode == 20, 'ToDate'] = np.minimum(df.loc[df.QualityCode == 20, 'ToDate'].iloc[0],
                                                                        df.loc[df.QualityCode == 10, 'FromDate'].iloc[0] - pd.Timedelta(days=1))
                # if lower quality record runs till later date as higher quality record, but higher quality record starts earlier, then set start date of lower quality record to todate + 1 of higher quality record
                elif (fromDate_10 <= fromDate_20) & (toDate_10 < toDate_20):
                    df.loc[df.QualityCode == 20, 'FromDate'] = np.maximum(df.loc[df.QualityCode == 20, 'FromDate'].iloc[0],
                                                                          df.loc[df.QualityCode == 10, 'ToDate'].iloc[0] + pd.Timedelta(days=1))

            elif (10 in qcodes) and (18 in qcodes):
                fromDate_10 = df_sel2.loc[df_sel2.QualityCode == 10, 'FromDate'].iloc[0]
                toDate_10 = df_sel2.loc[df_sel2.QualityCode == 10, 'ToDate'].iloc[0]
                fromDate_18 = df_sel2.loc[df_sel2.QualityCode == 18, 'FromDate'].iloc[0]
                toDate_18 = df_sel2.loc[df_sel2.QualityCode == 18, 'ToDate'].iloc[0]
                # Compare 10 vs 18
                # If higher quality record is longer and starts before and ends after lower quality code record, then kick out lower quality record
                if (fromDate_10 <= fromDate_18) & (toDate_10 >= toDate_18):
                    df = df.loc[df.QualityCode != 18]
                # If lower quality record starts before higher quality record, but higher quality record runs till later date, then set todate of lower quality record to fromdate - 1 of higher quality record
                elif (fromDate_10 > fromDate_18) & (toDate_10 >= toDate_18):
                    df.loc[df.QualityCode == 18, 'ToDate'] = np.minimum(df.loc[df.QualityCode == 18, 'ToDate'].iloc[0],
                                                                        df.loc[df.QualityCode == 10, 'FromDate'].iloc[0] - pd.Timedelta(days=1))
                # if lower quality record runs till later date as higher quality record, but higher quality record starts earlier, then set start date of lower quality record to todate + 1 of higher quality record
                elif (fromDate_10 <= fromDate_18) & (toDate_10 < toDate_18):
                    df.loc[df.QualityCode == 18, 'FromDate'] = np.maximum(df.loc[df.QualityCode == 18, 'FromDate'].iloc[0],
                                                                          df.loc[df.QualityCode == 10, 'ToDate'].iloc[0] + pd.Timedelta(days=1))
            elif (20 in qcodes) and (18 in qcodes):
                fromDate_20 = df_sel2.loc[df_sel2.QualityCode == 20, 'FromDate'].iloc[0]
                toDate_20 = df_sel2.loc[df_sel2.QualityCode == 20, 'ToDate'].iloc[0]
                fromDate_18 = df_sel2.loc[df_sel2.QualityCode == 18, 'FromDate'].iloc[0]
                toDate_18 = df_sel2.loc[df_sel2.QualityCode == 18, 'ToDate'].iloc[0]
                # Compare 20 vs 18
                # If higher quality record is longer and starts before and ends after lower quality code record, then kick out lower quality record
                if (fromDate_18 <= fromDate_20) & (toDate_18 >= toDate_20):
                    df = df.loc[df.QualityCode != 20]
                # If lower quality record starts before higher quality record, but higher quality record runs till later date, then set todate of lower quality record to fromdate - 1 of higher quality record
                elif (fromDate_18 > fromDate_20) & (toDate_18 >= toDate_20):
                    df.loc[df.QualityCode == 20, 'ToDate'] = np.minimum(df.loc[df.QualityCode == 20, 'ToDate'].iloc[0],
                                                                        df.loc[df.QualityCode == 18, 'FromDate'].iloc[0] - pd.Timedelta(days=1))
                # if lower quality record runs till later date as higher quality record, but higher quality record starts earlier, then set start date of lower quality record to todate + 1 of higher quality record
                elif (fromDate_18 <= fromDate_20) & (toDate_18 < toDate_20):
                    df.loc[df.QualityCode == 20, 'FromDate'] = np.maximum(df.loc[df.QualityCode == 20, 'FromDate'].iloc[0],
                                                                          df.loc[df.QualityCode == 18, 'ToDate'].iloc[0] + pd.Timedelta(days=1))
            # Concat the updated df to df_new
            df_new = pd.concat([df_new, df])

    else:
        # Concat df_sel to df_new if it has only one record
        df_new = pd.concat([df_new, df_sel])

df_final = df_new.copy()
df = None; df_sel = None; df_new = None; df_sel2
del df, df_sel, df_sel2, df_new

# Finally, check if there's recorded and manual data for the same type of data, and keep best of both worlds
df_unique = df_final.drop_duplicates(subset=['Site', 'Feature', 'MeasurementType', 'DataProvider', 'Units'])
df_new = pd.DataFrame(columns=df_final.columns)
for index, col in df_unique.iterrows():
    df_sel = df_final.loc[(df_final.Site == col['Site']) & (df_final.Feature == col['Feature']) & (df_final.MeasurementType == col['MeasurementType']) &
                          (df_final.DataProvider == col['DataProvider']) & (df_final.Units == col['Units'])].copy()
    if len(df_sel) > 1:
        df = df_sel.copy()
        ctypes = pd.unique(df_sel.CollectionType).tolist()
        if ('manual' in ctypes) and ('recorder' in ctypes):
            man_minDate = df_sel.loc[df_sel.CollectionType == 'manual', 'FromDate'].min()
            man_maxDate = df_sel.loc[df_sel.CollectionType == 'manual', 'ToDate'].max()
            rec_minDate = df_sel.loc[df_sel.CollectionType == 'recorder', 'FromDate'].min()
            rec_maxDate = df_sel.loc[df_sel.CollectionType == 'recorder', 'ToDate'].max()
            if (rec_minDate <= man_minDate) & (rec_maxDate >= man_maxDate):
                df = df.loc[df.CollectionType != 'manual']
            elif (rec_minDate > man_minDate) & (rec_maxDate >= man_maxDate):
                df.loc[df.CollectionType == 'manual', 'ToDate'] = np.minimum(df.loc[df.CollectionType == 'manual', 'ToDate'].iloc[0],
                                                                             df.loc[df.CollectionType == 'recorder', 'FromDate'].iloc[0] - pd.Timedelta(days=1))
            elif (rec_minDate <= man_minDate) & (rec_maxDate < man_maxDate):
                df.loc[df.CollectionType == 'manual', 'FromDate'] = np.maximum(df.loc[df.CollectionType == 'manual', 'FromDate'].iloc[0],
                                                                               df.loc[df.CollectionType == 'recorder', 'ToDate'].iloc[0] + pd.Timedelta(days=1))
            # The ToDate for the manual gauged flow might have become smaller than its FromDate
            try:
                minDate = df.loc[df.CollectionType == 'manual', 'FromDate'].min()
                maxDate = df.loc[df.CollectionType == 'manual', 'ToDate'].max()
                if maxDate <= minDate:
                    df.loc[df.CollectionType == 'manual', 'FromDate'] = maxDate
            except:
                pass

        df_new = pd.concat([df_new, df])
    else:
        df_new = pd.concat([df_new, df_sel])

df_final = df_new.copy()
df = None; df_sel = None; df_new = None
del df, df_sel, df_new

# Get the min and max date if there are two manual records for one site, so only one record remains containing the max period length of these two records. Similar for recorder sites.
df_group = df_final.groupby(['Site', 'Feature', 'MeasurementType', 'CollectionType', 'DataProvider', 'Units'])
df_maxdate = df_group.max().reset_index().drop(['QualityCode', 'FromDate'], axis=1)
df_mindate = df_group.min().reset_index().drop(['QualityCode', 'ToDate'], axis=1)
df_final = pd.merge(df_mindate, df_maxdate, how='left', on=['Site', 'Feature', 'MeasurementType', 'CollectionType', 'DataProvider', 'Units'])
# Re-organise columns
df_final.insert(1, 'fdate', df_final.FromDate)
df_final.insert(2, 'tdate', df_final.ToDate)
df_final.drop(['FromDate', 'ToDate'], axis=1, inplace=True)
df_final.rename(columns={'fdate': 'FromDate', 'tdate': 'ToDate'}, inplace=True)
df_final.to_csv(hydstra_site_summary_filtered_csv, index=False)
