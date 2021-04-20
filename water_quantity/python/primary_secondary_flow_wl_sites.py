# -*- coding: utf-8 -*-

import pandas as pd


"""
Reads USM site data summary csv-file and filters out primary and secondary flow and water level sites for rivers and lakes (water level only).

- Primary sites are Recorder sites that have "Primary" as DataCode, and have data for 2020 and/or later.
- Secondary sites are Manual sites with "Primary" as DataCode, OR recorder sites with "Correlated" as DataCode, with both having data for 2020 and/or later.
"""

pd.options.display.max_columns = 100

# Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Environment Canterbury'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ = 'April 2021'
############################################################################################

# csv-file with the USM site summary to read
csvF = r'C:\Active\Projects\Essential_Freshwater\data\USM\USM_site_data_summary.csv'

# csv-file to write primary and secondary sites to
csvOut = r'C:\Active\Projects\Essential_Freshwater\data\USM\flow_wl_primary_secondary_sites.csv'


### Processing is done below

df = pd.read_csv(csvF, parse_dates=[5, 6], dayfirst=True)
flow_wl_df = df.loc[(df.MeasurementType == 'Flow') | (df.MeasurementType == 'Water Level')]


primary_df = flow_wl_df.loc[(flow_wl_df.CollectionType == 'Recorder') & (flow_wl_df.DataCode == 'Primary') & ((flow_wl_df['Date for 2020 and later'] == 'YES') | (flow_wl_df['Data prior to 2020, 2020, and later'] == 'YES'))]
primary_df = primary_df[['UpstreamSiteID', 'Name', 'FromDate', 'ToDate', 'Feature', 'MeasurementType']]
primary_df['Primary or Secondary'] = 'Primary'


secondary_df = flow_wl_df.loc[~((flow_wl_df.CollectionType == 'Recorder') & (flow_wl_df.DataCode == 'Primary'))]
secondary_df = secondary_df.loc[(secondary_df['Date for 2020 and later'] == 'YES') | (secondary_df['Data prior to 2020, 2020, and later'] == 'YES')]
secondary_df = secondary_df[['UpstreamSiteID', 'Name', 'FromDate', 'ToDate', 'Feature', 'MeasurementType']]
secondary_df['Primary or Secondary'] = 'Secondary'

all_df = pd.concat([primary_df, secondary_df])

all_df.to_csv(csvOut, index=False)



