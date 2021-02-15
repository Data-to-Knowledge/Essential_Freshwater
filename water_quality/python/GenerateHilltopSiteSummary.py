# -*- coding: utf-8 -*-
"""
Python Script to generate summary .csv files of Hilltop .hts files

- Sites within the file
- Sites within the file that have no water quality data
- Site and Sample metadata timeframe summary
- Site and measurement timeframe summary
- Site and measurement timeframe summary with sample counts

Created on Fri Jan 22 09:08:13 2021

@author: KurtV
"""

# import python modules
from hilltoppy import web_service as ws
import pandas as pd

# Set URL
base_url = 'http://wateruse.ecan.govt.nz'

# Set server hts file (Server hts name, not Hilltop01 hts name!)
# WQGroundwater.hts = \Hilltop01\Data\WQGroundwaterCombined.dsn
hts = 'WQAll.hts'

# Generate a dataframe of all sites in the server file with location data
hts_sites_df = ws.site_list(base_url,hts,location=True)
# Export hts sites table to csv file
hts_sites_df.to_csv('Sites in hts file.csv',index=False)

# Generate a list of all sites in the server file
hts_sites_list = sorted(hts_sites_df.SiteName.tolist(),key=str.lower)

# Generate a measurement summary and sample parameter summary for all sites
# Initialise empty lists
site_measurement_summary = []
site_sample_parameter_summary = []

# Loop through all sites in the hts file
for site in hts_sites_list:
    # Call site-specific measurement list
    measurement_summary = ws.measurement_list(base_url,hts,site)
    # Append list to measurement summary table
    site_measurement_summary.append(measurement_summary)
    # Try calling the site-specific sample parameter list
    try:
        sample_parameter_summary = ws.wq_sample_parameter_list(base_url,hts,site)
        # Append list to sample parameter summary table
        site_sample_parameter_summary.append(sample_parameter_summary)
    # Some sites have measurements but no sample parameters
    except ValueError:
        continue

# Concatenate summary lists into pandas dataframes
site_measurement_summary_df = pd.concat(site_measurement_summary,sort = False)
site_sample_parameter_summary_df = pd.concat(site_sample_parameter_summary,sort = False)
# Export measurement and sample parameter summaries to csv files
site_measurement_summary_df.to_csv('Site Measurement Summary.csv')
site_sample_parameter_summary_df.to_csv('Site Sample Parameter Summary.csv')

# Generate the list of sites with measurements
sites_with_measurements = site_measurement_summary_df.index.unique(0).tolist()

# Generate the list of sites without measurements
sites_without_measurements = [site for site in hts_sites_list if site not in sites_with_measurements]
# Obtain location data for sites without measurements
sites_without_measurements_df = hts_sites_df[hts_sites_df['SiteName'].isin(sites_without_measurements)]
# Export table of sites without measurements to csv file
sites_without_measurements_df.to_csv('Sites in hts file without measurements.csv',index=False)

# Drop measurement 'WQ Sample' before looping through site/measurement pairs
site_measurement_df = site_measurement_summary_df.drop(index='WQ Sample',level=1)

# Call each site/measurement pair to determine number of measurements for each site
# Initialise empty list
site_measurement_counts = []
# Loop through each site that has measurements
for site in site_measurement_df.index.unique(0).tolist():
    # Loop through each measurement listed at the site
    for measurement in site_measurement_df.loc[site].index.tolist():
        # Record site measurement to which sample count data is to be appended
        counts = [site,measurement]
        # Aluminium, Total results cannot be extracted with python - skip
        # See BY20/0150 Measurement Parameters or try to view table in Manager
        if measurement == 'Aluminium, Total':
            counts += ['Unknown']*6
            site_measurement_counts.append(counts)
            continue
        # Call the data for the specified site and measurement
        try:
            data = ws.get_data(base_url,hts,site,measurement,from_date='1001-01-01',to_date='9999-01-01')
        # Some measurements have no data (ie. BX23/0035 - Benzo[a]anthracene)
        except ValueError:
            data = pd.DataFrame()
        # Check if data exists for site and measurement
        if data.empty:
            counts += [None]*6
        else:
            # Format data to filter by date
            data_all = data.reset_index(level=2)
            # Record the min, max, and count of dates with samples
            counts += [min(data_all['DateTime'].tolist()),
                       max(data_all['DateTime'].tolist()),
                       len(data_all['DateTime'].tolist())]
            # Remove results that are entered as *
            data_actual = data_all[data_all.Value != '*']
            # Check that actual data exists
            if data_actual.empty:
                # Measurement only contains * values (ie. BU24/0002 - Filtration, Unpreserved)
                counts += [None]*3
            else:
                # Record the min, max, and count of dates with non-* samples
                counts += [min(data_actual['DateTime'].tolist()),
                           max(data_actual['DateTime'].tolist()),
                           len(data_actual['DateTime'].tolist())]
        # Append the site-measurement sample count data to list
        site_measurement_counts.append(counts)

# Convert data to a dateframe
site_measurement_counts_df = pd.DataFrame(site_measurement_counts,
                                         columns=['Site','Measurement',
                                                  'From(Check)','To(Check)','Sample Count',
                                                  'Actual From','Actual To',
                                                  'Actual Sample Count'])
# Set sample count dataframe index to match format of the site measurement summary dataframe
site_measurement_counts_df = site_measurement_counts_df.set_index(['Site','Measurement'])

# Merge the sample count dataframe to site measurement summary dataframe
merged_summary_df = pd.merge(site_measurement_summary_df,site_measurement_counts_df,how = 'left',on = ['Site','Measurement'])
#Export site measurement summary data with sample count data
merged_summary_df.to_csv('Site Measurement Summary-Sample Counts.csv')
