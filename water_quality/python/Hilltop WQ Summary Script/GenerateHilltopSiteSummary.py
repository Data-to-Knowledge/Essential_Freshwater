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
import datetime

# Set URL
base_url = 'http://wateruse.ecan.govt.nz'

# Set server hts file (Server hts name, not Hilltop01 hts name!)
# WQGroundwater.hts = \Hilltop01\Data\WQGroundwaterCombined.dsn
hts = 'WQAll.hts'

'''
For sample distribution by water year, choose whether to count all samples,
days sampled (doesn't count duplicates but fails to count samples at different times of day),
months sampled, or quarters sampled
'''
wateryear_count = 'months' # options = 'samples', 'days', 'months', 'quarters'

# Set earliest year to start distribution. SQ30641 has sample from 1916.
first_year = 1900

# Choose whether to sort by calendar or water year, WY (WY2021 = 1 July 2020 to 30 June 2021)
year_choice = 'water year' # 'water year' or 'calendar'

##############################################################################

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
sample_parameter_results = []

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
    # Try SQ21274
    except UnboundLocalError:
        continue
    # Try calling 'WQ Sample' to get full list of sample parameters
    data = ws.get_data(base_url,hts,site,'WQ Sample',from_date='1001-01-01',to_date='9999-01-01').unstack('Parameter')
    data.columns = data.columns.droplevel()
    sample_parameter_results.append(data)
    

# Concatenate summary lists into pandas dataframes
site_measurement_summary_df = pd.concat(site_measurement_summary,sort = False)
site_sample_parameter_summary_df = pd.concat(site_sample_parameter_summary,sort = False)
sample_parameter_results_df = pd.concat(sample_parameter_results,sort = False)
# Export measurement and sample parameter summaries to csv files
site_measurement_summary_df.to_csv('Site Measurement Summary.csv')
site_sample_parameter_summary_df.to_csv('Site Sample Parameter Summary.csv')
sample_parameter_results_df.to_csv('Sample Parameter Results.csv')

# Generate the list of sites with measurements
sites_with_measurements = site_measurement_summary_df.index.unique(0).tolist()

# Generate the list of sites without measurements
sites_without_measurements = [site for site in hts_sites_list if site not in sites_with_measurements]
# Obtain location data for sites without measurements
sites_without_measurements_df = hts_sites_df[hts_sites_df['SiteName'].isin(sites_without_measurements)]
# Export table of sites without measurements to csv file
sites_without_measurements_df.to_csv('Sites in hts file without measurements.csv',index=False)

# Call each site/measurement pair to determine number of measurements for each site
# Initialise empty list
site_measurement_counts = []

# Determine current water/calendar year
if datetime.datetime.now().month >= 7 and year_choice == 'water year':
    current_year = datetime.datetime.now().year + 1
else:
    current_year = datetime.datetime.now().year

# Loop through each site that has measurements
for site in site_measurement_summary_df.index.unique(0).tolist():
    print(site)
    # Loop through each measurement listed at the site
    for measurement in site_measurement_summary_df.loc[site].index.tolist():
        # Record site measurement to which sample count data is to be appended
        counts = [site,measurement]
        # WQ Sample pulls sample metadata, counts are calculated as unique dates listed
        if measurement == 'WQ Sample':
            data = ws.get_data(base_url,hts,site,measurement,from_date='1001-01-01',to_date='9999-01-01')
            sample_dates = data.index.unique(level=2)
            # Error with SQ21274 not having WQ Sample info
            if sample_dates.empty == True:
                counts += ['']*6
                for year in range(first_year,current_year + 1):
                    counts.append(0)
            else:
                counts += [min(sample_dates),max(sample_dates),len(sample_dates),'','','']
                # Obtain sample distribution by calendar/water year
                for year in range(first_year,current_year + 1):
                    if year_choice == 'calendar':
                        start_date = '{}-1-1'.format(year)
                        end_date = '{}-12-31 23:59'.format(year)
                    elif year_choice == 'water year':
                        start_date = '{}-7-1'.format(year-1)
                        end_date = '{}-6-30 23:59'.format(year)
                    annual_dates = sample_dates[(sample_dates >= start_date) & (sample_dates <= end_date)]
                    if wateryear_count == 'samples':
                        counts.append(len(annual_dates))
                    elif wateryear_count == 'days':
                        counts.append(len(set(annual_dates.dayofyear)))
                    elif wateryear_count == 'months':
                        counts.append(len(set(annual_dates.month)))
                    elif wateryear_count == 'quarters':
                        counts.append(len(set(annual_dates.quarter)))
            site_measurement_counts.append(counts)
            continue
        # Call the data for the specified site and measurement
        code = 'OK'
        data = 0.0
        while code == 'OK' and type(data) == float:    
            try:
                data = ws.get_data(base_url,hts,site,measurement,from_date='1001-01-01',to_date='9999-01-01')
            # Some measurements have no data (ie. BX23/0035 - Benzo[a]anthracene)
            except ValueError:
                code = 'ValueError'
                data = pd.DataFrame()
            except:
                continue
        # Check if data exists for site and measurement
        if data.empty:
            counts += ([None]*2+[0])*2 + [None]*(current_year-first_year)
        else:
            # Format data to filter by date
            measurement_dates = data.index.get_level_values(2)
            # Record the min, max, and count of dates with samples
            counts += [min(measurement_dates),
                       max(measurement_dates),
                       len(measurement_dates)]
            # Remove results that are entered as *
            data_actual = data[data.Value != '*']
            # Check that actual data exists
            if data_actual.empty:
                # Measurement only contains * values (ie. BU24/0002 - Filtration, Unpreserved)
                counts += [None]*2+[0] + [None]*(current_year-first_year)
            else:
                measurement_dates = data_actual.index.get_level_values(2)
                # Record the min, max, and count of dates with non-* samples
                counts += [min(measurement_dates),
                           max(measurement_dates),
                           len(measurement_dates)]
                for year in range(first_year,current_year + 1):
                    if year_choice == 'calendar':
                        start_date = '{}-1-1'.format(year)
                        end_date = '{}-12-31 23:59'.format(year)
                    elif year_choice == 'water year':
                        start_date = '{}-7-1'.format(year-1)
                        end_date = '{}-6-30 23:59'.format(year)
                    annual_dates = measurement_dates[(measurement_dates >= start_date) & (measurement_dates <= end_date)]
                    if wateryear_count == 'samples':
                        counts.append(len(annual_dates))
                    elif wateryear_count == 'days':
                        counts.append(len(set(annual_dates.dayofyear)))
                    elif wateryear_count == 'months':
                        counts.append(len(set(annual_dates.month)))
                    elif wateryear_count == 'quarters':
                        counts.append(len(set(annual_dates.quarter)))
        # Append the site-measurement sample count data to list
        site_measurement_counts.append(counts)

# Name columns
cols=['Site','Measurement','From(Check)','To(Check)','Sample Count',
         'Actual From','Actual To','Actual Sample Count']
for year in range(first_year,current_year + 1):
    if year_choice ==  'calendar':
        cols.append('Y{}'.format(year))
    elif year_choice == 'water year':
        cols.append('WY{}'.format(year))

# Convert data to a dateframe
site_measurement_counts_df = pd.DataFrame(site_measurement_counts,columns=cols)
# Set sample count dataframe index to match format of the site measurement summary dataframe
site_measurement_counts_df = site_measurement_counts_df.set_index(['Site','Measurement'])

# Merge the sample count dataframe to site measurement summary dataframe
merged_summary_df = pd.merge(site_measurement_summary_df,site_measurement_counts_df,how = 'left',on = ['Site','Measurement'])
#Export site measurement summary data with sample count data
merged_summary_df.to_csv('Site Measurement Summary-Sample Counts.csv')
