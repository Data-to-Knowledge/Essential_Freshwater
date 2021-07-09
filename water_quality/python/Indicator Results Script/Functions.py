# -*- coding: utf-8 -*-
"""
Python Script with common indicator functions for GW and SW

Created on Fri Jun 6 09:08:13 2021

@author: KurtV
"""

# import python modules
from hilltoppy import web_service as ws
import pandas as pd
import numpy as np
import math
import pymannkendall as mk
from scipy import stats

def hilltop_data(base_url, hts, sites, measurements):
    """
    Function to query a Hilltop server for the measurement summary of selected
    sites and measurement in an hts file.

    Parameters
    ----------
    base_url : str
        root url str. e.g. http://wateruse.ecan.govt.nz
    hts : str
        hts file name including the .hts extension.
    sites : list of str
        list of sites to pull from the hts file
    measurements : list of str
        list of measurements to pull from the selected sites

    Returns
    -------
    DataFrame
        indexed by Site and DateTime
    """
    '''
    Format WQ table to match view in Hilltop Manager on site basis
    '''
    # Initiate empty list of dataframes
    WQData = []
    #Extract Nitrate Nitrogen and E.coli data with measurement and sample parameters
    for site in sites:
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

    return WQData_df

def stacked_data(df, measurements, units_dict):
    """
    Function to transform Hilltop view of dataframe to stacked and filtered
    dataframe

    Parameters
    ----------
    df : DataFrame
        dataframe in the format output from hilltop_data()
    measurements : list of str
        list of measurements to pull from the selected sites
    units_dict : dictionary
        dictionary matching measurements to units stored in Hilltop
    
    Returns
    -------
    DataFrame
        dataframe of measurement results that can be used in indicator stats
    """
    
    # Set dataframe structure
    StatsData_df = pd.DataFrame(columns=['Site','Measurement','Units','HydroYear','DateTime','Observation','Censor','Numeric'])
    
    for measurement in measurements:
        MeasurementData = df[measurement,'({})'.format(units_dict[measurement])]
        MeasurementData.name = 'Observation'
        # Drop NaN values and * values
        MeasurementData = MeasurementData[MeasurementData != '*'].dropna()
        MeasurementData_df = MeasurementData.to_frame().reset_index()
        MeasurementData_df['Measurement'] = measurement
        MeasurementData_df['Units'] = units_dict[measurement]
        StatsData_df = StatsData_df.append(MeasurementData_df)
    
    # Create HydroYear column from year and month
    StatsData_df['HydroYear'] = np.where(StatsData_df.DateTime.dt.month <= 6,
                                                     StatsData_df.DateTime.dt.year,
                                                     StatsData_df.DateTime.dt.year+1)
    
    # Split censor component from numeric component of observation
    StatsData_df['Censor'] = np.where(StatsData_df['Observation'].str.startswith(tuple(['<','>'])),StatsData_df['Observation'].str[0],None)
    StatsData_df['Numeric'] = StatsData_df['Observation'].map(lambda x: x.replace('<','').replace('>',''))
    StatsData_df['Numeric'] = pd.to_numeric(StatsData_df['Numeric'])

    return StatsData_df

def sample_freq(df,semiannual):
    """
    Function to estimate data collection frequency for each hydro year
    and measurement

    Parameters
    ----------
    df : DataFrame
        dataframe in the format output from stacked_data()
    semiannual : True/False
        Set whether to consider semiannual frequency as an option or not in
        addition to monthly, quarterly, and annual frequency
    
    Returns
    -------
    DataFrame
        dataframe of measurement results with estimated sampling frequency
    """
    
   # Initiate sample frequency dataframe
    Frequency_df = df.copy()
    # Add semester, quarter, month, and day columns
    if semiannual:
        Frequency_df['Semester'] = np.where(Frequency_df['HydroYear']==Frequency_df['DateTime'].dt.year,2,1)
    Frequency_df['Quarter'] = Frequency_df['DateTime'].dt.quarter
    Frequency_df['Month'] = Frequency_df['DateTime'].dt.month
    # Leap year can cause 1 July and 30 June to have the same dayofyear integer
    # A new index is created to prevent this.
    Frequency_df['Day'] = Frequency_df['DateTime'].dt.month*31 + Frequency_df['DateTime'].dt.day
    # Add sample count column for each hydro year
    Samples = Frequency_df.groupby(['Site','Measurement','HydroYear']).agg({"DateTime": "nunique"}).rename(columns={'DateTime':'Samples'})
    DaysSampled = Frequency_df.groupby(['Site','Measurement','HydroYear']).agg({"Day": "nunique"}).rename(columns={'Day':'DaysSampled'})
    MonthsSampled = Frequency_df.groupby(['Site','Measurement','HydroYear']).agg({"Month": "nunique"}).rename(columns={'Month':'MonthsSampled'})
    QuartersSampled = Frequency_df.groupby(['Site','Measurement','HydroYear']).agg({"Quarter": "nunique"}).rename(columns={'Quarter':'QuartersSampled'})
    if semiannual:
        SemestersSampled = Frequency_df.groupby(['Site','Measurement','HydroYear']).agg({"Semester": "nunique"}).rename(columns={'Semester':'SemestersSampled'})
    Frequency_df = pd.merge(Samples,DaysSampled,on=['Site','Measurement','HydroYear'],how='outer')
    Frequency_df = pd.merge(Frequency_df,MonthsSampled,on=['Site','Measurement','HydroYear'],how='outer')
    Frequency_df = pd.merge(Frequency_df,QuartersSampled,on=['Site','Measurement','HydroYear'],how='outer')
    if semiannual:
        Frequency_df = pd.merge(Frequency_df,SemestersSampled,on=['Site','Measurement','HydroYear'],how='outer')
    # Set rules for estimating sampling frequency
    Frequency_df['Frequency'] = np.where(Frequency_df['QuartersSampled']==1,'A',None)
    Frequency_df['Frequency'] = np.where(Frequency_df['QuartersSampled']==2,'Q',Frequency_df['Frequency'])
    if semiannual:
        Frequency_df['Frequency'] = np.where(Frequency_df['SemestersSampled']==2,'S',Frequency_df['Frequency'])
    Frequency_df['Frequency'] = np.where(Frequency_df['QuartersSampled']>2,'Q',Frequency_df['Frequency'])
    Frequency_df['Frequency'] = np.where(Frequency_df['MonthsSampled']>=6,'M',Frequency_df['Frequency'])

    
    return Frequency_df

def round_half_up(n, decimals=0):
    out = n
    if ~np.isnan(out):
        multiplier = 10 ** decimals
        out = math.floor(n*multiplier + 0.5) / multiplier
    return out

def sort_censors(df,censor,numeric,ascending):
    '''
    Function to sort a dataframe using a column of values which may be censored.
    This function sorts in a way where censored values are placed as high
    as their range allows.
    
    Parameters
    ----------
    df : DataFrame
        dataframe to sort
    column : str
        column to use to sort the dataframe
    ascending : True/False
        if True, least to greatest. if False, greatest to least.
    
    Returns
    -------
    Dataframe
        Sorted by column
    '''
    
    # Rank censors for sorting
    df['CensorRank1'] = df[censor].map({'>':2,None:1,'<':1})
    df['CensorRank2'] = df[censor].map({'>':100,None:10,'<':1})
    # Sort by '>' vs other, then by numeric component, then by None vs '<'
    df = df.sort_values(by=['CensorRank1',numeric,'CensorRank2'],ascending=ascending)
    df = df.drop(columns=['CensorRank1','CensorRank2'])
    
    return df

def Hazen_percentile(df,percentile,group_columns,censor_column_in,numeric_column_in,censor_column_out,numeric_column_out):
    """
    Function to calculate percentile or medians

    Parameters
    ----------
    df : DataFrame
        dataframe
    percentile : float
        percentile to calculate (i.e., 95 or 50 for median)
    groupby : list of str
        list of columns to groupby data by
    censor_column_in : str
        censor component input (<, >, or None)
    numeric_column_in : float
        numeric component input
    censor_column_out : str
        censor component input (<, >, or None)
    numeric_column_out : float
        numeric component input
    
    
    Returns
    -------
    DataFrame
        dataframe of Hazen percentile results grouped and joined to original table
    """
    
    # Calculate required sample size for percentile
    if percentile >= 50:
        required = int(np.ceil(100/(2*(100-percentile))))
    elif percentile < 50:
        required = int(np.ceil(100/(2*percentile)))
    
    # Filter measurement and drop DateTime column
    hazen_df = df.copy()
    # Sort dataframe from least to greatest
    hazen_df = sort_censors(hazen_df, censor_column_in, numeric_column_in, ascending=True)
    # Rank values when grouped by chosen columns from largest to smallest
    hazen_df['Rank'] = hazen_df.groupby(group_columns).cumcount()+1
    # Define the number of values within the groups and join to dataframe
    hazen_df = pd.merge(hazen_df,hazen_df.groupby(group_columns)['Rank'].max().rename('Samples'),on=group_columns,how='outer')
    # Determine Hazen Rank to be used to calculate the percentile
    # Ensure the minimum numbered of samples required is satisfied
    hazen_df['HazenRank'] = np.where(hazen_df['Samples']>=required,0.5+percentile/100*hazen_df['Samples'],np.nan)
    # Set numerical values for censors
    hazen_df['CensorRank'] = hazen_df[censor_column_in].map({'>':100,None:10,'<':1})
    
    # Determine the contribution of each value towards the percentile
    # if Hazen rank is an integer, then percentile is the ranked value
    hazen_df['CensorRankContribution'] = np.where(hazen_df['HazenRank']==hazen_df['Rank'],hazen_df['CensorRank'],np.nan)
    hazen_df['NumericContribution'] = np.where(hazen_df['HazenRank']==hazen_df['Rank'],hazen_df[numeric_column_in],np.nan)
    # if Hazen Rank is a decimal, then percentile is combination of two values
    # Define the contribution from the lower rank value
    hazen_df['CensorRankContribution'] = np.where((((hazen_df['HazenRank']-hazen_df['Rank'])>0) & ((hazen_df['HazenRank']-hazen_df['Rank'])<1)),hazen_df['CensorRank'],hazen_df['CensorRankContribution'])
    hazen_df['NumericContribution'] = np.where((((hazen_df['HazenRank']-hazen_df['Rank'])>0) & ((hazen_df['HazenRank']-hazen_df['Rank'])<1)),(1-(hazen_df['HazenRank']-hazen_df['Rank']))*hazen_df[numeric_column_in],hazen_df['NumericContribution'])
    # Define the contribution from the higher rank value
    hazen_df['CensorRankContribution'] = np.where((((hazen_df['Rank']-hazen_df['HazenRank'])>0) & ((hazen_df['Rank']-hazen_df['HazenRank'])<1)),hazen_df['CensorRank'],hazen_df['CensorRankContribution'])
    hazen_df['NumericContribution'] = np.where((((hazen_df['Rank']-hazen_df['HazenRank'])>0) & ((hazen_df['Rank']-hazen_df['HazenRank'])<1)),(hazen_df['Rank']-hazen_df['HazenRank'])*hazen_df[numeric_column_in],hazen_df['NumericContribution'])
    # Sum the contributing values to create the Hazen percentile
    hazen_df = pd.merge(hazen_df,hazen_df.groupby(group_columns).sum(min_count=1)[['CensorRankContribution','NumericContribution']].rename(columns={'CensorRankContribution':'CensorRankOut','NumericContribution':numeric_column_out}),on=group_columns,how='outer')
    # Return appropriate censor value based on censor rank or sum of censor ranks from contributing values
    hazen_df[censor_column_out] = hazen_df['CensorRankOut'].map({200:'>',110:'>',101:'Error',100:'>',20:None,11:'<',10:None,2:'<',1:'<'})
    # Drop extraneous columnes
    hazen_df = hazen_df.drop(columns=['CensorRank','Rank','Samples','HazenRank','CensorRankContribution','NumericContribution','CensorRankOut'])
    
    return hazen_df

def reduce_to_monthly(df):
    '''
    Function to reduce DateTime sample results to monthly values, which is the
    most frequent monitoring regime for most ECan programmes.
    First take median of results taken on the same day (this ensures that duplicate
    samples taken for NEMS are combined first and do not bias outputs). Then
    take the median of all daily results to obtain a monthly result.
    
    Parameters
    ----------
    df : DataFrame
        dataframe should include columns as output by stacked_data
    
    Returns
    -------
    Dataframe
        With monthly results
    '''
    
    # Determine the Month and day for each sample in the given hydro year.
    # Note that due to the use of hydro years, a custom day of year value is
    # used since leap years cause 1 july and 30 June to be the same day of the year.
    df['Month'] = (df['DateTime'].dt.month + 6 - 1)%12 + 1
    df['Day'] = (df['DateTime'].dt.month*31 + df['DateTime'].dt.day + 31*6 - 1)%(31*12) + 1
    
    # Obtain daily values by taking median of samples collected in a day
    df = Hazen_percentile(df,50,['Site','HydroYear','Day'],'Censor','Numeric','DayCensor','DayNumeric')
    # Drop unnecessary columns and duplicates
    df = df.drop(columns=['DateTime','Observation','Censor','Numeric']).drop_duplicates()
    
    # Obtain monthly values by taking median of samples collected within a month
    df = Hazen_percentile(df,50,['Site','HydroYear','Month'],'DayCensor','DayNumeric','MonthCensor','MonthNumeric')
    # Drop unnecessary columns and duplicates
    df = df.drop(columns=['Day','DayCensor','DayNumeric']).drop_duplicates()
    # Sort by Site and hydroyear
    df = df.sort_values(by=['Site','HydroYear'],ascending=True)
    
    return df

def trend_format(df,frequency):
    '''
    Function to format monthly data into stacked dataframe with quarterly
    and annual data frequency as well. Quarterly and annual values obtained by
    taking median of monthly values within the quarter or year, respectively.
    
    Parameters
    ----------
    df : DataFrame
        dataframe should include columns as output by reduced_to_monthly
    frequency : list of str
        sampling frequency upon which to calculate trends. Should be from
        ['Annual','Quarterly','Monthly']
    
    Returns
    -------
    Dataframe
        with values assigned to a frequency and an interval within each hydro year
        at a site
    '''
    
    # Indicate the quarter for each monthly value
    df['Quarter'] = np.where(df['Month']>=10,2,
                    np.where(df['Month']>=7,1,
                    np.where(df['Month']>=4,4,3)))
    # Obtain quarterly values by taking median of monthly values collected within a quarter
    df = Hazen_percentile(df,50,['Site','HydroYear','Quarter'],'MonthCensor','MonthNumeric','QuarterCensor','QuarterNumeric')
    # Set interval for annual data (always 1 since 1 year in each hydroyear)
    df['Year'] = 1
    # Obtain annual values by taking median of monthly values collected within a year
    df = Hazen_percentile(df,50,['Site','HydroYear'],'MonthCensor','MonthNumeric','YearCensor','YearNumeric')
    # Set index such that the interval number, censor, and numeric components can be stacked
    df = df.set_index(['Site','Measurement','Units','HydroYear'])
    # Check that the ordering is as such before replacing with multiindex
    df = df[['Month','MonthCensor','MonthNumeric','Quarter','QuarterCensor','QuarterNumeric','Year','YearCensor','YearNumeric']]
    # Create multiindex columns in prep for stacking
    df.columns = pd.MultiIndex.from_tuples(list(zip(*[['Monthly']*3+['Quarterly']*3+['Annual']*3,['Interval','Censor','Numeric']*3])),names=['Frequency','Value'])
    # Stack columns
    df = df.stack(level=0)
    # Reset index and remove duplicates
    df = df.reset_index().drop_duplicates()
    # Only keep desired sampling frequency results
    df = df[df['Frequency'].isin(frequency)]
    # Convert result to a string
    df['Result'] = df['Censor'].fillna('')+df['Numeric'].astype(str)
    # Sort by Site and hydroyear
    df = df.sort_values(by=['Site','Frequency','HydroYear'],ascending=True)
    
    return df

def trends(df,trend_periods=[5,10,15,20],final_year=[2021],requirement=0.80):
    '''
    Function to calculate trend analyses on a dataset. Can only handle
    Monthly, Quarterly, and Annual sampling frequency
    
    Parameters
    ----------
    df : DataFrame
        dataframe should include columns as output by trends_format()
    trend_periods : list of int
        List of trend periods to calculate for
    final_year : list of int
        List of hydroyears to calculate trend results for
    requirements : float
        Percentage of expected intervals represented by values in the trend period
    
    Returns
    -------
    Dataframe
        with trends results output and an estimated value for 2035 utilising slope and intercept
    '''
    
    # Set maximum hydro year. No trend results should be calculated for years past this
    year_max = df['HydroYear'].max()
    # Create empty list to append results
    TrendResults = []
    # Cycle through sites
    for site in df['Site'].unique():
        # Reduce dataset to be specfic to the chosern site
        site_data = df[df['Site']==site]
        # Cycle through the desired trend lengths
        for trend_period in trend_periods:
            # Cycle through the desired trend year. Note that the final year should never be greater than the current hydroyear
            # and should not be considered if there has been no data collected from the site in the desired trend period
            for year in set(final_year).intersection(set([x+i for i in range(trend_period) for x in site_data['HydroYear'].unique() if x+i<=year_max])):
                # Cycle through the different sampling frequencies that exist in the trend data
                for frequency in site_data.Frequency.unique():
                    # Only consider data with the chosen frequency and within the trend period
                    trend_data = site_data[(site_data['Frequency']==frequency)&(site_data['HydroYear']<=year)&(site_data['HydroYear']>(year-trend_period))][['HydroYear','Interval','Censor','Numeric']].copy()
                    # Count the number of intervals that are represented by data
                    count = len(trend_data)
                    # Determine if there is enough data to run the trend analysis
                    if (frequency == 'Monthly') & (count/(12*trend_period) < requirement):
                        continue
                    elif (frequency == 'Quarterly') & (count/(4*trend_period) < requirement):
                        continue
                    elif (frequency == 'Annual') & (count/trend_period < requirement):
                        continue
                    # If requirements are loose enough, ensure no trends are run if count is 2  or less
                    if count <= 2:
                        continue
                    # Determine maximum detection limit and minimum quantification limit in the data
                    max_DL = trend_data[trend_data['Censor']=='<']['Numeric'].max()
                    min_QL = trend_data[trend_data['Censor']=='>']['Numeric'].min()
                    # Convert data below the maximum detection limit
                    if ~np.isnan(max_DL):
                        trend_data['Numeric'] = np.where((trend_data['Censor']=='<')|(trend_data['Numeric']<max_DL),0.5*max_DL,trend_data['Numeric'])
                    # Convert data above the minimum quantification limit
                    if ~np.isnan(min_QL):
                        trend_data['Numeric'] = np.where((trend_data['Censor']=='>')|(trend_data['Numeric']>min_QL),1.1*min_QL,trend_data['Numeric'])
                    # Make the HydroYear categorical
                    trend_data.HydroYear = pd.Categorical(trend_data.HydroYear,categories=[(year-trend_period+1)+i for i in range(trend_period)])
                    # Make the Interval column categorical and set the trend line start data as the middle of first interval
                    if frequency == 'Monthly':
                        StartDate = pd.to_datetime(str(year-trend_period)+'0715')
                        trend_data.Interval = pd.Categorical(trend_data.Interval,categories=[i+1 for i in range(12)])
                    elif frequency == 'Quarterly':
                        StartDate = pd.to_datetime(str(year-trend_period)+'0815')
                        trend_data.Interval = pd.Categorical(trend_data.Interval,categories=[i+1 for i in range(4)])
                    elif frequency == 'Annual':
                        StartDate = pd.to_datetime(str(year-trend_period+1)+'0101')
                    # Set each hydroyear and interval to have a value (np.nan for intervals without data)
                    trend_data = trend_data.groupby(['HydroYear','Interval'])['Numeric'].first()
                    # If annual frequency, don't run seasonal test
                    if frequency == 'Annual':
                        KWp = np.nan
                    else:
                        # See if data frequency allows for seasonality test
                        try:
                            if frequency == 'Monthly':
                                KW = stats.kruskal(*[trend_data.loc[:,month].tolist() for month in range(1,13)],nan_policy='omit')
                            elif frequency == 'Quarterly':
                                KW = stats.kruskal(*[trend_data.loc[:,quarter].tolist() for quarter in range(1,5)],nan_policy='omit')
                            KWp = KW.pvalue
                        # Otherwise run test as non-seasonal
                        except ValueError:
                            KWp = np.nan
                    # Determine seasonality from seasonal test pvalues
                    if pd.isna(KWp):
                        seasonality = 'Cannot assess - treated as non-seasonal'
                    elif KWp <= 0.05:
                        seasonality = 'Seasonal'
                    else:
                        seasonality = 'Non-seasonal'
                    # Use seasonality to determine which Mann-Kendall test to perform
                    if seasonality == 'Seasonal':
                        if frequency == 'Monthly':
                            MK = mk.seasonal_test(trend_data,period=12)
                        elif frequency == 'Quarterly':
                            MK = mk.seasonal_test(trend_data,period=4)
                        TheilSlope = MK.slope
                    else:
                        MK = mk.original_test(trend_data)
                        # If non-seasonal test used, multiply slope result by number of intervals within a year
                        if frequency == 'Monthly':
                            TheilSlope = MK.slope*12
                        elif frequency == 'Quarterly':
                            TheilSlope = MK.slope*4
                        elif frequency == 'Annual':
                            TheilSlope = MK.slope
                    # Convert Mann-Kendall analysis results to a liklihood that the trend is decreasing
                    if MK.s <= 0:
                        Likelihood = 1 - 0.5*MK.p
                    elif MK.s > 0:
                        Likelihood = 0.5*MK.p
                    # Convert the likelihood of a decreasing trend to a trend category
                    if Likelihood >= 0.90:
                        TrendResult = 'Very Likely Decreasing'
                    elif Likelihood >= 0.67:
                        TrendResult = 'Likely Decreasing'
                    elif Likelihood > 0.33:
                        TrendResult = 'Indeterminate'
                    elif Likelihood > 0.10:
                        TrendResult = 'Likely Increasing'
                    elif Likelihood >= 0.0:
                        TrendResult = 'Very Likely Increasing'
                    # Report relevant data into a list and append to trend results list
                    row_data = [site,year,trend_period,frequency,count,max_DL,min_QL,KWp,seasonality,MK.p,MK.z,MK.Tau,MK.s,MK.var_s,Likelihood,TrendResult,StartDate,MK.intercept,TheilSlope,pd.to_datetime('2035'),MK.intercept+TheilSlope*(pd.to_datetime('2035')-StartDate).days/365.25]
                    TrendResults.append(row_data)
    # Create DataFrame from results
    Results_df = pd.DataFrame(TrendResults,columns=['Site','HydroYear','TrendLength','DataFrequency','Intervals','MaxDetectionLimit','MinQuantLimit','Seasonal_pvalue','Seasonality','MK_pvalue','MK_Zscore','MK_Tau','MK_S','MK_VarS','DecreasingLikelihood','TrendCategory','TrendLineStartDate','TrendLineStartValue','Slope','TrendLineEndDate','TrendLineEndValue'])
    # Sort values by hydroyear, site, trend length, and data frequency
    Results_df = Results_df.sort_values(by=['HydroYear','Site','TrendLength','DataFrequency'],ascending=True)
    
    return Results_df



def annual_max(df):
    '''
    Function to obtain the annual maximum for each site and hydroyear.
    
    Parameters
    ----------
    df : DataFrame
        dataframe should include columns as output by stacked_data
    
    Returns
    -------
    Dataframe
        With sample values reduced to annual maximum values for each site/hydroyear
    '''
    
    # Sort values from largest to smallest using censor and numeric components
    max_df = sort_censors(df,'Censor','Numeric',ascending=False)
    # Count number of samples collected in Hydroyear
    max_df = pd.merge(max_df,max_df.groupby(['Site','HydroYear']).size().rename('SamplesOrIntervals'),on=['Site','HydroYear'],how='outer')
    # Keep maximum value for each hydro year
    max_df = max_df.drop_duplicates(subset=['Site','HydroYear'],keep='first')
    # Rename Observation column to be Result column and drop DateTime
    max_df = max_df.rename(columns={'Observation':'Result'}).drop(columns=['DateTime'])
    # Sort by Site and hydroyear
    max_df = max_df.sort_values(by=['Site','HydroYear'],ascending=True)
    
    return max_df

def annual_percentile(df,percentile):
    '''
    Function to obtain the percentile for each site and hydroyear from monthly data.
    
    Parameters
    ----------
    df : DataFrame
        dataframe should include columns as output by stacked_data
    percentile : flt
        Hazen-percentile to calculate
    
    Returns
    -------
    Dataframe
        With annual percentile values for each site/hydroyear
    '''
    
    # Obtain annual values by taking median of monthly values collected within a year
    df = Hazen_percentile(df,percentile,['Site','HydroYear'],'MonthCensor','MonthNumeric','AnnualCensor','AnnualNumeric')
    # Count the number of months represented by the data
    df = pd.merge(df,df.groupby(['Site','HydroYear']).size().rename('Months'),on=['Site','HydroYear'],how='outer')
    # Drop rows that don't meet the Hazen percentile requirements (result=nan)
    df = df.dropna(subset=['AnnualNumeric'])
    # Drop unneeded columns
    df = df.drop(columns=['Month','MonthCensor','MonthNumeric']).drop_duplicates()
    # Rename columns to fit indicator dataframe strcuture
    df = df.rename(columns={'Months':'SamplesOrIntervals','AnnualNumeric':'Numeric','AnnualCensor':'Censor'})
    # Sort by Site and hydroyear
    df = df.sort_values(by=['Site','HydroYear'],ascending=True)
    
    return df

def multiyear_percentile(df,percentile,years,frequency,requirements):
    '''
    Function to obtain the percentile for each site and hydroyear from monthly data.
    
    Parameters
    ----------
    df : DataFrame
        dataframe should include columns as output by stacked_data
    percentile : flt
        Hazen-percentile to calculate
    years : int
        number of years over which to include data for percentile
    frequency : list of str
        list of frequencies to allow for percentile
        - Annual, Semi-annual, Quarterly, Monthly
    requirements : list of int
        list of same length as frequency describing the minimum number of
        intervals needed to consider indicator result as valid
    
    Returns
    -------
    Dataframe
        With percentile values for each site/hydroyear
    '''
    
    # Add Semester and Quarter indicator relative to hydro year period
    df['Semester'] = np.where(df['Month']>=7,1,2)
    df['Quarter'] = np.where(df['Month']>=10,2,
                               np.where(df['Month']>=7,1,
                            np.where(df['Month']>=4,4,3)))
    # Every hydro year of data is used in the next 'years' hydroyears for the multiyear percentile
    # Create column to indicate which years should be included in a given hydroyear's indicator result
    df['IndicatorYear'] = df['HydroYear']
    tempdata = df.copy()
    # Repeat rows years-1 times and add 1,2,3... to IndicatorYear column
    for i in range(years-1):
        tempdata['IndicatorYear'] += 1
        df = df.append(tempdata,ignore_index=True)
    # Drop rows where IndicatorYear is larger than the largest HydroYear
    df = df[df['IndicatorYear']<=df['HydroYear'].max()]
    # Obtain quarterly values by taking median of monthly values collected within a quarter
    df = Hazen_percentile(df,50,['Site','HydroYear','Quarter'],'MonthCensor','MonthNumeric','QuarterCensor','QuarterNumeric')
    # Obtain semi-annual values by taking median of monthly values collected within a half year
    df = Hazen_percentile(df,50,['Site','HydroYear','Semester'],'MonthCensor','MonthNumeric','SemesterCensor','SemesterNumeric')
    # Obtain annual values by taking median of monthly values collected within a year
    df = Hazen_percentile(df,50,['Site','HydroYear'],'MonthCensor','MonthNumeric','AnnualCensor','AnnualNumeric')
    
    # Count months and calculate the multiyear percentile based on monthly values
    df = pd.merge(df,df.groupby(['Site','IndicatorYear']).size().rename('Months'),on=['Site','IndicatorYear'],how='outer')
    df = Hazen_percentile(df,percentile,['Site','IndicatorYear'],'MonthCensor','MonthNumeric','MonthsCensor','MonthsNumeric')
    df = df.drop(columns=['Month','MonthCensor','MonthNumeric']).drop_duplicates()
    # Count quarters and calculate the 5-year median based on quarterly values
    df = pd.merge(df,df.groupby(['Site','IndicatorYear']).size().rename('Quarters'),on=['Site','IndicatorYear'],how='outer')
    df = Hazen_percentile(df,percentile,['Site','IndicatorYear'],'QuarterCensor','QuarterNumeric','QuartersCensor','QuartersNumeric')
    df = df.drop(columns=['Quarter','QuarterCensor','QuarterNumeric']).drop_duplicates()
    # Count semesters and calculate the 5-year median based on semi-annual values
    df = pd.merge(df,df.groupby(['Site','IndicatorYear']).size().rename('Semesters'),on=['Site','IndicatorYear'],how='outer')
    df = Hazen_percentile(df,percentile,['Site','IndicatorYear'],'SemesterCensor','SemesterNumeric','SemestersCensor','SemestersNumeric')
    df = df.drop(columns=['Semester','SemesterCensor','SemesterNumeric']).drop_duplicates()
    # Count years and calculate the 5-year median based on annual values
    df = pd.merge(df,df.groupby(['Site','IndicatorYear']).size().rename('Years'),on=['Site','IndicatorYear'],how='outer')
    df = Hazen_percentile(df,percentile,['Site','IndicatorYear'],'AnnualCensor','AnnualNumeric','YearsCensor','YearsNumeric')
    df = df.drop(columns=['HydroYear','AnnualCensor','AnnualNumeric']).drop_duplicates().rename(columns={'IndicatorYear':'HydroYear'})
    
    # Use months, quarters, semesters, and years to determine sampling frequency
    # that will be used in the percentile calculation by comparing to required
    # data thresholds. Ordered to generate finest scale frequency when reqs met
    
    # Set base frequency to None
    df['Frequency'] = None
    for freq in [['Annual','Years'],['Semi-annual','Semesters'],['Quarterly','Quarters'],['Monthly','Months']]:
        if freq[0] in frequency:
            df['Frequency'] = np.where(df[freq[1]] >= requirements[frequency.index(freq[0])],freq[0],df['Frequency'])
    # Choose the appropriately calculated median based on the sampling frequency
    df[['Censor','Numeric','SamplesOrIntervals']] = \
        np.where(np.transpose(np.asarray([df['Frequency'] == 'Monthly']*3)),
                 df[['MonthsCensor','MonthsNumeric','Months']],
        np.where(np.transpose(np.asarray([df['Frequency'] == 'Quarterly']*3)),
                 df[['QuartersCensor','QuartersNumeric','Quarters']],
        np.where(np.transpose(np.asarray([df['Frequency'] == 'Semi-annual']*3)),
                 df[['SemestersCensor','SemestersNumeric','Semesters']],
        np.where(np.transpose(np.asarray([df['Frequency'] == 'Annual']*3)),
                 df[['YearsCensor','YearsNumeric','Years']],
                 [None,np.nan,np.nan]))))
    # Drop unnecessary columns and years that don't have enough data for calculation from annual data
    df = df.drop(columns=['MonthsCensor','MonthsNumeric','Months','QuartersCensor','QuartersNumeric','Quarters','SemestersCensor','SemestersNumeric','Semesters','YearsCensor','YearsNumeric','Years',]).dropna(subset=['Frequency'])
    # Sort by Site and hydroyear
    df = df.sort_values(by=['Site','HydroYear'],ascending=True)
    
    return df

def grades(df,bins):
    '''
    Function to set indicator grades
    
    Parameters
    ----------
    df : DataFrame
        dataframe should include columns as output by stacked_data
    bins : list of float
        set of values that should be used to separate grades into A,B,C,D,etc.
    
    Returns
    -------
    Dataframe
        With results graded
    '''
    
    # Set grades based on number of items in bins
    # Start with A and move through alphabet
    grade_options = [chr(i+65) for i in range(len(bins)-1)]
    # Use bins values to generate grade ranges
    grade_range = ['{}-{}'.format(bins[0],bins[1])]+['>{}-{}'.format(bins[i],bins[i+1]) for i in range(1,len(bins)-2)]+['>{}'.format(bins[-2])]
    # Set grades and ranges based on the numeric component column
    df['Grade'] = pd.cut(df['Numeric'],bins,labels=grade_options,include_lowest=True)
    df['GradeRange'] = pd.cut(df['Numeric'],bins,labels=grade_range,include_lowest=True)
    
    return df

def grade_check(df,data_df,bins,frequency):
    '''
    Function to check where median censored values are graded below A and
    if there are lower ranked detections that clarify whether a unique grade
    can be determiend. For example, if the median result is <3 and grade A
    cutoff is 2, then a 2.5 value is ranked below <3 and gaurantees a grade of B,
    otherwise a combo grade is used A/B.
    
    Parameters
    ----------
    df : DataFrame
        dataframe should include columns as output by stacked_data
    data_df : DataFrame
        dataframe where the values used to calculate the result are saved
    bins : list of float
        set of values that should be used to separate grades into A,B,C,D,etc.
    frequency : str
        declare frequency of sampling to call appropriate columns
    
    Returns
    -------
    Dataframe
        With grades reworked to include uncertain results
    '''
    
    # Set grades based on number of items in bins
    # Start with A and move through alphabet
    grades = [chr(i+65) for i in range(len(bins)-1)]
    # Add combinations of grades and ranges to the categorical columns
    new_grades = []
    new_ranges = []
    # Loop through number of grades combined: 2 to number of grades
    for j in range(2,len(bins)):
        # Loop through starting points
        for k in range(len(bins)-j):
            # Separate grades with /
            new_grades.append('/'.join(grades[k:k+j]))
            # Create ranges from values
            new_ranges.append('>{}-{}'.format(bins[k],bins[k+j]))
    # Ensure that first bin value is included in ranges
    new_ranges = list(map(lambda s: str.replace(s,'>{}-'.format(bins[0]),'{}-'.format(bins[0])),new_ranges))
    # Remove any range components that end at inf
    new_ranges = list(map(lambda s: str.replace(s,'-inf',''),new_ranges))
    # If inf is last bound, replace bin[0] with >= bin[0]
    if new_ranges[-1] == '{}'.format(bins[0]):
        new_ranges[-1] = '>={}'.format(bins[0])
    # Add new grades and range to categorical columns
    df['Grade'].cat.add_categories(new_categories=new_grades,inplace=True)
    df['GradeRange'].cat.add_categories(new_categories=new_ranges,inplace=True)
    # Reset index for use in locating censored results outside of grade A
    df = df.reset_index(drop=True)
    
    # Find where result is censored and not A grade
    for i in df[(df['Grade']!='A')&(df['Censor']=='<')].index:
        # Find largest detected value below censor level, if any
        if frequency == 'Monthly':
            detect_below_median = data_df[(data_df['Site']==df.iloc[i]['Site'])&
                     (data_df['HydroYear']==df.iloc[i]['HydroYear'])&
                     (data_df['MonthNumeric'] < df.iloc[i]['Numeric'])&
                     (data_df['MonthCensor']!='<')]['MonthNumeric'].max()
        elif frequency == 'All':
            detect_below_median = data_df[(data_df['Site']==df.iloc[i]['Site'])&
                     (data_df['HydroYear']==df.iloc[i]['HydroYear'])&
                     (data_df['Numeric'] < df.iloc[i]['Numeric'])&
                     (data_df['Censor']!='<')]['Numeric'].max()
        # If there is no detection below the median censored result or if the
        # largest detection is grade A, assign grade A/B/... to censor grade
        if np.isnan(detect_below_median) or detect_below_median <= bins[1]:
            df.at[i,'Grade'] = list(filter(lambda k: k[-1] == df.iloc[i]['Grade'], new_grades))[-1]
            df.at[i,'GradeRange'] = new_ranges[new_grades.index(df.iloc[i]['Grade'])]
        # Check if highest detection below median is same grade as censored median
        elif grades[len([x for x in bins if x <= detect_below_median])-1] == df.iloc[i]['Grade']:
            continue
        # Otherwise, set grade to be range between highest detect below median grade and censor grade
        else:
            df.at[i,'Grade'] = list(filter(lambda k: ((k[0] == grades[len([x for x in bins if x <= detect_below_median])-1])&(k[-1] == df.iloc[i]['Grade'])), new_grades))[0]
            df.at[i,'GradeRange'] = new_ranges[new_grades.index(df.iloc[i]['Grade'])]
            
    return df