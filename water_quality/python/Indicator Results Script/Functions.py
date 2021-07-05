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
    
    # Determine the Month and day for each sample. Note that due to the use of
    # hydro years, a custom day of year value is used since leap years cause
    # 1 july and 30 June to be the same day of the year.
    df['Month'] = df['DateTime'].dt.month
    df['Day'] = df['DateTime'].dt.month*31 + df['DateTime'].dt.day
    
    # Obtain daily values by taking median of samples collected in a day
    df = Hazen_percentile(df,50,['Site','HydroYear','Day'],'Censor','Numeric','DayCensor','DayNumeric')
    # Drop unnecessary columns and duplicates
    df = df.drop(columns=['DateTime','Observation','Censor','Numeric']).drop_duplicates()
    
    # Obtain monthly values by taking median of samples collected within a month
    df = Hazen_percentile(df,50,['Site','HydroYear','Month'],'DayCensor','DayNumeric','MonthCensor','MonthNumeric')
    # Drop unnecessary columns and duplicates
    df = df.drop(columns=['Day','DayCensor','DayNumeric']).drop_duplicates()
    
    return df

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
