# -*- coding: utf-8 -*-
"""
Created on Sat Mar 26 23:23:52 2022

@author: samuel ubrezi
"""

import pandas as pd
from db_setup import is_dataset_available
from datetime import datetime
from os import makedirs


def find_missing_values(dataframe):
    return dataframe.isnull().sum().loc[lambda x: x > 0].sort_values(ascending = False)

def filter_missing_values(dataframe, col):
    return dataframe[dataframe[col].isnull()]

def replace_invalid_data(dataframe, col, df_filter, new_value):
    df_new = dataframe.copy()
    df_new.loc[df_filter, col] = new_value
    
    return df_new
    
def preprocess_missing_values(dataframe):
    print(f'{datetime.now()}\tData preprocessing')
    df_new = dataframe.copy()
    missing_cols = find_missing_values(df_new)
    
    for col in missing_cols.index:
        print(f'{datetime.now()}\tData preprocessing\t{col}')
        if col == 'ZipCode':
            df_new[col] = df_new[col].fillna(0)
            
        elif col == 'City':
            # df_new[col] = df_new.apply(lambda x: f'{x["AirportCode"]} UNK' if pd.isna(x[col]) else x[col], axis = 1)
            
            df_tmp = filter_missing_values(df_new, col)
            for key in df_tmp['AirportCode'].unique():
                df_new = replace_invalid_data(df_new, 
                                              col, 
                                              df_new.index.isin(df_tmp.index) & (df_new['AirportCode'] == key), 
                                              f'{key} UNK')
        else:
            ...
    
    return df_new

def prepare_for_db_import(dataframe, cols_to_keep, cols_to_relate = None, df_to_relate = None, drop_duplicates = False):
    df_import = dataframe[cols_to_keep].copy()
    
    if drop_duplicates:
        df_import.drop_duplicates(inplace = True)
        
    if (type(None) != type(cols_to_relate)) and (type(None) != type(df_to_relate)):
        
        replace_dict = dict(zip(df_to_relate[cols_to_relate[1]], df_to_relate.index))
        # df_import.replace({cols_to_relate[0]: replace_dict}, inplace = True)
        df_import.loc[:, cols_to_relate[0]] = df_import[cols_to_relate[0]].apply(lambda x: replace_dict[x])
        
    df_import.reset_index(drop=True, inplace = True)
    return df_import.to_frame() if type(df_import) == pd.Series else df_import

def preprocess_db_import(dataframe, save_to_csv = False):
    print(f'{datetime.now()}\tPreparing data for DB import')
    df_dict = dict()
    
    print(f'{datetime.now()}\tPreparing data for DB import\tweather_type')
    df_dict['weather_type'] = prepare_for_db_import(dataframe, 
                                                    'Type', 
                                                    drop_duplicates = True)
    df_dict['weather_type'].rename(columns = {'Type':'type_name'},
                                   inplace = True)
    
    print(f'{datetime.now()}\tPreparing data for DB import\tweather_severity')
    df_dict['weather_severity'] = prepare_for_db_import(dataframe, 
                                                        'Severity', 
                                                        drop_duplicates = True)
    df_dict['weather_severity'].rename(columns = {'Severity':'severity_type'},
                                       inplace = True)
    
    print(f'{datetime.now()}\tPreparing data for DB import\tus_state')
    df_dict['us_state'] = prepare_for_db_import(dataframe, 
                                                'State', 
                                                drop_duplicates = True)
    df_dict['us_state'].rename(columns = {'State':'state_code'},
                               inplace = True)
    
    print(f'{datetime.now()}\tPreparing data for DB import\tcounty')
    df_dict['county'] = prepare_for_db_import(dataframe, 
                                              ['State','County'], 
                                              cols_to_relate = ('State','state_code'),
                                              df_to_relate = df_dict['us_state'],
                                              drop_duplicates = True)
    df_dict['county'].rename(columns = {'State': 'state_id', 
                              'County':'county_name'},
                             inplace = True)
    
    print(f'{datetime.now()}\tPreparing data for DB import\tcity')
    df_dict['city'] = prepare_for_db_import(dataframe, 
                                              ['County','City','TimeZone'], 
                                              cols_to_relate = ('County','county_name'),
                                              df_to_relate = df_dict['county'],
                                              drop_duplicates = True)
    df_dict['city'].rename(columns = {'County': 'county_id', 
                            'City':'city_name', 
                            'TimeZone': 'timezone'},
                           inplace = True)
    
    print(f'{datetime.now()}\tPreparing data for DB import\tstation')
    df_dict['station'] = prepare_for_db_import(dataframe, 
                                              ['City','AirportCode','ZipCode','LocationLat','LocationLng'], 
                                              cols_to_relate = ('City','city_name'),
                                              df_to_relate = df_dict['city'],
                                              drop_duplicates = True)
    df_dict['station'].rename(columns = {'City': 'city_id', 
                            'AirportCode': 'airport_code', 
                            'ZipCode': 'zipcode', 
                            'LocationLat': 'latitude', 
                            'LocationLng': 'longitude'},
                           inplace = True)
    
    
    print(f'{datetime.now()}\tPreparing data for DB import\tevent')
    df_dict['event'] = prepare_for_db_import(dataframe, 
                                              ['AirportCode','EventId','Type','Severity','StartTime(UTC)','EndTime(UTC)','Precipitation(in)'], 
                                              cols_to_relate = ('AirportCode','airport_code'),
                                              df_to_relate = df_dict['station'])
    df_dict['event'] = prepare_for_db_import(df_dict['event'], 
                                              ['AirportCode','EventId','Type','Severity','StartTime(UTC)','EndTime(UTC)','Precipitation(in)'], 
                                              cols_to_relate = ('Type','type_name'),
                                              df_to_relate = df_dict['weather_type'])
    df_dict['event'] = prepare_for_db_import(df_dict['event'], 
                                              ['AirportCode','EventId','Type','Severity','StartTime(UTC)','EndTime(UTC)','Precipitation(in)'], 
                                              cols_to_relate = ('Severity','severity_type'),
                                              df_to_relate = df_dict['weather_severity'])
    df_dict['event'].drop_duplicates(inplace = True)
    df_dict['event'].reset_index(drop = True, inplace = True)
    
    df_dict['event'].rename(columns = {'AirportCode': 'station_id', 
                            'EventId': 'event_id', 
                            'Type': 'type_id', 
                            'Severity': 'severity_id', 
                            'StartTime(UTC)': 'start_time', 
                            'EndTime(UTC)': 'end_time', 
                            'Precipitation(in)': 'precipitation'},
                           inplace = True)
    
    
    if save_to_csv:
        print(f'{datetime.now()}\tSaving dataframes to csv')
        makedirs("df_tables", exist_ok=True)
        for key, df in df_dict.items():
            print(f'{datetime.now()}\tSaving dataframes to csv\t{key}')
            df.to_csv(f'df_tables/{key}.csv')
    
    return df_dict

def save_prepared_dataframes(df_dict):
    return None

if __name__ == "__main__":
    INCH_TO_CM = 2.54
    
    try:
        dataset_filename = 'WeatherEvents_Jan2016-Dec2021.csv'
        assert is_dataset_available(dataset_filename)
        
        print(f'{datetime.now()}\tLoading data')
        df_raw = pd.read_csv(dataset_filename)
        
        
        print(f'{datetime.now()}\tData analysis\n')
    
        print(f'Columns: \n{(", ").join(df_raw.columns)}\n')
        
        print(f'Most common wheather events: \n{df_raw.groupby(["Type","Severity"]).size().sort_values(ascending = False)}\n')
        
        print(f'Top 5 rainy cities [cm]:\n{(df_raw[df_raw["Type"] == "Rain"].groupby(["State","County","City"])["Precipitation(in)"].sum()*INCH_TO_CM).nlargest()}\n')
        
        print(f'Missing values:\n{find_missing_values(df_raw)}\n')
        
        df = preprocess_missing_values(df_raw)
        df.loc[:,'Precipitation(in)'] *= INCH_TO_CM
        
        df_dict = preprocess_db_import(df, save_to_csv = True)
        
    except AssertionError as e:
        print(f'Missing dataset file in directory!\nDownload dataset from Kaggle first!\n{e}')