# -*- coding: utf-8 -*-
"""
Created on Sat Mar 26 23:23:52 2022

@author: samuel ubrezi
"""

import pandas as pd
from db_setup import is_dataset_available
from datetime import datetime


def find_missing_values(dataframe):
    return dataframe.isnull().sum().loc[lambda x: x > 0].sort_values(ascending = False)

def filter_missing_values(dataframe, col):
    return dataframe[dataframe[col].isnull()]

def replace_invalid_data(dataframe, col, df_filter, new_value):
    df_new = dataframe.copy()
    df_new.loc[df_filter, col] = new_value
    
    return df_new

def prepare_for_db_import(dataframe, cols_to_keep, cols_to_relate = None, df_to_relate = None, drop_duplicates = False):
    df_import = dataframe[cols_to_keep].copy()
    
    if (type(None) != type(cols_to_relate)) and (type(None) != type(df_to_relate)):
        # replace_dict = dict(zip(df_to_relate[cols_to_relate[1]], df_to_relate.index))
        # df_import = df_import.replace({cols_to_relate[0]: replace_dict})
        
        df_import = df_import.merge(df_to_relate[cols_to_relate[1]].reset_index(), 
                                    left_on = cols_to_relate[0], 
                                    right_on = cols_to_relate[1])
        if cols_to_relate[0] == cols_to_relate[1]:
            df_import.drop(columns = [f'{cols_to_relate[0]}'], 
                           inplace = True)
        else:
            df_import.drop(columns = [f'{cols_to_relate[0]}', f'{cols_to_relate[1]}'], 
                           inplace = True)
        df_import.rename(columns = {"index": cols_to_relate[0]}, 
                         inplace = True)
            
  
    if drop_duplicates:
        df_import = df_import.drop_duplicates().reset_index(drop=True)
        return df_import.to_frame() if type(df_import) == pd.Series else df_import
    else:
        df_import = df_import.reset_index(drop=True)
        return df_import.to_frame() if type(df_import) == pd.Series else df_import
    
def preprocess_missing_values(dataframe):
    df_new = dataframe.copy()
    missing_cols = find_missing_values(df_new)
    
    for col in missing_cols.index:
        print(f'{datetime.now()}\tData preprocessing\t{col}')
        if col == 'ZipCode':
            df_tmp = filter_missing_values(df_new, col)
            df_new = replace_invalid_data(df_new, 
                                          col, 
                                          df_new.index.isin(df_tmp.index), 
                                          0)
        elif col == 'City':
            # One liner that will iterate over entire dataset
            # df_new['City'] = df_new['City'].apply(lambda x: f'{x["AirportCode"]} UNK' if pd.isnull(x['City']) else x['City'])
            
            # Long version that will replace only relevant values
            df_tmp = filter_missing_values(df_new, col)
            for key in df_tmp['AirportCode'].unique():
                df_new = replace_invalid_data(df_new, 
                                              col, 
                                              df_new.index.isin(df_tmp.index) & (df_new['AirportCode'] == key), 
                                              f'{key} UNK')
        else:
            ...
    
    return df_new

def preprocess_db_import(dataframe):
    df_dict = dict()
    
    print(f'{datetime.now()}\tPreparing data for DB import\tweather_type')
    df_dict['weather_type'] = prepare_for_db_import(dataframe, 
                                                    'Type', 
                                                    drop_duplicates = True)
    print(f'{datetime.now()}\tPreparing data for DB import\tweather_severity')
    df_dict['weather_severity'] = prepare_for_db_import(dataframe, 
                                                        'Severity', 
                                                        drop_duplicates = True)
    print(f'{datetime.now()}\tPreparing data for DB import\tus_state')
    df_dict['us_state'] = prepare_for_db_import(dataframe, 
                                                'State', 
                                                drop_duplicates = True)
    print(f'{datetime.now()}\tPreparing data for DB import\tcounty')
    df_dict['county'] = prepare_for_db_import(dataframe, 
                                              ['State','County'], 
                                              cols_to_relate = ('State','State'),
                                              df_to_relate = df_dict['us_state'],
                                              drop_duplicates = True)
    print(f'{datetime.now()}\tPreparing data for DB import\tcity')
    df_dict['city'] = prepare_for_db_import(dataframe, 
                                              ['County','City','TimeZone'], 
                                              cols_to_relate = ('County','County'),
                                              df_to_relate = df_dict['county'],
                                              drop_duplicates = True)
    print(f'{datetime.now()}\tPreparing data for DB import\tstation')
    df_dict['station'] = prepare_for_db_import(dataframe, 
                                              ['City','AirportCode','ZipCode','LocationLat','LocationLng'], 
                                              cols_to_relate = ('City','City'),
                                              df_to_relate = df_dict['city'],
                                              drop_duplicates = True)
    print(f'{datetime.now()}\tPreparing data for DB import\tevent')
    df_dict['event'] = prepare_for_db_import(dataframe, 
                                              ['AirportCode','EventId','Type','Severity','StartTime(UTC)','EndTime(UTC)','Precipitation(in)'], 
                                              cols_to_relate = ('AirportCode','AirportCode'),
                                              df_to_relate = df_dict['station'])
    df_dict['event'] = prepare_for_db_import(df_dict['event'], 
                                              ['AirportCode','EventId','Type','Severity','StartTime(UTC)','EndTime(UTC)','Precipitation(in)'], 
                                              cols_to_relate = ('Type','Type'),
                                              df_to_relate = df_dict['weather_type'])
    df_dict['event'] = prepare_for_db_import(df_dict['event'], 
                                              ['AirportCode','EventId','Type','Severity','StartTime(UTC)','EndTime(UTC)','Precipitation(in)'], 
                                              cols_to_relate = ('Severity','Severity'),
                                              df_to_relate = df_dict['weather_severity'])
    
    return df_dict

def save_prepared_dataframes(df_dict):
    return None

if __name__ == "__main__":
    try:
        dataset_filename = 'WeatherEvents_Jan2016-Dec2021.csv'
        assert is_dataset_available(dataset_filename)
        
        print(f'{datetime.now()}\tLoading data')
        df_raw = pd.read_csv(dataset_filename)
        
        print(f'{datetime.now()}\tData preprocessing')
        df = preprocess_missing_values(df_raw)
        
        print(f'{datetime.now()}\tPreparing data for DB import')
        df_dict, a = preprocess_db_import(df)
        
    except AssertionError:
        print('Missing dataset file in directory!\nDownload dataset from Kaggle first!')