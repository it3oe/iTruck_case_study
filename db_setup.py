# -*- coding: utf-8 -*-
"""
Created on Sat Mar 26 16:01:36 2022

@author: samuel ubrezi
"""

# Import libraries
import sqlite3 as sql
import glob
from os.path import exists
from datetime import datetime
import pandas as pd


def is_dataset_available(directory = 'WeatherEvents_Jan2016-Dec2021.csv'):
    """
    Parameters
    ----------
    directory : String, optional
        Directory path to weather events dataset. The default is 'WeatherEvents_Jan2016-Dec2021.csv'.

    Returns
    -------
    Bool
        Returns True if file is in directory.

    """
    return exists(f'{directory}')

def connect_database(database_path):
    
    print(f'{datetime.now()}\tConnect database\t{database_path}')
    connection = None
    try: 
        connection = sql.connect(database_path)
        return connection
    except sql.Error as err :
        print(f'Database connection has failed!\n{err}')
    
    return connection

def execute_scripts_from_file(file_path, cursor):
    
    print(f'{datetime.now()}\tRunning SQL scripts from file\t{file_path}')
    file = open(file_path, 'r')
    sql_script = file.read()
    file.close()

    sql_commands = sql_script.split(';')

    for command in sql_commands:
        try:
            cursor.execute(command)
        except sql.OperationalError as err:
            print(f'Command skipped because: {err}')

def import_data_from_csv(sql_connection, source_path):
    IMPORT_ORDER = ['weather_type', 'weather_severity', 'us_state', 'county', 'city', 'station', 'event']
    print(f'{datetime.now()}\tImporting data into database')
    
    for tbl in IMPORT_ORDER:
        print(f'{datetime.now()}\tImporting data into table\t{tbl}')
        df = pd.read_csv(f'{source_path}/{tbl}.csv', index_col = 0)
        df.index.name = 'id'
        df.to_sql(tbl, sql_connection, if_exists='append')
        # del df
        
# def import_single_record(sql_connection, row):
    

if __name__ == "__main__":
    try: 
        dataset_filename = 'WeatherEvents_Jan2016-Dec2021.csv'
        database_name = 'weather.db'
        table_setup_filename = 'create_tables.sql'
        import_data_path = 'df_tables/'
        
        print(f'{datetime.now()}\tCheck if dataset available\t{dataset_filename}')
        assert is_dataset_available(dataset_filename)
        
        db_connection = connect_database(database_name)
        cursor = db_connection.cursor()
        
        execute_scripts_from_file(table_setup_filename, cursor)
        
        import_data_from_csv(db_connection, import_data_path)
        
    except AssertionError:
        print('Missing dataset file in directory!\nDownload dataset from Kaggle first!')