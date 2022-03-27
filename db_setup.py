# -*- coding: utf-8 -*-
"""
Created on Sat Mar 26 16:01:36 2022

@author: samuel ubrezi
"""

# Import libraries
import sqlite3 as sql
from os.path import exists


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
    
    connection = None
    try: 
        connection = sql.connect(database_path)
        return connection
    except sql.Error as err :
        print(f'Database connection has failed!\n{err}')
    
    return connection

def execute_scripts_from_file(file_path, cursor):
    
    file = open(file_path, 'r')
    sql_script = file.read()
    file.close()

    sql_commands = sql_script.split(';')

    for command in sql_commands:
        try:
            cursor.execute(command)
        except sql.OperationalError as err:
            print(f'Command skipped because: {err}')


if __name__ == "__main__":
    try: 
        dataset_filename = 'WeatherEvents_Jan2016-Dec2021.csv'
        database_name = 'weather.db'
        table_setup_filename = 'create_tables.sql'
        
        assert is_dataset_available(dataset_filename)
        
        db_connection = connect_database(database_name)
        cursor = db_connection.cursor()
        
        execute_scripts_from_file(table_setup_filename, cursor)
        
        
    except AssertionError:
        print('Missing dataset file in directory!\nDownload dataset from Kaggle first!')