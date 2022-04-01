# -*- coding: utf-8 -*-
"""
Created on Sat Mar 26 16:01:36 2022

@author: samuel ubrezi
"""

# Import libraries
import sqlite3 as sql
from os.path import exists
from datetime import datetime
import pandas as pd


def is_dataset_available(directory = 'WeatherEvents_Jan2016-Dec2021.csv'):
    
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
        i = 1
        for df in pd.read_csv(f'{source_path}/{tbl}.csv', index_col = 0, chunksize = 1_000_000):
            print(f'{datetime.now()}\tImporting data into table\t{tbl}\tbatch {i}')
            df.index.name = 'id'
            df.to_sql(tbl, sql_connection, if_exists='append')
            # del df
            i += 1
        
def get_table_columns(sql_connection, table):
    
    columns = None
    try:
        cursor = sql_connection.execute(f'select * from {table}')
        columns = [description[0] for description in cursor.description]
    except sql.Error as err :
        print(f'Database connection has failed!\n{err}')
        
    return columns

def get_table_types(sql_connection, table, columns):
    
    cursor = None
    
    temp = [f'typeOf({val})' for val in columns]
    
    sql_commnad = f"""
        Select {','.join(temp)} from {table} limit 1
    """
    # print(sql_commnad)
    
    try:
        cursor = sql_connection.execute(sql_commnad)
        row = cursor.fetchall()[0]
    except sql.Error as err :
        print(f'Database connection has failed!\n{err}')
    
    return list(zip(columns,row))

def get_id(sql_connection, table, value = None):
    
    idx = None
    try:
        if value is None:
            sql_commnad = f"""
                select max(id) from {table}
            
            """
            cursor = sql_connection.execute(sql_commnad)
        else:
            val = value[1]
            if type(val) is str:
                val = '"'+val+'"'
            sql_commnad = f"""
                select id from {table} where {value[0]} = {val}
            """
            cursor = sql_connection.execute(sql_commnad)
        idx = cursor.fetchone()
    except sql.Error as err :
        print(f'Database connection has failed!\n{err}')
    
    return idx if idx is None else idx[0]

        
def insert_single_record(sql_connection, table, row):
    
    cursor = None
    dtype_map = {'integer': int,
                 'text': str,
                 'real': float}
    
    columns = get_table_columns(sql_connection, table)
    
    table_dtypes = get_table_types(sql_connection, table, columns)
    
    print(f'{datetime.now()}\tInserting row {row} into table {table}')    
    try:
        assert len(columns) == len(row)
        for dt, val in zip(table_dtypes, row):
            assert dtype_map[dt[1]] == type(val)
        
        sql_command = f'''
            INSERT INTO {table}({','.join(columns)})
            VALUES({','.join(['?']*len(columns))})
        '''
        # print(sql_command)
        
        cursor = sql_connection.cursor()
        cursor.execute(sql_command, row)
        sql_connection.commit()
        
    except AssertionError as e:
        print(f'Row doesnt match table requirements!\n{e}')
    except sql.Error as err :
        print(f'Database connection has failed!\n{err}')
    
    return cursor if cursor is None else cursor.lastrowid

def insert_single_event(sql_connection, row):
    
    table_order = ['us_state','county','city','station','weather_type','weather_severity', 'event']
    table_row = {t: dict() for t in table_order}
    
    table_row['event']['row'] = None, row[0], row[3], row[4], row[5], row[1], row[2], row[7]
    table_row['event']['ref'] = None
    table_row['event']['idx'] = ('event_id',1)
    
    table_row['station']['row'] = None, row[7], row[8], row[9], row[13], row[10]
    table_row['station']['ref'] = ('event', 7)
    table_row['station']['idx'] = ('airport_code', 1)   
    
    table_row['city']['row'] = None, row[10], row[6], row[11]
    table_row['city']['ref'] = ('station', 5)
    table_row['city']['idx'] = ('city_name', 1)   
    
    table_row['county']['row'] = None, row[11], row[12]
    table_row['county']['ref'] = ('city', 3) 
    table_row['county']['idx'] = ('county_name', 1) 
    
    table_row['us_state']['row'] = None, row[12]
    table_row['us_state']['ref'] = ('county', 2)
    table_row['us_state']['idx'] = ('state_code', 1)   
    
    table_row['weather_type']['row'] = None, row[1]
    table_row['weather_type']['ref'] = ('event', 5)
    table_row['weather_type']['idx'] = ('type_name', 1)   
    
    table_row['weather_severity']['row'] = None, row[2]
    table_row['weather_severity']['ref'] = ('event', 6)
    table_row['weather_severity']['idx'] = ('severity_type', 1)    
    
    for t in table_order:
        ref_id = get_id(sql_connection, t, value = (table_row[t]['idx'][0], table_row[t]['row'][table_row[t]['idx'][1]]))
        if ref_id is None:
            tuple_as_list = list(table_row[t]['row'])
            tuple_as_list[0] = get_id(sql_connection, t) + 1
            ref_id = insert_single_record(sql_connection, t, tuple(tuple_as_list))
            
        if table_row[t]['ref'] is not None:
            tuple_as_list = list(table_row[table_row[t]['ref'][0]]['row'])
            tuple_as_list[table_row[t]['ref'][1]] = ref_id
            table_row[table_row[t]['ref'][0]]['row'] = tuple(tuple_as_list)
            
    return ref_id

if __name__ == "__main__":
    try: 
        dataset_filename = 'WeatherEvents_Jan2016-Dec2021.csv'
        database_name = 'weather.db'
        table_setup_filename = 'create_tables.sql'
        import_data_path = 'df_tables/'
        
        print(f'{datetime.now()}\tCheck if dataset available\t{dataset_filename}')
        assert is_dataset_available(dataset_filename)
        
        sql_connection = connect_database(database_name)
        cursor = sql_connection.cursor()
        
        execute_scripts_from_file(table_setup_filename, cursor)
        
        import_data_from_csv(sql_connection, import_data_path)
        
        # insert_table = 'station'
        # insert_row_id = get_id(sql_connection, insert_table) + 1 
        # insert_row = (insert_row_id,'AAAA',35.0,100.0,0,0)
        # insert_result = insert_single_record(sql_connection, insert_table, insert_row)
        
        insert_event = ('W-1000000','Test','Test','2021-04-01 16:54:00','2021-04-01 20:34:00',1,'SK/Mountain','TST1',00.0000,-111.1111,'Test','Test','GG',11111)
        idx = insert_single_event(sql_connection, insert_event)
        
        idx_test = get_id(sql_connection, 'event', value = ('event_id', insert_event[0]))
        
    except AssertionError as e:
        print(f'Missing dataset file in directory!\nDownload dataset from Kaggle first!\n{e}')