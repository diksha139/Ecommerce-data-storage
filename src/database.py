import mysql.connector
import random
import time
import datetime


# Global methods to push interact with the Database
ERROR_STATEMENT = "There was an error while performing task"
# This method establishes the connection with the MySQL
def create_server_connection(host_name, user_name, user_password):
    # Implement the logic to create the server connection
    try:
        connection = mysql.connector.connect(host=host_name,
                                             user=user_name,
                                             password=user_password
                                             )
        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f'connection established successfully to MySQL server {db_info}')
            return connection
    except mysql.connector.Error as e:
        print(f'error while connecting to server {e}')

    return None
    


# This method will create the database and make it an active database
def create_and_switch_database(connection, db_name, switch_db):
    # For database creation use this method
    # If you have created your databse using UI, no need to implement anything
    try:
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name};')
            cursor.execute(f'USE {switch_db};')
            cursor.execute('SELECT DATABASE();')
            record = cursor.fetchone()
            print(f'{record} database created successfully')
            connection.commit()
            cursor.close()
            # connection.close()
    except Exception as e:
        print(ERROR_STATEMENT, e, sep='\n')

# This method will establish the connection with the newly created DB
def create_db_connection(host_name, user_name, user_password, db_name):
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f'connection to MySQL server {db_info} established successfully')
            return connection
    except mysql.connector.Error as e:
        print(f'connection failed to establish {e}')

    return None

# Use this function to create the tables in a database
def create_table(connection, table_creation_statement):
    try:
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(table_creation_statement)
            connection.commit()
            cursor.close()
            # connection.close()
    except Exception as e:
        print(ERROR_STATEMENT, e, sep='\n')

# Perform all single insert statments in the specific table through a single function call
def create_insert_query(connection, query):
    # This method will perform creation of the table
    # this can also be used to perform single data point insertion in the desired table
    try:
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(query)
            print("Query execution successfully")
            connection.commit()
            cursor.close()
            # connection.close()
    except Exception as e:
        print(ERROR_STATEMENT, e, sep='\n')


# retrieving the data from the table based on the given query
def select_query(connection, query):
    # fetching the data points from the table 
    try:
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            for record in records:
                print(record)
            cursor.close()
            print(f'found {len(records)} rows')
            # connection.close()
            return records
    except Exception as e:
        print(ERROR_STATEMENT, e, sep='\n')


# Execute multiple insert statements in a table
def insert_many_records(connection, sql, val):
    try:
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.executemany(sql, val)
            print(f"Successfully inserted {len(val)} records")
            connection.commit()
            cursor.close()
            # connection.close()
    except Exception as e:
        print(ERROR_STATEMENT, e, sep='\n')

