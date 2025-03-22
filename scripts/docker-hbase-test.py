#!/usr/bin/env python3
# Test script for HBase using Docker

import happybase
import time

def connect_to_hbase(host='localhost', port=9090, timeout=30):
    """Establish connection to HBase with retry
    
    Docker containers might take a moment to fully initialize
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            connection = happybase.Connection(host=host, port=port)
            print(f"Successfully connected to HBase at {host}:{port}")
            return connection
        except Exception as e:
            print(f"Connection attempt failed: {e}")
            print("Retrying in 3 seconds...")
            time.sleep(3)
    
    print(f"Could not connect to HBase after {timeout} seconds")
    return None

def list_tables(connection):
    """List all tables in HBase"""
    if not connection:
        return []
    
    tables = connection.tables()
    print("HBase Tables:")
    for table in tables:
        print(f"- {table.decode('utf-8')}")
    return tables

def create_test_table(connection):
    """Create a test table"""
    if not connection:
        return
    
    table_name = 'test_table'
    column_families = ['cf1', 'cf2']
    
    # Check if table exists first
    tables = [t.decode('utf-8') for t in connection.tables()]
    if table_name in tables:
        print(f"Table '{table_name}' already exists. Recreating...")
        try:
            connection.delete_table(table_name, disable=True)
            print(f"Deleted existing table '{table_name}'")
        except Exception as e:
            print(f"Error deleting table: {e}")
            return
    
    try:
        cf_dict = {cf: dict() for cf in column_families}
        connection.create_table(table_name, cf_dict)
        print(f"Table '{table_name}' created successfully")
    except Exception as e:
        print(f"Error creating table: {e}")

def insert_test_data(connection):
    """Insert some test data"""
    if not connection:
        return
    
    try:
        table = connection.table('test_table')
        table.put(b'row1', {
            b'cf1:col1': b'value1',
            b'cf1:col2': b'value2',
            b'cf2:col1': b'value3'
        })
        
        table.put(b'row2', {
            b'cf1:col1': b'value4',
            b'cf1:col2': b'value5',
            b'cf2:col1': b'value6'
        })
        
        print("Test data inserted successfully")
    except Exception as e:
        print(f"Error inserting data: {e}")

def read_test_data(connection):
    """Read the test data back"""
    if not connection:
        return
    
    try:
        table = connection.table('test_table')
        print("\nReading single row:")
        row = table.row(b'row1')
        for key, value in row.items():
            print(f"  {key.decode('utf-8')}: {value.decode('utf-8')}")
        
        print("\nScanning all rows:")
        for key, data in table.scan():
            print(f"Row key: {key.decode('utf-8')}")
            for col, val in data.items():
                print(f"  {col.decode('utf-8')}: {val.decode('utf-8')}")
    except Exception as e:
        print(f"Error reading data: {e}")

def main():
    print("Testing connection to HBase in Docker...")
    connection = connect_to_hbase()
    
    if connection:
        list_tables(connection)
        create_test_table(connection)
        list_tables(connection)
        insert_test_data(connection)
        read_test_data(connection)
        connection.close()
        print("\nTest completed successfully")
    else:
        print("Could not establish connection to HBase")

if __name__ == "__main__":
    main()