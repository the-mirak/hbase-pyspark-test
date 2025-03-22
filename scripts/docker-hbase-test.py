#!/usr/bin/env python3
# ============================================================================ #
# docker-hbase-test.py
# 
# This script tests the connectivity and basic operations with HBase 
# running in a Docker container. It performs the following operations:
# 1. Connect to HBase using the Thrift protocol
# 2. List existing tables
# 3. Create a test table with specified column families
# 4. Insert test data into the table
# 5. Read data back using get and scan operations
#
# Requirements:
#   - happybase package (pip install happybase)
#   - Running HBase container with Thrift server enabled on port 9090
# ============================================================================ #

import happybase
import time

def connect_to_hbase(host='localhost', port=9090, timeout=30):
    """Establish connection to HBase with retry mechanism
    
    This function attempts to connect to the HBase Thrift server and implements
    a retry mechanism to handle delays in container startup or network issues.
    
    Args:
        host (str): Hostname or IP address of the HBase Thrift server
                    Default is 'localhost'
        port (int): Port number for the HBase Thrift server
                    Default is 9090
        timeout (int): Maximum time in seconds to keep trying connections
                    Default is 30 seconds
    
    Returns:
        happybase.Connection or None: Connection object if successful, None otherwise
    
    Note:
        Docker containers might take a moment to fully initialize, which is why
        we implement a retry mechanism with a backoff period.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # Attempt to create a connection to HBase
            connection = happybase.Connection(host=host, port=port)
            print(f"Successfully connected to HBase at {host}:{port}")
            return connection
        except Exception as e:
            # Print error and retry after a delay
            print(f"Connection attempt failed: {e}")
            print("Retrying in 3 seconds...")
            time.sleep(3)
    
    # If we've exhausted our timeout period, give up
    print(f"Could not connect to HBase after {timeout} seconds")
    return None

def list_tables(connection):
    """List all tables in HBase
    
    Retrieves and displays a list of all tables currently in the HBase instance.
    
    Args:
        connection: HappyBase connection object
    
    Returns:
        list: List of table names as bytes objects
    
    Note:
        Table names are returned as bytes objects by HappyBase and need to be
        decoded to UTF-8 for display purposes.
    """
    if not connection:
        return []
    
    # Get list of tables from HBase
    tables = connection.tables()
    print("HBase Tables:")
    for table in tables:
        print(f"- {table.decode('utf-8')}")
    return tables

def create_test_table(connection):
    """Create a test table in HBase
    
    Creates a new table named 'test_table' with two column families: 'cf1' and 'cf2'.
    If the table already exists, it will be deleted and recreated.
    
    Args:
        connection: HappyBase connection object
    
    Returns:
        None
    
    Note:
        This function demonstrates how to:
        1. Check if a table already exists
        2. Delete an existing table (must be disabled first)
        3. Create a new table with specified column families
    """
    if not connection:
        return
    
    # Define table name and column families
    table_name = 'test_table'
    column_families = ['cf1', 'cf2']
    
    # Check if table exists first
    tables = [t.decode('utf-8') for t in connection.tables()]
    if table_name in tables:
        print(f"Table '{table_name}' already exists. Recreating...")
        try:
            # Must disable table before deletion in HBase
            connection.delete_table(table_name, disable=True)
            print(f"Deleted existing table '{table_name}'")
        except Exception as e:
            print(f"Error deleting table: {e}")
            return
    
    try:
        # Create a dictionary of column family configurations
        # Each column family can have specific properties like
        # max_versions, time_to_live, etc.
        cf_dict = {cf: dict() for cf in column_families}
        connection.create_table(table_name, cf_dict)
        print(f"Table '{table_name}' created successfully")
    except Exception as e:
        print(f"Error creating table: {e}")

def insert_test_data(connection):
    """Insert test data into the HBase table
    
    Puts sample data into the 'test_table' with two rows, each containing
    multiple column values across different column families.
    
    Args:
        connection: HappyBase connection object
    
    Returns:
        None
    
    Note:
        In HBase, data is identified by:
        - Row key
        - Column family
        - Column qualifier
        - Timestamp (auto-generated if not specified)
        
        The format for column specification is 'family:qualifier'
    """
    if not connection:
        return
    
    try:
        # Get a reference to the table
        table = connection.table('test_table')
        
        # Insert first row - in HBase data is organized by row key
        # Each row can have different columns
        table.put(b'row1', {
            b'cf1:col1': b'value1',  # Column family 'cf1', qualifier 'col1'
            b'cf1:col2': b'value2',  # Column family 'cf1', qualifier 'col2'
            b'cf2:col1': b'value3'   # Column family 'cf2', qualifier 'col1'
        })
        
        # Insert second row with the same column structure
        table.put(b'row2', {
            b'cf1:col1': b'value4',
            b'cf1:col2': b'value5',
            b'cf2:col1': b'value6'
        })
        
        print("Test data inserted successfully")
    except Exception as e:
        print(f"Error inserting data: {e}")

def read_test_data(connection):
    """Read the test data back from HBase
    
    Demonstrates two ways to read data from HBase:
    1. Get a specific row by its row key
    2. Scan the table for multiple rows
    
    Args:
        connection: HappyBase connection object
    
    Returns:
        None
    
    Note:
        - The row() method retrieves a single row by row key
        - The scan() method allows iteration through multiple rows
        - Both return data as dictionaries with column names as keys
    """
    if not connection:
        return
    
    try:
        # Get a reference to the table
        table = connection.table('test_table')
        
        # Method 1: Read a specific row by row key
        print("\nReading single row:")
        row = table.row(b'row1')
        for key, value in row.items():
            # Decode bytes to strings for display
            print(f"  {key.decode('utf-8')}: {value.decode('utf-8')}")
        
        # Method 2: Scan all rows in the table
        print("\nScanning all rows:")
        for key, data in table.scan():
            print(f"Row key: {key.decode('utf-8')}")
            for col, val in data.items():
                # Decode bytes to strings for display
                print(f"  {col.decode('utf-8')}: {val.decode('utf-8')}")
    except Exception as e:
        print(f"Error reading data: {e}")

def main():
    """Main execution function
    
    Coordinates the HBase test operations in sequence:
    1. Connect to HBase
    2. List existing tables
    3. Create/recreate test table
    4. Confirm table creation
    5. Insert test data
    6. Read test data
    7. Close connection
    
    Returns:
        None
    """
    print("Testing connection to HBase in Docker...")
    
    # Establish connection to HBase
    connection = connect_to_hbase()
    
    if connection:
        # If connection successful, run all test operations
        list_tables(connection)
        create_test_table(connection)
        list_tables(connection)
        insert_test_data(connection)
        read_test_data(connection)
        
        # Always close the connection when done
        connection.close()
        print("\nTest completed successfully")
    else:
        print("Could not establish connection to HBase")

# Standard boilerplate to call the main() function
if __name__ == "__main__":
    main()