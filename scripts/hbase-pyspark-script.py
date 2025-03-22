#!/usr/bin/env python3
# ================================================================================ #
# hbase-pyspark-script.py
#
# This script demonstrates the integration between HBase and Apache Spark (PySpark).
# It performs the following operations:
# 1. Connects to HBase using happybase (Python wrapper for HBase Thrift)
# 2. Creates and manipulates tables in HBase
# 3. Inserts and retrieves data from HBase
# 4. Initializes a PySpark session with HBase configurations
# 5. Demonstrates how to read from and write to HBase using PySpark DataFrames
# 6. Handles CSV data for PySpark operations
#
# The script is designed to gracefully handle missing dependencies (such as the
# HBase connector for Spark) and will continue with supported operations if some
# components are unavailable.
#
# Requirements:
#   - happybase package (pip install happybase)
#   - pyspark package (pip install pyspark)
#   - Running HBase instance with Thrift server enabled
#   - Java JDK installed and JAVA_HOME environment variable set
#   - For full functionality: HBase connector for Spark
#
# Usage:
#   python3 hbase-pyspark-script.py
#
# When run in a container environment, use the run_hbase_pyspark.sh wrapper script
# which sets the necessary environment variables and dependencies.
# ================================================================================ #

from pyspark.sql import SparkSession
import happybase
import json
import sys
import os

def create_spark_session():
    """Create a Spark session configured for HBase integration
    
    Initializes a SparkSession with the appropriate configurations for
    connecting to HBase via the Zookeeper quorum. This is the entry point
    for all PySpark operations.
    
    Returns:
        SparkSession: Configured Spark session object
        
    Note:
        The Zookeeper configuration assumes HBase service name 'hbase' for
        Docker environments. In production environments, replace with the
        appropriate Zookeeper quorum addresses.
    """
    return (SparkSession.builder
            .appName("HBase Interaction")
            .config("spark.hadoop.hbase.zookeeper.quorum", "hbase")
            .config("spark.hadoop.hbase.zookeeper.property.clientPort", "2181")
            .getOrCreate())

def connect_to_hbase(host='hbase', port=9090):
    """Establish connection to HBase
    
    Creates a connection to the HBase Thrift server. This is the entry point
    for all HBase operations using the happybase library.
    
    Args:
        host (str): Hostname or IP address of HBase Thrift server
                  Default is 'hbase' for Docker environments
        port (int): Port number of HBase Thrift server
                  Default is 9090
                  
    Returns:
        happybase.Connection: Connection object for HBase operations
        
    Raises:
        SystemExit: If connection cannot be established
    """
    try:
        connection = happybase.Connection(host=host, port=port)
        print(f"Successfully connected to HBase at {host}:{port}")
        return connection
    except Exception as e:
        print(f"Error connecting to HBase: {e}")
        sys.exit(1)

def list_tables(connection):
    """List all tables in HBase
    
    Retrieves and displays all table names from the HBase instance.
    
    Args:
        connection: HappyBase connection object
        
    Returns:
        list: List of table names as bytes objects
        
    Note:
        Table names are returned as bytes and decoded to UTF-8 for display
    """
    tables = connection.tables()
    print("HBase Tables:")
    for table in tables:
        print(f"- {table.decode('utf-8')}")
    return tables

def create_table(connection, table_name, column_families):
    """Create a new HBase table with specified column families
    
    Creates a table in HBase with the given name and column families.
    If the table already exists, it will use the existing table instead
    of attempting to create a new one.
    
    Args:
        connection: HappyBase connection object
        table_name (str): Name of the table to create
        column_families (list or dict): Either a list of column family names
                                      or a dictionary with column family options
    
    Note:
        HBase tables are organized by column families, which are defined
        at table creation time. Column qualifiers can be added dynamically
        within these families.
    """
    # Convert string list to dictionary for proper column family creation
    if isinstance(column_families, list):
        cf_dict = {cf: dict() for cf in column_families}
    else:
        cf_dict = column_families
    
    # Check if table exists before trying to create it
    tables = [t.decode('utf-8') for t in connection.tables()]
    if table_name in tables:
        print(f"Table '{table_name}' already exists. Using existing table.")
        return
        
    try:
        connection.create_table(table_name, cf_dict)
        print(f"Table '{table_name}' created successfully")
    except Exception as e:
        print(f"Error creating table: {e}")

def insert_data(connection, table_name, row_key, data):
    """Insert data into an HBase table
    
    Adds or updates a row in an HBase table with the specified data.
    
    Args:
        connection: HappyBase connection object
        table_name (str): Target table name
        row_key (bytes): Row identifier (primary key)
        data (dict): Dictionary with column family:qualifier as keys and values to insert
                   All keys and values should be bytes objects
    
    Example:
        insert_data(conn, 'users', b'user1', {
            b'personal:name': b'John Doe',
            b'personal:age': b'30',
            b'contact:email': b'john@example.com'
        })
    """
    try:
        table = connection.table(table_name)
        table.put(row_key, data)
        print(f"Data inserted successfully for row '{row_key}'")
    except Exception as e:
        print(f"Error inserting data: {e}")

def get_row(connection, table_name, row_key, columns=None):
    """Retrieve a single row from an HBase table
    
    Gets a specific row from HBase by its row key, optionally
    filtering for specific columns.
    
    Args:
        connection: HappyBase connection object
        table_name (str): Source table name
        row_key (bytes): Row identifier to retrieve
        columns (list, optional): List of specific columns to retrieve
                               Format: [b'family:qualifier', ...]
    
    Returns:
        dict: Dictionary containing the row data, or None if error occurs
              Format: {b'family:qualifier': b'value', ...}
    """
    try:
        table = connection.table(table_name)
        row = table.row(row_key, columns=columns)
        print(f"Data for row '{row_key}':")
        for key, value in row.items():
            print(f"  {key.decode('utf-8')}: {value.decode('utf-8')}")
        return row
    except Exception as e:
        print(f"Error retrieving row: {e}")
        return None

def scan_table(connection, table_name, row_start=None, row_stop=None, columns=None, limit=10):
    """Scan an HBase table for multiple rows
    
    Retrieves multiple rows from an HBase table with various filtering options.
    This is more efficient than multiple get operations when retrieving
    many rows.
    
    Args:
        connection: HappyBase connection object
        table_name (str): Source table name
        row_start (bytes, optional): Start row for scan (inclusive)
        row_stop (bytes, optional): Stop row for scan (exclusive)
        columns (list, optional): List of specific columns to retrieve
        limit (int, optional): Maximum number of rows to return (default 10)
    
    Returns:
        list: List of tuples (row_key, row_data) with decoded values
              Format: [('row_key', {'family:qualifier': 'value', ...}), ...]
    """
    try:
        table = connection.table(table_name)
        scanner = table.scan(row_start=row_start, row_stop=row_stop, 
                           columns=columns, limit=limit)
        
        results = []
        print(f"Scanning table '{table_name}':")
        for row_key, row_data in scanner:
            print(f"Row: {row_key.decode('utf-8')}")
            row_dict = {}
            for col, val in row_data.items():
                col_decoded = col.decode('utf-8')
                val_decoded = val.decode('utf-8')
                print(f"  {col_decoded}: {val_decoded}")
                row_dict[col_decoded] = val_decoded
            results.append((row_key.decode('utf-8'), row_dict))
        
        return results
    except Exception as e:
        print(f"Error scanning table: {e}")
        return []

def delete_row(connection, table_name, row_key, columns=None):
    """Delete a row or specific columns from an HBase table
    
    Removes either an entire row or specified columns from an HBase table.
    
    Args:
        connection: HappyBase connection object
        table_name (str): Target table name
        row_key (bytes): Row identifier to delete
        columns (list, optional): List of specific columns to delete
                               If None, deletes the entire row
    
    Note:
        In HBase, deletes mark the data with a tombstone rather than
        immediately removing it. The data is actually removed during
        compaction operations.
    """
    try:
        table = connection.table(table_name)
        if columns:
            table.delete(row_key, columns=columns)
            print(f"Specified columns deleted from row '{row_key}'")
        else:
            table.delete(row_key)
            print(f"Row '{row_key}' deleted successfully")
    except Exception as e:
        print(f"Error deleting row: {e}")

def pyspark_hbase_read(spark, table_name, columns=None):
    """Read HBase table into PySpark DataFrame
    
    This function loads data from an HBase table into a PySpark DataFrame,
    which can then be used for Spark-based analytical processing.
    
    Args:
        spark: SparkSession object
        table_name (str): HBase table to read
        columns (list, optional): List of columns to read
                               Format: ['family:qualifier', ...]
                               
    Returns:
        pyspark.sql.DataFrame or None: DataFrame containing the HBase data,
                                    or None if operation fails
                                    
    Note:
        This function requires the HBase connector for Spark, which may not be
        available in all environments. The function handles this situation
        gracefully with appropriate error messages.
    """
    try:
        # Configure catalog mapping for the HBase table
        # The catalog defines how HBase table structure maps to DataFrame schema
        catalog = {
            "table": {"namespace": "default", "name": table_name},
            "rowkey": "key",
            "columns": {
                "rowkey": {"cf": "rowkey", "col": "key", "type": "string"}
            }
        }
        
        # Add the specified columns to the catalog
        if columns:
            for col in columns:
                if ':' in col:
                    cf, qualifier = col.split(':')
                    catalog["columns"][qualifier] = {"cf": cf, "col": qualifier, "type": "string"}
        
        # Read from HBase using the Shaded Client API
        df = (spark.read
              .format("org.apache.hadoop.hbase.spark")
              .options(catalog=json.dumps(catalog))
              .load())
        
        return df
    except Exception as e:
        print(f"Error reading from HBase: {e}")
        print("Note: This might be due to missing HBase connector for Spark.")
        print("In a production environment, you would need to add the appropriate HBase-Spark connector JAR.")
        return None

def pyspark_hbase_write(spark, df, table_name, row_key_column, column_mapping):
    """Write PySpark DataFrame to HBase table
    
    Saves data from a PySpark DataFrame to an HBase table, mapping
    DataFrame columns to HBase column families and qualifiers.
    
    Args:
        spark: SparkSession object
        df: DataFrame to write
        table_name (str): HBase table to write to
        row_key_column (str): Column in DataFrame to use as row key
        column_mapping (dict): Mapping from DataFrame columns to HBase
                             Format: {'df_column': 'cf:qualifier'}
                             
    Note:
        This function requires the HBase connector for Spark, which may not be
        available in all environments. The function handles this situation
        gracefully with appropriate error messages.
    """
    try:
        # Configure catalog mapping for the HBase table
        # This defines how DataFrame columns map to HBase table structure
        catalog = {
            "table": {"namespace": "default", "name": table_name},
            "rowkey": "key",
            "columns": {
                "key": {"cf": "rowkey", "col": "key", "type": "string"}
            }
        }
        
        # Add column mappings to catalog
        for df_col, hbase_col in column_mapping.items():
            cf, qualifier = hbase_col.split(':')
            catalog["columns"][df_col] = {"cf": cf, "col": qualifier, "type": "string"}
        
        # Rename the row key column to 'key' as expected by the catalog
        df_to_write = df.withColumnRenamed(row_key_column, "key")
        
        # Write to HBase
        (df_to_write.write
         .format("org.apache.hadoop.hbase.spark")
         .options(catalog=json.dumps(catalog))
         .save())
        
        print(f"Data successfully written to HBase table '{table_name}'")
    except Exception as e:
        print(f"Error writing to HBase: {e}")
        print("Note: This might be due to missing HBase connector for Spark.")
        print("In a production environment, you would need to add the appropriate HBase-Spark connector JAR.")

def main():
    """Main execution function
    
    Orchestrates the demo of HBase and PySpark integration with the following steps:
    1. Connect to HBase
    2. Create a table with personal and professional column families
    3. Insert sample user data
    4. Retrieve and display the data
    5. If JAVA_HOME is set, initialize PySpark and perform DataFrame operations:
       a. Try to read from HBase (requires connector)
       b. Read CSV data or create sample data
       c. Attempt to write DataFrame to HBase (requires connector)
       d. Verify final data state in HBase
    
    This function demonstrates both the HBase API directly and the
    integration with PySpark for more complex data processing.
    """
    # Example usage
    connection = connect_to_hbase()
    
    # Create a table
    create_table(connection, 'user_data', ['personal', 'professional'])
    
    # Insert data
    insert_data(connection, 'user_data', b'user1', {
        b'personal:name': b'John Doe',
        b'personal:age': b'30',
        b'personal:city': b'New York',
        b'professional:title': b'Data Engineer',
        b'professional:company': b'Tech Corp'
    })
    
    insert_data(connection, 'user_data', b'user2', {
        b'personal:name': b'Jane Smith',
        b'personal:age': b'28',
        b'personal:city': b'San Francisco',
        b'professional:title': b'Data Scientist',
        b'professional:company': b'AI Solutions'
    })
    
    # Retrieve data
    get_row(connection, 'user_data', b'user1')
    
    # Scan table
    scan_table(connection, 'user_data')
    
    # Check if we can use PySpark
    if 'JAVA_HOME' not in os.environ:
        print("\nJAVA_HOME is not set. Skipping PySpark operations.")
        print("To run PySpark operations, set JAVA_HOME environment variable.")
        return
    
    try:
        # Use PySpark for more complex operations
        print("\nInitializing PySpark session...")
        spark = create_spark_session()
        
        # Try to read from HBase into PySpark DataFrame
        print("\nAttempting to read from HBase into PySpark...")
        users_df = pyspark_hbase_read(spark, 'user_data')
        if users_df is not None:
            users_df.show()
        
        # Create a DataFrame from our own data
        print("\nCreating sample DataFrame...")
        
        # Check if we have a CSV file to read (in the /home/jovyan/data directory in the Docker container)
        csv_path = "/home/jovyan/data/sample_data.csv"
        if os.path.exists(csv_path):
            print(f"Reading data from {csv_path}")
            schema_users = spark.read.option("header", "true").csv(csv_path)
        else:
            # Create sample data manually if no CSV
            print("Using hardcoded sample data")
            from pyspark.sql import Row
            new_users = [
                Row(id="user3", name="Alice Johnson", age="35", 
                    city="Chicago", title="ML Engineer", company="Data Corp"),
                Row(id="user4", name="Bob Williams", age="42", 
                    city="Boston", title="Data Architect", company="Tech Systems")
            ]
            schema_users = spark.createDataFrame(new_users)
        
        print("New users DataFrame:")
        schema_users.show()
        
        # Define column mapping for HBase
        col_mapping = {
            "name": "personal:name",
            "age": "personal:age",
            "city": "personal:city",
            "title": "professional:title",
            "company": "professional:company"
        }
        
        # Write to HBase
        print("\nAttempting to write DataFrame to HBase...")
        pyspark_hbase_write(spark, schema_users, 'user_data', 'id', col_mapping)
        
        # Verify data was written
        print("\nVerifying data after PySpark operations:")
        scan_table(connection, 'user_data')
        
        spark.stop()
        
    except Exception as e:
        print(f"\nError in PySpark operations: {e}")
        print("HBase operations completed, but PySpark operations failed.")

# Standard boilerplate to call the main() function
if __name__ == "__main__":
    main()