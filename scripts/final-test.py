#!/usr/bin/env python3
# Docker-specific test script for PySpark with HBase

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import happybase
import time
import os

def connect_to_hbase(host='hbase', port=9090, timeout=30):
    """Establish connection to HBase with retry"""
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

def create_spark_session():
    """Create a Spark session configured for Docker environment"""
    # Path to the HBase connector JARs
    jar_dir = "/home/jovyan/jars"
    
    # Check if the JAR directory exists
    if not os.path.exists(jar_dir):
        print(f"WARNING: JAR directory {jar_dir} not found!")
        print("PySpark-HBase integration will not work without the proper JARs")
        print("See the guide for downloading the required JARs")
    
    # Look for JAR files
    jar_files = []
    if os.path.exists(jar_dir):
        jar_files = [os.path.join(jar_dir, f) for f in os.listdir(jar_dir) if f.endswith('.jar')]
    
    if jar_files:
        print(f"Found {len(jar_files)} JAR files")
        jars = ",".join(jar_files)
    else:
        print("No JAR files found. Using default configuration.")
        jars = ""
    
    # Create the Spark session
    spark_builder = (SparkSession.builder
                    .appName("Docker HBase Test")
                    .config("spark.hadoop.hbase.zookeeper.quorum", "hbase")
                    .config("spark.hadoop.hbase.zookeeper.property.clientPort", "2181"))
    
    # Add JARs if available
    if jars:
        spark_builder = spark_builder.config("spark.jars", jars)
    
    return spark_builder.getOrCreate()

def prepare_test_data():
    """Create a sample DataFrame for testing"""
    spark = create_spark_session()
    
    # Define schema
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", StringType(), True),
        StructField("occupation", StringType(), True)
    ])
    
    # Create data
    data = [
        ("1", "Alice Johnson", "32", "Engineer"),
        ("2", "Bob Smith", "45", "Manager"),
        ("3", "Carol White", "28", "Developer"),
        ("4", "Dave Brown", "35", "Architect")
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(data, schema)
    print("Created test DataFrame:")
    df.show()
    
    return spark, df

def setup_hbase_table(connection):
    """Create an HBase table for testing"""
    if not connection:
        return False
    
    table_name = 'user_data'
    column_families = ['personal', 'professional']
    
    # Check if table exists first
    tables = [t.decode('utf-8') for t in connection.tables()]
    if table_name in tables:
        print(f"Table '{table_name}' already exists. Recreating...")
        try:
            connection.delete_table(table_name, disable=True)
            print(f"Deleted existing table '{table_name}'")
        except Exception as e:
            print(f"Error deleting table: {e}")
            return False
    
    try:
        cf_dict = {cf: dict() for cf in column_families}
        connection.create_table(table_name, cf_dict)
        print(f"Table '{table_name}' created successfully")
        return True
    except Exception as e:
        print(f"Error creating table: {e}")
        return False

def insert_test_data_hbase(connection):
    """Insert data directly using HappyBase"""
    if not connection:
        return
    
    try:
        table = connection.table('user_data')
        
        # Insert some records
        table.put(b'1', {
            b'personal:name': b'Alice Johnson',
            b'personal:age': b'32',
            b'professional:title': b'Engineer'
        })
        
        table.put(b'2', {
            b'personal:name': b'Bob Smith',
            b'personal:age': b'45',
            b'professional:title': b'Manager'
        })
        
        print("Test data inserted successfully via HappyBase")
    except Exception as e:
        print(f"Error inserting data: {e}")

def main():
    """Main test function"""
    print("STARTING DOCKER PYSPARK-HBASE TEST")
    print("==================================")
    
    # Connect to HBase
    print("\n1. Testing HBase Connection...")
    connection = connect_to_hbase()
    if not connection:
        print("Cannot proceed without HBase connection")
        return
    
    # Set up HBase table
    print("\n2. Setting up HBase test table...")
    success = setup_hbase_table(connection)
    if not success:
        print("Failed to set up HBase table")
        return
    
    # Insert data via HappyBase
    print("\n3. Inserting test data via HappyBase...")
    insert_test_data_hbase(connection)
    
    # Create Spark session and test DataFrame
    print("\n4. Testing PySpark DataFrame creation...")
    try:
        spark, df = prepare_test_data()
        print("PySpark setup successful")
    except Exception as e:
        print(f"Error setting up PySpark: {e}")
        return
    
    # Test connecting PySpark to HBase
    # Note: This part is commented out as it requires proper
    # HBase-Spark connector setup which is beyond this basic test
    
    print("\n5. Final test: Read data from HBase table...")
    try:
        table = connection.table('user_data')
        print("Reading all rows from user_data table:")
        for key, data in table.scan():
            print(f"Row key: {key.decode('utf-8')}")
            for col, val in data.items():
                print(f"  {col.decode('utf-8')}: {val.decode('utf-8')}")
    except Exception as e:
        print(f"Error reading data: {e}")
    
    # Clean up
    print("\nTest completed. Cleaning up...")
    connection.close()
    spark.stop()
    print("Resources released.")

if __name__ == "__main__":
    main()