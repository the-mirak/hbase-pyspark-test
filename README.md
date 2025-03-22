# HBase and PySpark Integration Demo

This repository demonstrates the integration between Apache HBase and Apache Spark (PySpark) using Docker containers. It provides scripts to test HBase operations and PySpark connectivity.

## Project Structure

```
hbase-pyspark-test/
├── docker-compose.yml       # Docker setup for HBase and PySpark
├── requirements.txt         # Python dependencies
├── run_hbase_pyspark.sh     # Script to run HBase-PySpark integration in Docker
├── data/
│   └── sample_data.csv      # Sample data for PySpark operations
└── scripts/
    ├── docker-hbase-test.py     # Simple HBase connectivity test
    ├── hbase-pyspark-script.py  # Main script for HBase-PySpark integration
    └── run_hbase_pyspark.sh     # Local script for running tests
```

## Prerequisites

- Docker and Docker Compose
- WSL or Linux environment

## Setup and Installation

1. Clone this repository:
   ```bash
   git clone <repository-url>
   cd hbase-pyspark-test
   ```

2. Start the Docker containers:
   ```bash
   docker-compose up -d
   ```

This will start:
- HBase container (`harisekhon/hbase`) with required ports
- PySpark container (`jupyter/pyspark-notebook`) with mounted volumes

## Running the Demo

### Basic HBase Test

To test simple HBase connectivity and operations:

```bash
cd scripts
python3 docker-hbase-test.py
```

This will:
- Connect to HBase using the Thrift server
- Create a test table
- Insert and read test data

### HBase-PySpark Integration

To run the full HBase-PySpark integration script:

```bash
# From project root
chmod +x run_hbase_pyspark.sh
./run_hbase_pyspark.sh
```

This script:
- Runs inside the PySpark Docker container
- Sets the necessary Java environment
- Installs required Python packages
- Connects to HBase using container networking
- Performs HBase operations
- Reads data from CSV in the data directory
- Attempts PySpark-HBase integration

## Scripts Explained

### docker-hbase-test.py

A simple script that:
- Connects to HBase with retry logic
- Creates a test table with specified column families
- Inserts sample data into the table
- Reads and scans the table to verify data

### hbase-pyspark-script.py

A comprehensive script that demonstrates:
- HBase connection using HappyBase
- Table creation and management
- Data insertion and retrieval
- PySpark session initialization
- Reading CSV data into PySpark DataFrames
- Attempting to read/write between PySpark and HBase

**Note**: Full PySpark-HBase integration requires the HBase connector for Spark, which is not included in the default image. The script handles this limitation gracefully.

## Docker Environment

The Docker setup includes:

- **HBase Container**:
  - Exposes ZooKeeper (2181), Web UI (8080), Thrift server (9090)
  - Used for storing and retrieving data

- **PySpark Container**:
  - Provides Jupyter Notebook access (port 8888)
  - Mounts the `scripts` directory as `/home/jovyan/work`
  - Mounts the `data` directory as `/home/jovyan/data`
  - Has JAVA_HOME configured

## Working with HBase

The demo shows how to:
- Create tables with column families
- Insert data with row keys
- Retrieve single rows by key
- Scan tables for multiple rows
- Delete rows or specific columns

## Working with PySpark

The PySpark part demonstrates:
- Creating Spark sessions
- Reading CSV files into DataFrames
- Configuring DataFrame schemas
- Mapping DataFrame columns to HBase column families

## Troubleshooting

### JAVA_HOME Not Set
If you encounter JAVA_HOME issues:
```
JAVA_HOME is not set. Skipping PySpark operations.
```
The script will skip PySpark operations but still perform HBase operations.

### HBase Connector Missing
If you see errors like:
```
Failed to find the data source: org.apache.hadoop.hbase.spark
```
This is expected as the HBase connector for Spark is not included in the default image. In a production environment, you would need to add the appropriate connector JAR.

## Extending the Demo

To add HBase-Spark integration capabilities:
1. Build a custom Docker image with the HBase connector for Spark
2. Modify docker-compose.yml to use your custom image
3. Update the run_hbase_pyspark.sh script as needed

## Conclusion

This demo provides a foundation for learning HBase operations and how PySpark can be integrated with HBase. While the complete integration requires additional components, the scripts demonstrate the overall architecture and data flow.

For production use, consider using a more complete Hadoop distribution that includes all necessary components for HBase-Spark integration.

## Running Scripts in a Hadoop Environment (HDP VM or Production)

The scripts provided in this demo can be adapted to run in a Hortonworks Data Platform (HDP) environment or other production Hadoop environments. Here's how to configure and run them outside of Docker:

### Prerequisites for Hadoop Environment

- HDP or other Hadoop distribution with HBase and Spark
- Python 3.x installed
- Python packages: `happybase`, `pyspark`

### Installation Steps

1. Install required Python packages:
   ```bash
   pip install happybase pyspark
   ```

2. Copy the scripts to your environment:
   ```bash
   # Copy the script files to your environment
   scp scripts/*.py user@hadoop-master:/path/to/destination/
   scp data/*.csv user@hadoop-master:/path/to/data/
   ```

### Modifying the Scripts for Hadoop

Before running the scripts, you need to adjust the connection parameters:

1. **Update HBase Connection Settings**:
   - In both scripts (`docker-hbase-test.py` and `hbase-pyspark-script.py`), modify the `connect_to_hbase` function to use your HBase Thrift server:
     ```python
     def connect_to_hbase(host='your-hbase-thrift-server', port=9090):
         # Function code remains the same
     ```

2. **Update Spark Session Configuration**:
   - In `hbase-pyspark-script.py`, update the `create_spark_session` function:
     ```python
     def create_spark_session():
         """Create a Spark session configured for HDP environment"""
         return (SparkSession.builder
                 .appName("HBase Interaction")
                 .config("spark.hadoop.hbase.zookeeper.quorum", "your-zookeeper-quorum")
                 .config("spark.hadoop.hbase.zookeeper.property.clientPort", "2181")
                 # Add additional Hadoop/HBase configurations as needed
                 .getOrCreate())
     ```

3. **Configure HBase-Spark Integration**:
   - For full HBase-Spark integration, add the HBase connector JAR to your Spark configuration:
     ```python
     def create_spark_session():
         return (SparkSession.builder
                 .appName("HBase Interaction")
                 .config("spark.hadoop.hbase.zookeeper.quorum", "your-zookeeper-quorum")
                 .config("spark.hadoop.hbase.zookeeper.property.clientPort", "2181")
                 .config("spark.jars", "/path/to/hbase-spark-connector.jar")
                 .getOrCreate())
     ```

4. **Update CSV File Path**:
   - Modify the path to your CSV data file:
     ```python
     csv_path = "/path/to/data/sample_data.csv"
     ```

### Running the Scripts in Hadoop

1. **Basic HBase Test**:
   ```bash
   python3 docker-hbase-test.py
   ```

2. **HBase-PySpark Integration Script**:
   ```bash
   # If using Spark Submit
   spark-submit --jars /path/to/hbase-connector.jar hbase-pyspark-script.py
   
   # Or directly with Python if your environment is properly configured
   python3 hbase-pyspark-script.py
   ```

### Integration with Ambari or Cloudera Manager

If you're using Ambari (HDP) or Cloudera Manager:

1. You can submit the script as a Spark job through the management UI
2. Schedule it as a recurring job using Oozie workflows
3. Set up appropriate resource allocation through YARN

### Additional Hadoop-Specific Considerations

1. **Security**: If your Hadoop cluster uses Kerberos authentication:
   ```python
   # Add this to your create_spark_session function
   .config("spark.yarn.principal", "your-principal")
   .config("spark.yarn.keytab", "/path/to/your.keytab")
   ```

2. **Resource Management**: Configure appropriate resource allocation:
   ```python
   # Add these to your create_spark_session function
   .config("spark.executor.memory", "4g")
   .config("spark.executor.cores", "2")
   .config("spark.driver.memory", "2g")
   ```

3. **HBase Namespace**: If using a specific HBase namespace, update catalog configurations:
   ```python
   catalog = {
       "table": {"namespace": "your_namespace", "name": table_name},
       # Rest of the catalog configuration
   }
   ```

By following these guidelines, you can successfully adapt the demonstration scripts to run in a full Hadoop environment, taking advantage of enterprise features like security, resource management, and high availability. 