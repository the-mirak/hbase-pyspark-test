# HBase and PySpark Integration Demo

This repository demonstrates the integration between Apache HBase and Apache Spark (PySpark) using Docker containers. It provides scripts to test HBase operations and PySpark connectivity.

## Project Structure

```
hbase-pyspark-test/
├── docker-compose.yml       # Docker setup for HBase and PySpark
├── requirements.txt         # Python dependencies
├── run_hbase_pyspark.sh     # Script to run HBase-PySpark integration in Docker
├── data/                    # Directory for sample data
└── scripts/                 # Directory for Python scripts
    ├── docker-hbase-test.py     # Simple HBase connectivity test
    └── hbase-pyspark-script.py  # Main script for HBase-PySpark integration
```

## Prerequisites

- Docker and Docker Compose
- WSL or Linux environment

## Setup and Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/the-mirak/hbase-pyspark-test.git
   cd hbase-pyspark-test
   ```

2. Start the Docker containers:
   ```bash
   docker-compose up -d
   ```

   This will start:
   - HBase container with required ports
   - PySpark container with mounted volumes

## Docker Environment

The Docker setup includes:

- **HBase Container**:
  - Exposes ZooKeeper (2181), Web UI (8080), Thrift server (9090)
  - Used for storing and retrieving data

- **PySpark Container**:
  - Provides Jupyter Notebook access (port 8888)
  - Mounts the `scripts` directory as `/home/jovyan/work`
  - Mounts the `data` directory as `/home/jovyan/data`

## Running the Demo

### Basic HBase Test

To test simple HBase connectivity and operations:

```bash
cd scripts
python3 docker-hbase-test.py
```

### HBase-PySpark Integration

To run the full HBase-PySpark integration script:

```bash
chmod +x run_hbase_pyspark.sh
./run_hbase_pyspark.sh
```

## Scripts Explained

### docker-hbase-test.py

A simple script that connects to HBase, creates a test table, and performs basic operations.

### hbase-pyspark-script.py

A comprehensive script that demonstrates:
- HBase connection using HappyBase
- Table creation and data operations
- PySpark session initialization
- Reading data into PySpark DataFrames
- Integration between PySpark and HBase

### Comprehensive Final Test

The `final-test.py` script provides a comprehensive test of the entire HBase-PySpark integration. It:
- Connects to HBase with retry logic
- Creates and manages test tables
- Inserts test data
- Creates and demonstrates PySpark DataFrames
- Tests the full integration workflow

To run this test in the Docker environment:

1. Run it using the Docker container:
   ```bash
   docker exec -it pyspark-docker bash -c "cd /home/jovyan/work && python final-test.py"
   ```

## Troubleshooting

### JAVA_HOME Not Set or Java Not Found

If you encounter JAVA_HOME-related issues:

Manually find the correct Java path in the container:
   ```bash
   docker exec -it pyspark-docker bash
   which java
   readlink -f $(which java)
   ```

Specify the correct JAVA_HOME when running the script:
   ```bash
   docker exec -it pyspark-docker bash -c "export JAVA_HOME=/opt/conda/jre && cd /home/jovyan/work && python hbase-pyspark-script.py"
   ```

### HBase Connector Missing

If you see errors related to missing HBase connector:

```
Failed to find the data source: org.apache.hadoop.hbase.spark
```

This is expected, as the HBase connector for Spark is not included in the default Docker image. The script will handle this gracefully and continue with basic operations.

To add the connector:

1. Download the required JARs:
   ```bash
   mkdir -p jars
   wget -P jars https://repo1.maven.org/maven2/org/apache/hbase/hbase-spark/3.0.0/hbase-spark-3.0.0.jar
   wget -P jars https://repo1.maven.org/maven2/org/apache/hbase/hbase-client/2.4.12/hbase-client-2.4.12.jar
   wget -P jars https://repo1.maven.org/maven2/org/apache/hbase/hbase-common/2.4.12/hbase-common-2.4.12.jar
   ```

2. Update docker-compose.yml to mount the jars directory:
   ```yaml
   volumes:
     - ./scripts:/home/jovyan/work
     - ./data:/home/jovyan/data
     - ./jars:/home/jovyan/jars
   ```

3. Use a modified run script that includes the JARs in the classpath.

### Jupyter Notebook Error

If you see errors like:
```
No such file or directory: /home/jovyan/#
```

Update the docker-compose.yml file:
```yaml
command: >
  bash -c "
    fix-permissions /home/jovyan &&
    start-notebook.sh --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.notebook_dir='/home/jovyan/work'
  "
```

## Running in a Hadoop Environment

For deploying in a production Hadoop environment such as Hortonworks Data Platform (HDP):

### Prerequisites

1. Install required packages: 
   ```bash
   pip install happybase pyspark
   ```

2. Verify HBase and Spark are properly installed on your HDP cluster
   - Check using Ambari dashboard or command line tools
   - Ensure HBase Thrift server is running

### Required Modifications

#### 1. Connection Settings (HBase)

In both scripts (`docker-hbase-test.py` and `hbase-pyspark-script.py`), modify the `connect_to_hbase` function:

```python
# FROM:
def connect_to_hbase(host='hbase', port=9090):
    # ...existing code...

# TO:
def connect_to_hbase(host='your-hbase-thrift-server-hostname', port=9090):
    # ...keep the rest of the function the same...
```

> **Note**: Replace 'your-hbase-thrift-server-hostname' with your actual HBase Thrift server hostname. 
> Get this from Ambari UI or by running `hostname -f` on the HBase Thrift server node.

#### 2. Spark Session Configuration

In `hbase-pyspark-script.py`, update the `create_spark_session` function:

```python
# FROM:
def create_spark_session():
    return (SparkSession.builder
            .appName("HBase Interaction")
            .getOrCreate())

# TO:
def create_spark_session():
    return (SparkSession.builder
            .appName("HBase Interaction")
            .config("spark.hadoop.hbase.zookeeper.quorum", "zk1.example.com,zk2.example.com,zk3.example.com")
            .config("spark.hadoop.hbase.zookeeper.property.clientPort", "2181")
            .config("spark.hadoop.hbase.mapreduce.inputtable", "user_data")
            # If using HDP's Spark, you might need additional configs
            .getOrCreate())
```

> **Note**: 
> - Get the ZooKeeper quorum from Ambari → HBase → Configs → Advanced → hbase-site.xml 
> - Look for the property `hbase.zookeeper.quorum`
> - Adjust client port if your cluster uses a non-default port

#### 3. Add HBase Connector for Spark

For HDP, the connector JARs may be available in your cluster already, but you need to specify them:

```python
# In create_spark_session function, add:
.config("spark.jars", "/usr/hdp/current/hbase-client/lib/hbase-spark-connector.jar,/usr/hdp/current/hbase-client/lib/hbase-client.jar")
```

> **Note**: Verify actual JAR locations on your HDP cluster using:
> `find /usr/hdp -name "hbase-*.jar" | grep -E "client|spark"`

#### 4. For Secure Clusters (Kerberos)

If your HDP cluster uses Kerberos security:

```python
# Add to create_spark_session:
.config("spark.yarn.principal", "user@REALM.COM")
.config("spark.yarn.keytab", "/path/to/user.keytab")
```

> **Note**: 
> - Replace with your Kerberos principal and keytab location
> - Ensure you've run `kinit` before executing the script if not using keytab

### Running final-test.py in HDP Environment

To run the final test in an HDP environment:

1. Modify the connection settings in the script:
   ```python
   def connect_to_hbase(host='your-hbase-thrift-server', port=9090, timeout=30):
       # Function body stays the same
   ```

2. Update the Spark session configuration:
   ```python
   def create_spark_session():
       # Path to JARs - update this to the HDP path
       jar_dir = "/usr/hdp/current/hbase-client/lib"
       
       # Update ZooKeeper configuration
       spark_builder = (SparkSession.builder
                      .appName("HDP HBase Test")
                      .config("spark.hadoop.hbase.zookeeper.quorum", "zk1.example.com,zk2.example.com,zk3.example.com")
                      .config("spark.hadoop.hbase.zookeeper.property.clientPort", "2181"))
       
       # Rest of function stays the same
   ```

3. Run with spark-submit:
   ```bash
   spark-submit \
     --master yarn \
     --deploy-mode client \
     --jars $(find /usr/hdp/current/hbase-client/lib -name "hbase-*.jar" | tr '\n' ',') \
     --files /etc/hbase/conf/hbase-site.xml \
     final-test.py
   ```

### Running the Script

#### Option 1: Using spark-submit (Recommended)

```bash
# First, authenticate if using Kerberos
kinit -kt /path/to/keytab user@REALM.COM

# Run with spark-submit
spark-submit \
  --master yarn \
  --deploy-mode client \
  --jars /usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/hbase-protocol.jar,/usr/hdp/current/hbase-client/lib/hbase-server.jar \
  --files /etc/hbase/conf/hbase-site.xml \
  --conf spark.driver.memory=2g \
  --conf spark.executor.memory=4g \
  hbase-pyspark-script.py
```

#### Option 2: Using Ambari/Oozie

1. Create a workflow.xml file for Oozie
2. Configure the Spark action with necessary JARs and configurations
3. Submit through the Ambari UI or oozie CLI

### Resource Optimization

For production workloads, adjust resource allocation:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 4g \
  --executor-cores 2 \
  --driver-memory 2g \
  --num-executors 10 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=5 \
  --conf spark.dynamicAllocation.maxExecutors=20 \
  hbase-pyspark-script.py
```

> **Note**: Adjust memory and core settings based on your cluster capacity and workload requirements.

### Troubleshooting HDP-specific Issues

1. **ClassNotFoundException for HBase connector**:
   - Verify connector JAR paths in your HDP installation
   - Add additional JARs from `/usr/hdp/current/hbase-client/lib/` if needed

2. **Authentication failures**:
   - Check Kerberos ticket: `klist`
   - Renew if expired: `kinit`
   - Ensure proper permissions for keytab files

3. **ZooKeeper connection issues**:
   - Verify ZooKeeper is running: `echo stat | nc zk-host 2181`
   - Check network connectivity between execution node and ZooKeeper

4. **HBase region server connectivity**:
   - Verify HBase region servers are up in Ambari
   - Check logs at `/var/log/hbase/` 