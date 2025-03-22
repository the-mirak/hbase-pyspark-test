#!/bin/bash
# ================================================================================ #
# run_hbase_pyspark.sh
#
# This script is a wrapper for running the HBase-PySpark integration script
# inside the Docker container environment. It handles the following tasks:
#
# 1. Sets the JAVA_HOME environment variable in the container
# 2. Changes to the correct working directory
# 3. Installs required Python packages (pyspark, happybase)
# 4. Executes the main Python script
#
# The script executes commands in the pyspark-docker container, which
# has access to the mounted scripts and data directories.
#
# Usage:
#   ./run_hbase_pyspark.sh
#
# Prerequisites:
#   - Docker containers must be running (started with docker-compose up -d)
#   - Docker image 'pyspark-docker' must be available
#   - The Python script files must be available in the container
# ================================================================================ #

# Print informational message
echo "Running the script in the PySpark Docker container..."

# Execute commands in the Docker container:
# 1. Set JAVA_HOME to the Java installation in the container
# 2. Change to the directory containing the scripts
# 3. Install required Python packages
# 4. Run the main Python script
docker exec -it pyspark-docker bash -c "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && cd /home/jovyan/work && pip install pyspark==3.4.1 happybase && python hbase-pyspark-script.py" 