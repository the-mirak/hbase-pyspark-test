#!/bin/bash

# Run the Python script in the Docker container where Java is available
echo "Running the script in the PySpark Docker container..."
docker exec -it pyspark-docker bash -c "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && cd /home/jovyan/work && pip install pyspark==3.4.1 happybase && python hbase-pyspark-script.py" 