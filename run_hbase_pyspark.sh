#!/bin/bash
# ================================================================================ #
# run_hbase_pyspark.sh
#
# This script is a wrapper for running the HBase-PySpark integration script
# inside the Docker container environment. It handles the following tasks:
#
# 1. Sets the JAVA_HOME environment variable in the container
# 2. Changes to the correct working directory
# 3. Executes the main Python script
# ================================================================================ #

# Print informational message
echo "Running the script in the PySpark Docker container..."

# Execute the Python script in the Docker container with auto-detection of JAVA_HOME
docker exec -it pyspark-docker bash -c "
# Auto-detect JAVA_HOME
if [ -d /usr/lib/jvm/java-11-openjdk-amd64 ]; then
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
elif [ -d /usr/lib/jvm/java-8-openjdk-amd64 ]; then
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
elif [ -d /usr/local/openjdk-11 ]; then
    export JAVA_HOME=/usr/local/openjdk-11
elif [ -d /usr/local/openjdk-8 ]; then
    export JAVA_HOME=/usr/local/openjdk-8
elif [ -d /opt/java/openjdk ]; then
    export JAVA_HOME=/opt/java/openjdk
else
    # If specific paths are not found, try to detect using readlink
    JAVA_PATH=\$(readlink -f \$(which java) 2>/dev/null)
    if [ -n \"\$JAVA_PATH\" ]; then
        # Remove bin/java from the path to get JAVA_HOME
        export JAVA_HOME=\${JAVA_PATH%/bin/java}
    else
        echo 'Could not auto-detect JAVA_HOME. PySpark operations may fail.'
    fi
fi

echo \"*******Using JAVA_HOME: \$JAVA_HOME\"

# Download HBase-Spark connector JARs if they don't exist
echo \"Downloading HBase connector JARs if needed...\"
mkdir -p /tmp/hbase-jars
cd /tmp/hbase-jars

if [ ! -f hbase-client-2.4.12.jar ]; then
    wget https://repo.maven.apache.org/maven2/org/apache/hbase/hbase-client/2.4.12/hbase-client-2.4.12.jar
fi
if [ ! -f hbase-spark-2.4.12.jar ]; then
    wget https://repo.maven.apache.org/maven2/org/apache/hbase/hbase-spark/2.4.12/hbase-spark-2.4.12.jar
fi
if [ ! -f hbase-common-2.4.12.jar ]; then
    wget https://repo.maven.apache.org/maven2/org/apache/hbase/hbase-common/2.4.12/hbase-common-2.4.12.jar
fi

echo \"Running script with HBase connector JARs...\"
cd /home/jovyan/work && spark-submit --jars /tmp/hbase-jars/hbase-client-2.4.12.jar,/tmp/hbase-jars/hbase-spark-2.4.12.jar,/tmp/hbase-jars/hbase-common-2.4.12.jar hbase-pyspark-script.py
" 