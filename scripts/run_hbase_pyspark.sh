#!/bin/bash

# Set JAVA_HOME if not already set
if [ -z "$JAVA_HOME" ]; then
    # Try to find Java installations
    if [ -d "/usr/lib/jvm/java-11-openjdk-amd64" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
    elif [ -d "/usr/lib/jvm/default-java" ]; then
        export JAVA_HOME="/usr/lib/jvm/default-java"
    else
        echo "Could not find Java installation. Please set JAVA_HOME manually."
        exit 1
    fi
    echo "Set JAVA_HOME to $JAVA_HOME"
fi

# Run the Python script
python3 hbase-pyspark-script.py 