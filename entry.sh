#!/bin/bash

# Source the .bashrc to ensure the alias is available
source ~/.bashrc

# Check if SPARK_MODE is set to master or worker, default to master
if [ "$SPARK_MODE" = "worker" ]; then
    ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.worker.Worker $SPARK_MASTER_URL
else
    ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master
fi

exec "$@"
