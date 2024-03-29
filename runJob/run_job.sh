#!/bin/bash

SPARK_HOME=$SPARK_HOME
APP_JAR="/home/rahin/source-code/spark/Spark-Flights-Data-Analysis/data-extract-processor/target/data-extract-processor-1.0-SNAPSHOT.jar"
INPUT_PATH="/sandbox/storage/data/flight-data/data-set/"
OUTPUT_PATH="/sandbox/storage/data/flight-data/filter_data/"
PARTITIONS="2"

$SPARK_HOME/bin/spark-submit \
    --master spark://dev-server01:7077 \
    --deploy-mode cluster \
    --class org.flight.analysis.FlightDataProcessor \
    --name FlightDataProcessorSpark \
    --driver-memory 4G \
    --driver-cores 4 \
    --executor-memory 8G \
    --executor-cores 2 \
    --total-executor-cores 12 \
    $APP_JAR $INPUT_PATH $OUTPUT_PATH $PARTITIONS

