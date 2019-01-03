#!/bin/bash
FILE=job.properties
TBL_ID=$(grep -i 'TBL_ID' $FILE  | cut -f2 -d'=')
SRC_FILE_NM=$(grep -i 'SRC_FILE_NM' $FILE  | cut -f2 -d'=')
SRC_FILE_SCHEMA_NM=$(grep -i 'SRC_FILE_SCHEMA_NM' $FILE  | cut -f2 -d'=')
SRC_FILE_STATS_NM=$(grep -i 'SRC_FILE_STATS_NM' $FILE  | cut -f2 -d'=')
echo "TBL_ID: " $TBL_ID
echo "SRC_FILE_NM: " $SRC_FILE_NM
echo "SRC_FILE_SCHEMA_NM: " $SRC_FILE_SCHEMA_NM
echo  "SRC_FILE_STATS_NM: " $SRC_FILE_STATS_NM

spark-submit --class "SimpleApp" target/scala-2.11/SimpleApp_2.11-1.0.jar TBL_ID SRC_FILE_SCHEMA_NM SRC_FILE_SCHEMA_NM SRC_FILE_STATS_NM
