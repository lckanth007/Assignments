#!/bin/bash
FILE=/home/cloudera/Hive/job.properties
db_name=$(grep -i 'databaseName' $FILE  | cut -f2 -d'=')
table_name=$(grep -i 'tableName' $FILE  | cut -f2 -d'=')
echo "Database Name: " $db_name
echo  "Table Name: " $table_name
hive -hiveconf DB_NAME=$db_name -hiveconf TABLE_NAME=$table_name -f /home/cloudera/Hive/hive.hql
