To start the spark job run the script nx_live.sh with parameters:
./nx_live.sh [dev|prod] [hive_table]

1. dev or prod is the environment for which the script is executed
2. hive_table is the hive table, into which the migrated files are added. Currently it is nxmetrics_prod.clickstream

Keep in minf that the script refers to the location of a JAR file containing the application code (currently points to /home/mapr/)
