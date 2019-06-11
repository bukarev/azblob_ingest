export MAPR_TICKET_LOCATION=/tmp/maprticket_5000
MASTER=yarn-client /opt/mapr/spark/spark-2.3.1/bin/spark-submit --executor-memory 12G --num-executors 40 --class logsLive /home/mapr/logs_process/target/scala-2.11/logs-live-data-process_2.11-0.0.1.jar $1 $2
