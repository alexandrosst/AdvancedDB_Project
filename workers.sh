#!/bin/bash

declare -a start_workers=("spark-daemon.sh start org.apache.spark.deploy.worker.Worker 1 --webui-port 8080 --port 65509 --cores 2 --memory 4g spark://192.168.0.1:7077"
                        "~/spark-3.1.3-bin-hadoop2.7/sbin/spark-daemon.sh start org.apache.spark.deploy.worker.Worker 2 --webui-port 8080 --port 65510 --cores 2 --memory 4g spark://192.168.0.1:7077")
declare -a stop_workers=("spark-daemon.sh stop org.apache.spark.deploy.worker.Worker 1 --webui-port 8080 --port 65509 --cores 2 --memory 4g spark://192.168.0.1:7077"
                        "~/spark-3.1.3-bin-hadoop2.7/sbin/spark-daemon.sh stop org.apache.spark.deploy.worker.Worker 2 --webui-port 8080 --port 65510 --cores 2 --memory 4g spark://192.168.0.1:7077")

if [ $# -lt 2 ]
then
    echo "Invalid input! Try this syntax: \n ./workers.sh [number of workers] [start|stop]"
elif [ $1 = 1 ]
then
    if [ $2 = start ]
    then
        eval ${start_workers[0]}
    elif [ $2 = stop ]
    then
        eval ${stop_workers[0]}
    else
        echo "You have to use either start or stop"
    fi
elif [ $1 = 2 ]
then
    if [ $2 = start ]
    then
        eval ${start_workers[0]}
        ssh user@slave "${start_workers[1]}"
    elif [ $2 = stop ]
    then
        eval ${stop_workers[0]}
        ssh user@slave "${stop_workers[1]}"
    else
        echo "You have to use either start or stop"
    fi
else
    echo "You have to use either 1 or 2 for number of workers"
fi