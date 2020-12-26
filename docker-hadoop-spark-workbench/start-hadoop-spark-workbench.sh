#!/bin/bash

docker-compose up -d

my_ip="localhost"
echo "Namenode: http://${my_ip}:50070"
echo "Datanode: http://${my_ip}:50075"
echo "Spark-master: http://${my_ip}:8080"
echo "Scala-notebook: http://${my_ip}:9001"
echo "Py-Spark-notebook: http://${my_ip}:9101"
echo "Hue (HDFS Filebrowser): http://${my_ip}:8088/home"

