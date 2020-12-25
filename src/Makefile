

get-uberjar:
	if [ ! -f uberjar/test.jar ]; then \
	sbt assembly ; \
	fi

run-etl: get-uberjar
	docker run --rm -it --network docker-hadoop-spark-workbench_default --env-file /home/becco/Desktop/LastFm-Univ/docker-hadoop-spark-workbench/hadoop.env --volume $(shell pwd)/uberjar:/uberjar bde2020/spark-base:2.1.0-hadoop2.8-hive-java8 /spark/bin/spark-submit --master spark://spark-master:7077 /uberjar/test.jar
