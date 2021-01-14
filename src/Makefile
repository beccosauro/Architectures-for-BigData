

get-uberjar:
	cd .. ; \
	sbt assembly ; \
	cp -f target/scala-2.11/lastFm-ETL-app.jar src/uberjar/


run-etl: get-uberjar
	docker run --rm -it --network docker-hadoop-spark-workbench_default --env-file ../docker-hadoop-spark-workbench/hadoop.env -e SPARK_MASTER=spark://spark-master:7077 --volume $(shell pwd)/uberjar:/uberjar bde2020/spark-base:2.1.0-hadoop2.8-hive-java8 /spark/bin/spark-submit --master spark://spark-master:7077 /uberjar/lastFm-ETL-app.jar

