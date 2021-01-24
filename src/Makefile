

get-uberjar:
	cd .. ; \
	sbt assembly ; \
	cp -f target/scala-2.11/lastFm-ETL-app.jar src/uberjar/


run-etl: get-uberjar
	docker run --rm --network docker-hadoop-spark-workbench_default --env-file ../docker-hadoop-spark-workbench/hadoop.env -e SPARK_MASTER=spark://spark-master:7077 --volume $(shell pwd)/uberjar:/uberjar bde2020/spark-base:2.1.0-hadoop2.8-hive-java8 /spark/bin/spark-submit --master spark://spark-master:7077 --class app.ETLIngestion /uberjar/lastFm-ETL-app.jar

run-attribute: run-etl
	docker run --rm --network docker-hadoop-spark-workbench_default --env-file ../docker-hadoop-spark-workbench/hadoop.env -e SPARK_MASTER=spark://spark-master:7077 --volume $(shell pwd)/uberjar:/uberjar bde2020/spark-base:2.1.0-hadoop2.8-hive-java8 /spark/bin/spark-submit --master spark://spark-master:7077 --class app.MostListenedSong /uberjar/lastFm-ETL-app.jar
	docker run --rm --network docker-hadoop-spark-workbench_default --env-file ../docker-hadoop-spark-workbench/hadoop.env -e SPARK_MASTER=spark://spark-master:7077 --volume $(shell pwd)/uberjar:/uberjar bde2020/spark-base:2.1.0-hadoop2.8-hive-java8 /spark/bin/spark-submit --master spark://spark-master:7077 --class app.MostListPerGenre /uberjar/lastFm-ETL-app.jar
	docker run --rm --network docker-hadoop-spark-workbench_default --env-file ../docker-hadoop-spark-workbench/hadoop.env -e SPARK_MASTER=spark://spark-master:7077 --volume $(shell pwd)/uberjar:/uberjar bde2020/spark-base:2.1.0-hadoop2.8-hive-java8 /spark/bin/spark-submit --master spark://spark-master:7077 --class app.ListeningSession /uberjar/lastFm-ETL-app.jar
