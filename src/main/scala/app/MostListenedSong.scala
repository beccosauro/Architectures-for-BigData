package app

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_date
import utilities.Util.{countSong, getLastPeriodSong, getPastDate}


object MostListenedSong {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("spark://spark-master:7077")
      .setAppName("SparkWriteApplication")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val userTrack = spark.read.parquet("/data/raw/user_track_listening").cache()
    val userTrackDaily = countSong(getLastPeriodSong(userTrack, "=", getPastDate(1)))
    val userTrackWeekly = countSong(getLastPeriodSong(userTrack, ">=", getPastDate(7)))
    val userTrackMonthly = countSong(getLastPeriodSong(userTrack, ">=", getPastDate(30)))
    val userTrackAll = countSong(getLastPeriodSong(userTrack, ">="))

    userTrackDaily
      .withColumn("data", current_date())
      .coalesce(1)
      .write
      .partitionBy("data")
      .option("header", "true")
      .option("delimiter", "|")
      .mode("append")
      .csv("hdfs://namenode:8020/data/attributes/most_listened_song=daily")

    userTrackWeekly
      .withColumn("data", current_date())
      .coalesce(1)
      .write
      .partitionBy("data")
      .option("header", "true")
      .option("delimiter", "|")
      .mode("append")
      .csv("hdfs://namenode:8020/data/attributes/most_listened_song=weekly")

    userTrackMonthly
      .withColumn("data", current_date())
      .coalesce(1)
      .write
      .partitionBy("data")
      .option("header", "true")
      .option("delimiter", "|")
      .mode("append")
      .csv("hdfs://namenode:8020/data/attributes/most_listened_song=monthly")

    userTrackAll
      .withColumn("data", current_date())
      .coalesce(1)
      .write
      .partitionBy("data")
      .option("header", "true")
      .option("delimiter", "|")
      .mode("append")
      .csv("hdfs://namenode:8020/data/attributes/most_listened_song=all")
  }
}
