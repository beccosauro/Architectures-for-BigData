package app

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{current_date, lower}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utilities.Util.{getLastPeriodSong, getPastDate}

object MostListPerGenre {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("spark://spark-master:7077")
      .setAppName("SparkWriteApplication")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val userTrack = spark.read.parquet("/data/raw/user_track_listening")
    val song = getLastPeriodSong(spark.read.parquet("/data/raw/track"), "=", getPastDate(0))
    val userTrackDaily = getLastPeriodSong(userTrack, "=", getPastDate(1))
    val userTrackWeekly = getLastPeriodSong(userTrack, ">=", getPastDate(7))

    val join_cond = (lower(userTrack("artist")) === lower(song("artist"))) && (lower(userTrack("title")) === lower(song("title")))
    val dailyGenre = userTrackDaily.join(song, join_cond, "inner")
      .drop(song("artist")).drop(song("title"))
      .select("title", "genre")
    val weeklyGenre: DataFrame = userTrackWeekly.join(song, join_cond, "inner")
      .drop(song("artist")).drop(song("title"))
      .select("title", "genre")

    val mostDailyGenre = dailyGenre.join(dailyGenre.groupBy("genre").count().filter("count > 10"),"genre")
    val mostWeeklyGenre = weeklyGenre.join(weeklyGenre.groupBy("genre").count().filter("count > 10"),"genre")

    mostDailyGenre
      .withColumn("data", current_date())
      .coalesce(1)
      .write
      .partitionBy("data", "genre")
      .option("header", "true")
      .option("delimiter", "|")
      .mode("append")
      .csv("hdfs://namenode:8020/data/attributes/most_listened_song_genre=daily")

    mostWeeklyGenre
      .withColumn("data", current_date())
      .coalesce(1)
      .write
      .partitionBy("data", "genre")
      .option("header", "true")
      .option("delimiter", "|")
      .mode("append")
      .csv("hdfs://namenode:8020/data/attributes/most_listened_song_genre=weekly")


  }
}