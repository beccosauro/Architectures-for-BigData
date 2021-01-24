package app

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, current_date, from_unixtime, lag, lower, sum, unix_timestamp}
import utilities.Util.{getLastPeriodSong, getPastDate}

import scala.tools.nsc.interactive.Pickler.TildeDecorator

object ListeningSession {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("spark://spark-master:7077")
      .setAppName("SparkWriteApplication")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val userTrack = spark.read.parquet("/data/raw/user_track_listening").withColumnRenamed("startSong", "start_song")
    val song = getLastPeriodSong(spark.read.parquet("/data/raw/track"), "=", getPastDate(0))
    val join_cond = (lower(userTrack("artist")) === lower(song("artist"))) && (lower(userTrack("title")) === lower(song("title")))

    val df = userTrack.join(song, join_cond).filter("duration > 0")
      .drop("year", "month", "day", "date")
      .drop(userTrack("title")).drop(userTrack("artist"))
      .withColumn("end_song", unix_timestamp(from_unixtime(col("start_song"))) + unix_timestamp(from_unixtime(col("duration"))))
      .select("username", "title", "start_song", "end_song", "artist", "duration").distinct()

    val dfSorted = df.withColumn("start_after", col("start_song") - lag(df("end_song"), 1)
      .over(Window.partitionBy("username").orderBy("start_song")))

    val w = Window.partitionBy("username").orderBy("start_song")
    val session = dfSorted
      .withColumn("keep_listen", col("start_after") <= 10 && col("start_after") >= -10)
      .withColumn("start_song", col("start_song").cast("timestamp"))
      .withColumn("end_song", col("end_song").cast("timestamp"))
      .withColumn("session", sum((!col("keep_listen")).cast("int")).over(w))

    session.groupBy("username", "session").count().filter("count > 1")
      .join(session, List("username", "session")).drop("count", "keep_listen")
      .withColumn("data", current_date())
      .write
      .partitionBy("data", "username", "session")
      .mode("append")
      .parquet("hdfs://namenode:8020/data/attributes/listening_session")

  }
}
