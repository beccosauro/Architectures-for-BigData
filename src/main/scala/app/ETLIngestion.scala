package app

import de.umass.lastfm.Caller
import lastFm.Client
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import utilities.util
object ETLIngestion {
  def main(args: Array[String]) {
    Caller.getInstance.setUserAgent("test-beccosauro")
    val client = new Client("dbf6b9f99ea1ee2762fcde6c1f17baff")
    val conf = new SparkConf().setMaster("spark://spark-master:7077").setAppName("SparkWriteApplication")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val logger: Logger = Logger.getLogger("INGESTION ETL")
    val userData = client.populate(2, limit = 500, from = util.getMidnightYesterday, to = util.getMidnightToday)
    logger.info("Numero canzoni da aggiungere" + userData.size)
    val userDataDf = (spark createDataFrame spark.sparkContext.parallelize(userData))
      .withColumn("date", to_date(from_unixtime(col("startSong"))))
      .withColumn("year", year(col("date")))
      .withColumn("month", month(col("date")))
      .withColumn("day", dayofmonth(col("date")))


    val allTrack = spark.read.parquet("/data/raw/user_track_listening")
      .select("artist", "title")
      .distinct()
      .withColumn("index", row_number().over(Window.orderBy("artist", "title")))
      .repartition(5)
      .cache()
    val totSong = spark.sparkContext.broadcast(allTrack.count())
    totSong.value
    val updatedTrack = allTrack.rdd.mapPartitions { iter =>
      val logger: Logger = Logger.getLogger("INGESTION ETL")
      iter.map { r: Row =>
        val c = new Client("dbf6b9f99ea1ee2762fcde6c1f17baff")
        logger.info("song processed: " + r.getInt(2) + " of: " + totSong.value)
        Thread.sleep(100)
        c.getInfoTrack(r.getString(0), r.getString(1))
      }
    }.toDF("title", "artist", "genre", "duration")

    userDataDf.write
      .mode("append")
      .partitionBy("username", "year", "month", "day")
      .parquet("/data/raw/user_track_listening")

    updatedTrack
      .withColumn("date", current_date())
      .withColumn("year", year(to_date(col("date"))))
      .withColumn("month", month(to_date(col("date"))))
      .withColumn("day", dayofmonth(to_date(col("date"))))
      .write
      .mode("append")
      .partitionBy("year", "month", "day")
      .parquet("/data/raw/track")

    Logger.getLogger("INGESTION ETL").info("PROCESSO INGESTION CONCLUSO CORRETTAMENTE")

  }
}
