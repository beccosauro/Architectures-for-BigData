package app

import de.umass.lastfm.Caller
import entities.{Song, UserTrack}
import lastFm.Client
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import traits.SparkSessionWrapper
import utilities.util

object Main extends SparkSessionWrapper with App {
  Caller.getInstance.setUserAgent("test-beccosauro")
  val client = new Client("dbf6b9f99ea1ee2762fcde6c1f17baff")
  val userData = client.populate(2,from = util.getMidnightYesterday, to = util.getMidnightToday)
  val logger: Logger = Logger.getLogger("INGESTION ETL")
  var counter = 0
  logger.info("Numero canzoni da aggiungere" + userData.size)
  val userDataDf = (spark createDataFrame spark.sparkContext.parallelize(userData))
    .withColumn("date", to_date(from_unixtime(col("startSong"))))
    .withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("day", dayofmonth(col("date")))
    .repartition(col("username"))

  userDataDf.write
    .mode("append")
    .partitionBy("username", "year", "month", "day")
    .parquet("/data/raw/user_track_listening")

  val songData: List[Song] = userData.map {
    ut: UserTrack =>(ut.artist, ut.title)
  }.distinct.toVector.par.map { t: (String, String) =>
    logger.info("Canzoni analizzate: " + counter + " Mancanti:" + (userData.size - counter))
    counter += 1
    Thread.sleep(200)
    client.getInfoTrack(t._2, t._1)
  }.toList


  val songDataDf = (spark createDataFrame spark.sparkContext.parallelize(songData))
    .withColumn("date", current_date())
    .withColumn("year", year(to_date(col("date"))))
    .withColumn("month", month(to_date(col("date"))))
    .withColumn("day", dayofmonth(to_date(col("date"))))
  songDataDf.write
    .mode("append")
    .partitionBy("year", "month", "day")
    .parquet("/data/raw/track")

}
