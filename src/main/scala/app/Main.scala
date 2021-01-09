package app

import de.umass.lastfm.Caller
import lastFm.Client
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, dayofmonth, from_unixtime, month, to_date, year}
import traits.SparkSessionWrapper

object Main extends SparkSessionWrapper with App {
  Caller.getInstance.setUserAgent("test-beccosauro")
  val client = new Client("dbf6b9f99ea1ee2762fcde6c1f17baff")

  val df: DataFrame = (spark createDataFrame spark.sparkContext.parallelize(client.populate()))
    .withColumn("date", to_date(from_unixtime(col("startSong"))))
    .withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("day", dayofmonth(col("date")))

  df.write
    .mode("overwrite")
    .partitionBy("username", "year", "month", "day")
    .parquet("/test/output")


}
