package app

import de.umass.lastfm.Caller
import entities.User
import lastFm.Client
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import traits.SparkSessionWrapper

object Main extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    Caller.getInstance.setUserAgent("test-beccosauro")
    val client  = new Client("dbf6b9f99ea1ee2762fcde6c1f17baff")
    //val topHits = spark.createDataFrame(spark.sparkContext.parallelize(client.getTopHits(100).toList))
    val df : DataFrame  = spark createDataFrame(
      spark.sparkContext.parallelize(client.populate().toList)
    )
    df.show(1000,false)
    df.repartition(1).write.option("header","true").mode(saveMode = "overwrite").parquet("output/test")

  }
}
