package app

import org.apache.hadoop.fs.{FileSystem, Path}
import traits.SparkSessionWrapper

import java.io.BufferedOutputStream

object Main extends SparkSessionWrapper {
  def main(args: Array[String]) : Unit = {
    spark.range(10).show()

    val numbersRdd = spark.sparkContext.parallelize((1 to 10000).toList)



  }
}
