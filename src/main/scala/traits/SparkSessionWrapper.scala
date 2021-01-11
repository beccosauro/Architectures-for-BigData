package traits

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    val conf = new SparkConf()
      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .setMaster("local[*]")
    SparkSession.builder().config(conf).getOrCreate()
  }

}
