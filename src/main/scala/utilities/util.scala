package utilities

import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.{Calendar, TimeZone}

object util {

  def getMidnightToday: Long = {
    val c = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    c.add(Calendar.DAY_OF_MONTH, 0)
    c.set(Calendar.HOUR_OF_DAY, 0)
    c.set(Calendar.MINUTE, 0)
    c.set(Calendar.SECOND, 0)
    c.getTimeInMillis / 1000
  }

  def writeLastIngestion(path: String)(implicit spark: SparkSession): Unit = {
    val format = new SimpleDateFormat("d-M-y")
    val today = format.format(Calendar.getInstance().getTime)
    import spark.implicits._
    spark.sparkContext.parallelize(Seq(today)).coalesce(1).toDF.write.mode("overwrite").text(path)
  }

  def readLastIngestion(path: String)(implicit spark: SparkSession): String = {
   spark.read.textFile(path).collect().head
  }

  def getEpochLastIngestion(path: String)(implicit spark: SparkSession): Long = {
    val dtf = DateTimeFormatter.ofPattern("d-M-y")
    val dt = LocalDate.parse(readLastIngestion(path), dtf)
    dt.toEpochDay * 86400
  }


}
