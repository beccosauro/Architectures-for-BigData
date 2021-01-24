package utilities

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, date_format, desc, lit}

import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.{Calendar, TimeZone}

object Util {

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
  def getLastPeriodSong(df: DataFrame, operator: String, data: String = "1900-1-1"): DataFrame = {
    val columns = Array(col("year"), col("month"), col("day"))
    val condition = operator match {
      case "=" => concat_ws("-", columns: _*) === date_format(lit(data).cast("date"),"yyyy-M-d")
      case ">" => concat_ws("-", columns: _*) > date_format(lit(data).cast("date"),"yyyy-M-d")
      case "<" => concat_ws("-", columns: _*) < date_format(lit(data).cast("date"),"yyyy-M-d")
      case ">=" => concat_ws("-", columns: _*) >= date_format(lit(data).cast("date"),"yyyy-M-d")
      case "<=" => concat_ws("-", columns: _*) <= date_format(lit(data).cast("date"),"yyyy-M-d")
    }
    df.filter(condition)
  }

  def getPastDate(days: Int): String = {
    val yesterday = ZonedDateTime.now(ZoneId.of("UTC")).minusDays(days)
    val formatter = DateTimeFormatter.ofPattern("yyyy-M-d")
    formatter format yesterday
  }

  def countSong(df: DataFrame, limit: Int = 20): DataFrame = {
    df.groupBy("artist", "title").count().orderBy(desc("count")).limit(limit)
  }
}
