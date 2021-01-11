package utilities
import java.util.Calendar
import java.util.TimeZone

object util {

  def getMidnightToday: Long = {
    val c = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    c.add(Calendar.DAY_OF_MONTH, 0)
    c.set(Calendar.HOUR_OF_DAY, 0)
    c.set(Calendar.MINUTE, 0)
    c.set(Calendar.SECOND, 0)
    c.getTimeInMillis
  }
  def getMidnightYesterday: Long ={
    val c = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    c.add(Calendar.DAY_OF_MONTH, -1)
    c.set(Calendar.HOUR_OF_DAY, 0)
    c.set(Calendar.MINUTE, 0)
    c.set(Calendar.SECOND, 0)
    c.getTimeInMillis
  }

}
