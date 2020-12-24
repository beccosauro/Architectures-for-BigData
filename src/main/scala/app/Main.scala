package app

import traits.SparkSessionWrapper
import de.umass.lastfm.User
import de.umass.lastfm.Caller

import java.text.DateFormat
object Main extends SparkSessionWrapper {
  def main(args: Array[String]) : Unit = {
    Caller.getInstance.setUserAgent("test-beccosauro")

    val key = "dbf6b9f99ea1ee2762fcde6c1f17baff" //this is the key used in the Last.fm API examples
    val user = "JRoar"
    val chart = User.getWeeklyArtistChart(user, 10, key)
    val format = DateFormat.getDateInstance
    val from = format.format(chart.getFrom)
    val to = format.format(chart.getTo)
    System.out.printf("Charts for %s for the week from %s to %s:%n", user, from, to)
    val artists = chart.getEntries
    import scala.collection.JavaConversions._
    for (artist <- artists) {
      System.out.println(artist.getName)
    }

  }
}
