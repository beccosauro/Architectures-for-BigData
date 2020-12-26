package lastFm

import de.umass.lastfm._
import entities.Tracks

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.Date
import scala.collection.JavaConversions.{asScalaIterator, iterableAsScalaIterable}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.io.Source.fromURL

class Client(key: String, secret: String = "13feb8ebeb0464478b79a4ab411a754e") {

  def session: Session = {
    val user = "simocerio" // user name
    val password = "" // user's password

    Authenticator.getMobileSession(user, password, key, secret)
  }

  def getResponse(url: String): String = {
    val html = fromURL(url.replace("api_key=", "api_key=" + key))
    val response = html.mkString
    html.close
    response
  }

  def getTopHits(pageNumber: Int = 1): Iterable[Tracks] = {

    Chart.getTopTracks(pageNumber, key).asScala
      .map { t: Track =>
        val info = Track.getInfo(t.getArtist, t.getName, key)
        val genre = info.getTags.iterator().take(2).mkString("-")
        Tracks(t.getName, t.getArtist, t.getPosition, t.getDuration, t.getListeners, genre)
      }
  }

  def getUsers(recurse: Int = 1, border: Set[String] = Set(), start: Boolean = true, known: Set[String] = Set()): Set[String] = {
    if (start && recurse > 0) {
      val seed = User.getFriends("rj", key).map(_.getName).toSet
      getUsers(recurse - 1, seed diff Set("rj"), start = false, known = seed union Set("rj"))
    }
    else if (!start && recurse > 0) {
      val discovered: Set[String] = border
        .map(u => User
          .getFriends(u, key)
          .map(_.getName).toSet
        ).reduce(_ union _)
      getUsers(recurse - 1, discovered diff known, start = false, discovered union border)
    } else known

  }
  def getDateAsString(d: Date): String = {
    val dateFormat = new SimpleDateFormat("dd-MM-yyyy")
    dateFormat.format(d)
  }
  def populate(recurse: Int = 1): Set[entities.User] = {
    val population = getUsers(recurse)
    val dateFormat = new SimpleDateFormat("dd-MM-yyyy")
    population.map { name: String =>
      val infoUser = User.getRecentTracks(name, -1, 100, key)
      val nameSong: Iterable[String]= infoUser.getPageResults.filterNot(_.isNowPlaying).map(_.getName)
      val played: Iterable[Long] = infoUser.getPageResults
        .filterNot(_.isNowPlaying).map(_.getPlayedWhen.toInstant.getEpochSecond)
      val sessions: Map[String,Long] = nameSong.zip(played).toMap
      entities.User(name,sessions)
    }
  }


}
