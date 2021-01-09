package lastFm

import de.umass.lastfm._
import entities.{RawData, Song}

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util
import java.util.{Date, HashMap, Map}
import scala.collection.JavaConversions.{asScalaIterator, iterableAsScalaIterable}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.io.Source.fromURL

class Client(key: String, secret: String = "13feb8ebeb0464478b79a4ab411a754e") {


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
  def populate(recurse: Int = 1, pageToRetrieve: Int = 1): List[RawData] = {
    val population = getUsers(recurse)
    population.flatMap { name: String =>
      var (counter, limit) = (0, 30)
      val totalPage = getRecentTracks(name, pageToRetrieve, from = 1607472000, 200, key).getTotalPages

      (1 to totalPage).flatMap { pgNumber: Int =>
        val infoUser = getRecentTracks(name, pgNumber, from = 1607472000, 200, key)
        infoUser.getPageResults.filterNot(_.isNowPlaying).map {
          t: Track =>
            counter += 1
            val duration = Track.getInfo(t.getArtist, t.getName, key).getDuration
            if (counter == limit) {
              Thread.sleep(3000)
              limit += 30
              println("Diamoci una calmata")
            }
            RawData(name, t.getName, t.getArtist, t.getPlayedWhen.toInstant.getEpochSecond, duration)
        }
      }
    }.toList
  }

  def getRecentTracks(user: String, page: Int, from: Long, limit: Int, apiKey: String): PaginatedResult[Track] = {
    val params: util.Map[String, String] = new util.HashMap[String, String]
    params.put("user", user)
    params.put("limit", String.valueOf(limit))
    params.put("page", String.valueOf(page))
    params.put("from", String.valueOf(from))
    val result: Result = Caller.getInstance.call("user.getRecentTracks", apiKey, params)
    ResponseBuilder.buildPaginatedResult(result, classOf[Track])
  }

}
