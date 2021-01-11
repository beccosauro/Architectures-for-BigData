package lastFm

import de.umass.lastfm._
import entities.{Song, UserTrack}
import org.apache.log4j.Logger

import java.util
import scala.collection.JavaConversions.iterableAsScalaIterable

class Client(key: String) {


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

  def populate(recurse: Int = 1, from: Long = 0, to: Long = 0,limit:Int): List[UserTrack] = {
    val logger: Logger = Logger.getLogger("INGESTION ETL")
    val population = getUsers(recurse).par.take(limit)
    var counter = 0
    population.flatMap { name: String =>
      logger.info("Utenti Analizzati = " + counter + " Mancanti = " + (population.size - counter))
      counter += 1
      try {
        val totalPage = getRecentTracks(name, 1, from = 1607472000, 200, key).getTotalPages
        (1 to totalPage).flatMap { pgNumber: Int =>
          val infoUser = getRecentTracks(name, pgNumber, from = 1607472000, 200, key)
          infoUser.getPageResults.filterNot(_.isNowPlaying).map {
            t: Track =>
              UserTrack(name, t.getName, t.getArtist, t.getPlayedWhen.toInstant.getEpochSecond)
          }
        }
      } catch {
        case _: Exception => logger.error("Error retrivieng song data")
          (1 to 2).map{_ =>UserTrack("error", "error","error",-1)}
      }

    }.toList
  }

  def getInfoTrack(artist: String, title: String): Song = {
    try {
      val infoTrack = Track.getInfo(artist, title, key)
      val genreList = infoTrack.getTags.toList
      val genre = if (genreList.isEmpty) "no-genre" else genreList.head
      Song(infoTrack.getName, infoTrack.getArtist, genre, infoTrack.getDuration)
    } catch {
      case _: Exception => println("Error retrivieng song data")
        Song("error", "error", "no-genre", -1)
    }
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
