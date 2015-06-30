package uk.sky

import scala.io.Source

/**
 * Data filterer - Scala implementation
 *
 * Created by clelio on 09/06/15.
 */
trait ScalaDataFilterer {

  def filterByCountry(source: String, country: String): Seq[String]

  def filterByCountryWithResponseTimeAboveLimit(source: String, country: String, limit: Long): Seq[String]

  def filterByResponseTimeAboveAverage(source: String): Seq[String]

  def getLines(source: String) = Source.fromFile(source).getLines()
}

class DataFiltererImpl extends ScalaDataFilterer {

  override def filterByCountry(source: String, country: String): Seq[String] = {
    val lines = getLines(source)
    lines.toList.drop(1).map(l => {
      val items = l.split(",")
      LogEntry(items(0).toLong, items(1), items(2).toLong)
    }).collect {
      case e if e.countryCode == country => s"${e.reqTimestamp},${e.countryCode},${e.resTime}"
    }
  }

  override def filterByCountryWithResponseTimeAboveLimit(source: String, country: String, limit: Long): Seq[String] = {
    val lines = getLines(source)
    lines.toList.drop(1).map(l => {
      val items = l.split(",")
      LogEntry(items(0).toLong, items(1), items(2).toLong)
    }).collect {
      case e if e.countryCode == country && e.resTime > limit => s"${e.reqTimestamp},${e.countryCode},${e.resTime}"
    }
  }

  override def filterByResponseTimeAboveAverage(source: String): Seq[String] = {
    val lines = getLines(source)
    val logEntries = lines.toList.drop(1)
    val sumResTime = logEntries.map(l => {
      val items = l.split(",")
      LogEntry(items(0).toLong, items(1), items(2).toLong)
    }).map(e => e.resTime).sum

    val average = sumResTime / logEntries.size

    logEntries.map(l => {
      val items = l.split(",")
      LogEntry(items(0).toLong, items(1), items(2).toLong)
    }).collect {
      case e if e.resTime > average => s"${e.reqTimestamp},${e.countryCode},${e.resTime}"
    }
  }

}

case class LogEntry(reqTimestamp: Long, countryCode: String, resTime: Long)