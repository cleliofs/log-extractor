package uk.sky

import org.scalatest.FunSuite

/**
 * Created by clelio on 09/06/15.
 */
class ScalaDataFiltererTest extends FunSuite {

  val filterer: ScalaDataFilterer = new DataFiltererImpl

  test("An empty log file should have no output") {
    assert(filterer.filterByCountry("src/test/resources/empty", "GB").isEmpty)
  }

  test("A single line log file should produce one output for matching country") {
    val result: Seq[String] = filterer.filterByCountry("src/test/resources/single-line", "GB")
    assert(1 == result.size)
    assert("1431592497,GB,200" == result.head)
  }

  test("A multiple lines log file should produce all matching by country results") {
    val resultForGB: Seq[String] = filterer.filterByCountry("src/test/resources/multi-lines", "GB")
    assert(2 == resultForGB.size)
    assert(resultForGB.contains("1432917066,GB,37"))
    assert(resultForGB.contains("1432364089,GB,515"))

    val resultForUS: Seq[String] = filterer.filterByCountry("src/test/resources/multi-lines", "US")
    assert(3 == resultForUS.size)
    assert(resultForUS.contains("1433190845,US,539"))
    assert(resultForUS.contains("1433666287,US,789"))
    assert(resultForUS.contains("1432484176,US,850"))
  }

  test("A multi lines log file should produce empty results for a country not matching") {
    val result: Seq[String] = filterer.filterByCountry("src/test/resources/multi-lines", "NONE")
    assert(0 == result.size)
  }

  test("A multi lines log file should produce entries with response time above limit for matching country") {
    val resultForUS: Seq[String] = filterer.filterByCountryWithResponseTimeAboveLimit("src/test/resources/multi-lines", "US", 600)
    assert(2 == resultForUS.size)
    assert(resultForUS.contains("1433666287,US,789"))
    assert(resultForUS.contains("1432484176,US,850"))
  }

  test("A multi lines log file should produce empty result when there is not response time above limit for matching country") {
    val resultForGB: Seq[String] = filterer.filterByCountryWithResponseTimeAboveLimit("src/test/resources/multi-lines", "GB", 600)
    assert(0 == resultForGB.size)
  }

  test("A multi lines log file should produce entries for response time above average regardless of country matching") {
    val result: Seq[String] = filterer.filterByResponseTimeAboveAverage("src/test/resources/multi-lines")
    assert(4 == result.size)
    assert(result.contains("1433190845,US,539"))
    assert(result.contains("1433666287,US,789"))
    assert(result.contains("1432484176,US,850"))
    assert(result.contains("1432364090,DE,615"))
  }

}
