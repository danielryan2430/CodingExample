package com.DollarShave.codetest

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by danielimberman on 29/01/17.
  */
class DSCEventTest extends FlatSpec with Matchers{
  val fullValue = "babbe8fe-5dd0-4c53-b1ea-4826095ac925,2016-01-01T01:30:55.000+00:00,fee830db7fd1554b,/blades"

  "parseDate" should "parse a date with the timezone" in {
    val dateString = "2016-01-01T01:30:55.000+00:00"
    println(dateString)
   val date = DSCEvent.parseDate(dateString)
    DSCEvent.formatDate(date) shouldEqual dateString
  }
  "parseFromCSV" should "return a user" in {
    val x = DSCEvent.parseDate("2015-01-01T01:30:55.000+00:00")
    val expected = Right(DSCEvent("babbe8fe-5dd0-4c53-b1ea-4826095ac925", DSCEvent.parseDate("2016-01-01T01:30:55.000+00:00"), "fee830db7fd1554b", "/blades"))
    DSCEvent.parseFromCSV(fullValue) shouldEqual expected
  }

  it should "error out if there are not enough fields" in {
    DSCEvent.parseFromCSV("babbe8fe-5dd0-4c53-b1ea-4826095ac925,2016-01-01T01:30:55.000+00:00,fee830db7fd1554b") shouldEqual Left("error in csv splitting: 3")
  }

}
