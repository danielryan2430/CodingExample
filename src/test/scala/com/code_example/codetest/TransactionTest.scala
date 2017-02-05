package com.code_example.codetest

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by danielimberman on 29/01/17.
  */
class TransactionTest extends FlatSpec with Matchers{
  val fullValue = "babbe8fe-5dd0-4c53-b1ea-4826095ac925,2016-01-01T01:30:55.000+00:00,fee830db7fd1554b,/blades"

  "parseDate" should "parse a date with the timezone" in {
    val dateString = "2016-01-01T01:30:55.000+00:00"
    println(dateString)
   val date = Transaction.parseDate(dateString)
    Transaction.formatDate(date) shouldEqual dateString
  }
  "parseFromCSV" should "return a user" in {
    val expected = Right(Transaction("babbe8fe-5dd0-4c53-b1ea-4826095ac925", Transaction.parseDate("2016-01-01T01:30:55.000+00:00"), "fee830db7fd1554b", "/blades"))
    Transaction.parseFromCSV(fullValue) shouldEqual expected
  }

  it should "error out if there are not enough fields" in {
    Transaction.parseFromCSV("babbe8fe-5dd0-4c53-b1ea-4826095ac925,2016-01-01T01:30:55.000+00:00,fee830db7fd1554b") shouldEqual Left("error in csv splitting: 3")
  }

}
