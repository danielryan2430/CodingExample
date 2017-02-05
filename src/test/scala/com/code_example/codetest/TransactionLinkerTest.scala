package com.code_example.codetest

import java.time._

import org.apache.spark.{SparkConf, SparkContext}
import org.scalacheck.{Gen, Arbitrary}
import org.scalatest.prop.Checkers
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by danielimberman on 29/01/17.
  */
class TransactionLinkerTest extends FlatSpec with Matchers with Checkers{




  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("coding test"))


  implicit val arbitraryTransaction:Arbitrary[Transaction] =
    Arbitrary(
      for{
        event_id <- Gen.chooseNum(0,(System.currentTimeMillis()/1000).toInt).map(_.toString)
        collector_tstamp <- Gen.chooseNum(0,(System.currentTimeMillis()/1000).toInt).map(epochToDate)
        domain_userid <- Gen.chooseNum(0,5).map(_.toString)
        page_urlpath <- Gen.chooseNum(0,(System.currentTimeMillis()/1000).toInt).map(_.toString)
      } yield Transaction(event_id, collector_tstamp,domain_userid,page_urlpath,""))

  def epochToDate: (Int) => OffsetDateTime = {
    LocalDateTime.ofEpochSecond(_, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC)
  }


  "parseUsersFromCSV" should "convert an RDD of string an RDD of (userId, user)" in {
    check((transactions:Seq[Transaction]) =>{
      val transStrings = transactions.map(t => t.toCsvEntry(addEventId = true))
      val userRDD = sc.parallelize(transStrings)

      val reserialized = TransactionLinker.parseUsersFromCSV(userRDD).collect().map(_._2)
      val tSet = transactions.toSet

      reserialized.length == transactions.length &&
      reserialized.forall(t => tSet.contains(t))
    })
  }

  "getTimeStamps" should "write timestamps for each user" in {
    check((transactions: Seq[Transaction]) => {
      val userRDD = sc.parallelize(transactions).map(_.toCsvEntry(true))
      val answer = TransactionLinker.linkTransactionsToNextTimestamp(userRDD).collect().toList.sorted

      answer.length shouldEqual transactions.length
      val relevantInfo = answer.map(_.split(",")).map(t => (t(0), Transaction.parseDate(t(1)), Transaction.parseNextId(t.toList)))
      val eventMap = relevantInfo.map {case (event, date, next) => (event, date)}.toMap

      relevantInfo.forall {
        case (event, date, next) if  next != "" && date == eventMap(next) => true
        case (event, date, next) if next != "" => date.isBefore(eventMap(next))
        case _ => true
      }
    })

  }

}
