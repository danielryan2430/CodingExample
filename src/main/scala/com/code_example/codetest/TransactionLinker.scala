package com.code_example.codetest

import java.io.{PrintWriter, File}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import cats.implicits._


/**
  * Created by danielimberman on 29/01/17.
  */


object TransactionLinkerHelper {

  import MinHeapAggregator._

  implicit class EitherRDD(rdd: RDD[Either[String, (String, Transaction)]]) {
    def extractOnlyCorrect(): RDD[(String, Transaction)] = rdd.filter(_.isRight) map (_.right.get)
  }

  implicit class EventPairRDD(rdd: RDD[(String, Transaction)]) extends Serializable {
    def mergeAndSortValues() = {
      rdd.aggregateByKey(emptyHeap)(addOneEvent, mergeEventHeaps)
    }
  }

}

object TransactionLinker extends App {
  import TransactionLinkerHelper._

  val conf = new SparkConf().setAppName("code example")
  val sc = new SparkContext(conf)
  val input = sc.textFile(args(0))
  val fields = sc.parallelize(Seq(Transaction.getFields.mkString(",")))
  val result = fields ++ linkTransactionsToNextTimestamp(input)
  val resString = result.collect()

  val file = new File("/app/output.csv")
  val writer = new PrintWriter(file)
  resString.foreach(s => writer.write(s + "\n"))


  /**
    * Takes in the sorted Transaction list, and ties each value to the "next" event
    * @return
    */

  def linkSortedList: (List[Transaction]) => List[Transaction] = {
    users => {
      var nextValue = ""
      val answer = users.reverse.map(d => {
        d.setNextEventId(nextValue)
        nextValue = d.event_id
        d
      })
      answer
    }
  }

  def parseUsersFromCSV(input: RDD[String]): RDD[(String, Transaction)] = {
    val outputOpt = for {
      users <- input.map(Transaction.parseFromCSV)
    } yield users.map(u => (u.domain_userid, u))

    /**
      * In production cases, I would use the Either[] Objects for error handling.
      * that can be logged at the end of calculation. This system would also have the benefit of allowing
      * 'railway oriented programming', where events can be handled further down the line.
      * http://fsharpforfunandprofit.com/posts/recipe-part2/
      *
      * For this example I will just remove the incorrect values
      */
    outputOpt.extractOnlyCorrect()

  }

  def linkTransactionsToNextTimestamp(input: RDD[String]): RDD[String] = {
    val userPairs: RDD[(String, Transaction)] = parseUsersFromCSV(input)
    val timestampSortedEvents = userPairs.mergeAndSortValues().values
    val eventLinkedList = timestampSortedEvents.flatMap(linkSortedList)
    eventLinkedList.map(_.toCsvEntry(addEventId = true))
  }

}
