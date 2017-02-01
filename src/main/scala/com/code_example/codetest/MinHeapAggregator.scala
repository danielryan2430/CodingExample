package com.code_example.codetest

import java.time.OffsetDateTime

import org.joda.time.DateTime

/**
  * Created by danielimberman on 29/01/17.
  */
object MinOrder extends Ordering[Transaction] {
  def compare(x:Transaction, y:Transaction) = y.collector_tstamp.compareTo(x.collector_tstamp)

}

object MinDateOrder extends Ordering[OffsetDateTime] {
  override def compare(x: OffsetDateTime, y: OffsetDateTime): Int = x.compareTo(y)
}


object MinHeapAggregator {

  /**
    * For this class I use a mutable priorityqueue, as I have found that
    * using immutable datastructures for aggregations can sometimes cause
    * signficant memory issues due to object creation.
    * @return
    */
  def emptyHeap = List[Transaction]()
  def addOneEvent = (q:List[Transaction], u:Transaction) => (q :+ u).sortBy(_.collector_tstamp)(MinDateOrder)
  def mergeEventHeaps = (q1:List[Transaction], q2:List[Transaction]) => (q1 ++ q2).sortBy(_.collector_tstamp)(MinDateOrder)

}
