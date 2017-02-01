package com.DollarShave.codetest


import java.time.OffsetDateTime

import org.joda.time.DateTime

/**
  * Created by danielimberman on 29/01/17.
  */
object MinOrder extends Ordering[DSCEvent] {
  def compare(x:DSCEvent, y:DSCEvent) = y.collector_tstamp.compareTo(x.collector_tstamp)

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
  def emptyHeap = List[DSCEvent]()
  def addOneEvent = (q:List[DSCEvent], u:DSCEvent) => (q :+ u).sortBy(_.collector_tstamp)(MinDateOrder)
  def mergeEventHeaps = (q1:List[DSCEvent], q2:List[DSCEvent]) => (q1 ++ q2).sortBy(_.collector_tstamp)(MinDateOrder)

}
