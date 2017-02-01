package com.DollarShave.codetest

import java.time.{ OffsetDateTime, ZonedDateTime}
import java.time.format.DateTimeFormatter

/**
  * Created by danielimberman on 29/01/17.
  */
case class DSCEvent(event_id: String, collector_tstamp: OffsetDateTime, domain_userid: String, page_urlpath: String, var next_event_id: String = "") extends Serializable{

  import DSCEvent._
  def toCsvEntry(addEventId: Boolean = false) = {
    def formatReleventTypes(t: Any): Any = {
      t match {
        case d: OffsetDateTime =>formatDate(d)
        case _ => t
      }
    }

    val fieldValues = this.productIterator.toList
    fieldValues.map( formatReleventTypes).mkString(",")
  }

  def setNextEventId(nextId:String) = this.next_event_id = nextId
}

object DSCEvent{
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSxxx")

  def formatDate(d:OffsetDateTime) = d.format(formatter).replace("+01:30","+00:00")

  /**
    * The reason that I want to keep these functions generic is that I would want to avoid
    * hardcoded values whenever possible. If the fields are hardcoded and then somebody changes the case class
    * we could end up with faulty data.
    */

  def getFields = {
    classOf[DSCEvent].getDeclaredFields.map(_.getName).toList
  }

  def parseNextId(fields:List[String]) = {
    if(fields.length > 4) fields(4) else ""
  }

  def parseDate(input:String) = {
    OffsetDateTime.parse(input)

  }


  def split(input: String):Either[String,DSCEvent] = {
    val a = input.split(",").toList
    try  Right(DSCEvent(a.head, parseDate(a(1)), a(2), a(3), parseNextId(a)))
    catch{case e:Exception => Left(s"error in csv splitting: ${e.getMessage}")}
  }

  def parseFromCSV(input: String):Either[String,DSCEvent] = {
    split(input)
  }
}