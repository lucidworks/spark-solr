package com.lucidworks.spark

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Collections, Date, TimeZone}

import com.lucidworks.spark.util.{SolrQuerySupport, SolrSupport}
import org.apache.solr.client.solrj.SolrQuery
import com.lucidworks.spark.util.QueryConstants._

import scala.util.control.Breaks._
import java.util.regex.Pattern

import scala.collection.JavaConversions._
import java.util.regex.Matcher
import java.util.TimeZone

import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser
import org.apache.lucene.search.{Query, TermRangeQuery}
import org.apache.lucene.util.BytesRef
import org.apache.solr.util.DateFormatUtil
import org.apache.spark.Logging


/**
  * Created by akashmehta on 6/16/16.
  */
class PartitionByTimeAssignmentStrategy(val zkHost: String,val tsFieldName:Option[String],val timePeriod:Option[String],val timeZone:Option[String],val dateTimePattern:Option[String],val baseCollection:String,val maxActivePartitions:Option[String],val queryOption:String) extends Logging{

  val solrCloudClient = SolrSupport.getCachedCloudClient(zkHost)
  val query:SolrQuery=SolrQuerySupport.toQuery(queryOption)
  val queryFilters: Array[String] = if (query.getFilterQueries != null) query.getFilterQueries else Array.empty[String]


  def getPartitionsForQuery():List[String]= {
    val prefixMatch=tsFieldName.getOrElse(DEFAULT_TS_FIELD_NAME) +":"
    var rangeQuery:String=null
    if(!queryFilters.isEmpty) {
       for(filter<-queryFilters){
        if (filter.startsWith(prefixMatch)){
          val rangeCrit= filter.substring(prefixMatch.length)
          if (!("[* TO *]" == rangeCrit)) {
            rangeQuery = filter
            break
          }

        }
      }
    }
    val allPartitions:List[String] = getPartitions(true)
    if (allPartitions.isEmpty) {
      log.warn("No filter query found to determine partitions and no time-based partitions exist in Solr, " + "returning base collection: {}", baseCollection)
      return List(baseCollection)
    }

    if (rangeQuery == null) {
      if (log.isDebugEnabled) {
        log.debug("No filter query available to select partitions, so using all partitions in Solr: {}", allPartitions)
      }
      return allPartitions
    }

    return getCollectionsForDateRangeQuery(rangeQuery, allPartitions)
  }

  def getPartitions(activeOnly: Boolean):List[String]= {
    val partitions: List[String] = findPartitions
    /* if (activeOnly) {
      val max_active_partitions=maxActivePartitions.getOrElse(null)
      if (maxActivePartitions != null) {
        val max_active_partitions_int=max_active_partitions.toInt
        val numToRemove: Int = partitions.size - max_active_partitions_int
        if (numToRemove > 0) partitions = partitions.slice(numToRemove, partitions.size)
      }
    }
    */
    return partitions
  }

  @throws[Exception]
  protected def findPartitions:List[String] = {

      val allCollections = solrCloudClient.getZkStateReader.getClusterState.getCollections
      val partitionMatchRegex: Pattern = getPartitionMatchRegex
      var partitions= List[String]()

      for (next <- allCollections) {
        val matcher: Matcher = partitionMatchRegex.matcher(next)
        if (matcher.matches) {
          partitions=next :: partitions
        }
      }
      if (!partitions.isEmpty) {
        partitions=partitions.sorted
      }


    return partitions
  }

  protected def getPartitionMatchRegex: Pattern = {
    var dtRegex: String =dateTimePattern.getOrElse(DEFAULT_DATETIME_PATTERN)
    dtRegex = dtRegex.replace("yyyy", "(\\d{4})")
    dtRegex = dtRegex.replace("yy", "(\\d{2})")
    dtRegex = dtRegex.replace("MM", "(1[0-2]|0[1-9])")
    dtRegex = dtRegex.replace("dd", "(3[0-1]|[0-2][0-9])")
    var underscore: String = ""
    var hoursAt: Int = dtRegex.indexOf("_HH")
    if (hoursAt != -1) {
      dtRegex = dtRegex.substring(0, hoursAt)
      underscore = "_"
    }
    else {
      hoursAt = dtRegex.indexOf("HH")
      if (hoursAt != -1) {
        dtRegex = dtRegex.substring(0, hoursAt)
      }
    }
    dtRegex += "(" + underscore + "(2[0-3]|[0-1][0-9]))?(" + underscore + "([0-5][0-9]))?"
    return Pattern.compile(baseCollection+"_"+dtRegex)
  }

  @throws[Exception]
  protected def getCollectionsForDateRangeQuery(rangeQuery: String, partitions:List[String]):List[String] = {
    val luceneQuery: Query = (new StandardQueryParser).parse(rangeQuery, rangeQuery.substring(0, rangeQuery.indexOf(":")))
    if (!(luceneQuery.isInstanceOf[TermRangeQuery])) throw new IllegalArgumentException("Failed to parse " + rangeQuery + " into a Lucene range query!")
    val rq: TermRangeQuery = luceneQuery.asInstanceOf[TermRangeQuery]
    val lower: String = bref2str(rq.getLowerTerm)
    val upper: String = bref2str(rq.getUpperTerm)
    if (lower == null && upper == null) {
      return partitions
    }
    val fromIndex: Int = if ((lower != null)) mapDateToExistingCollectionIndex(lower, partitions)
    else 0
    val toIndex: Int = if ((upper != null)) mapDateToExistingCollectionIndex(upper, partitions)
    else partitions.size - 1
    return partitions.slice(fromIndex, toIndex + 1)
  }

  @throws[ParseException]
  protected def mapDateToExistingCollectionIndex(dateCrit: String, partitions: List[String]): Int = {
    val collDate: Date = DateFormatUtil.parseMathLenient(null, dateCrit.toUpperCase, null)
    val coll: String = getCollectionNameForDate(collDate)
    val size: Int = partitions.size
    val lastIndex: Int = size - 1
    if (coll.compareTo(partitions.get(lastIndex)) > 0) {
      return lastIndex
    }
    var index: Int = -1
    var a: Int = 0
    while (a < size) {
      {
        if (coll == partitions.get(a)) {
          index = a
          break //todo: break is not supported
        }
        else {
          if (a < lastIndex) {
            if (coll.compareTo(partitions.get(a + 1)) < 0) {
              index = a
              break //todo: break is not supported
            }
          }
        }
      }
      ({
        a += 1; a - 1
      })
    }
    return index
  }

  private val dateFormatter: ThreadLocal[SimpleDateFormat] = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue: SimpleDateFormat = {
      val sdf: SimpleDateFormat = new SimpleDateFormat(dateTimePattern.getOrElse(DEFAULT_DATETIME_PATTERN))
      sdf.setTimeZone(TimeZone.getTimeZone(timeZone.getOrElse(DEFAULT_TIMEZONE_ID)))
      return sdf
    }
  }
  def getCollectionNameForDate(date: Date):String ={
    return baseCollection + dateFormatter.get.format(date)
  }
  private def bref2str(bytesRef: BytesRef): String = {
    return if ((bytesRef != null)) bytesRef.utf8ToString
    else null
  }
}

