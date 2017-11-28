package com.lucidworks.spark

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import com.typesafe.scalalogging.LazyLogging
import org.apache.solr.client.solrj.SolrQuery

import scala.collection.JavaConverters._
import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util.{ConfigurationConstants, SolrSupport}
import TimePartitioningQuery._
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser
import org.apache.lucene.search.{Query, TermRangeQuery}
import org.apache.lucene.util.BytesRef
import org.apache.solr.util.DateMathParser

import scala.collection.mutable.ListBuffer

class TimePartitioningQuery(solrConf: SolrConf, query: SolrQuery) extends LazyLogging {

  val dateTimePattern: String = getDateTimePattern
  val dateFormatter: ThreadLocal[SimpleDateFormat] = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue: SimpleDateFormat = {
      val sdf: SimpleDateFormat = new SimpleDateFormat(getDateTimePattern)
      sdf.setTimeZone(TimeZone.getTimeZone(solrConf.getTimeZoneId.getOrElse(DEFAULT_TIMEZONE_ID)))
      sdf
    }
  }

  def getPartitionsForQuery(): List[String] = {
    val timestampField = solrConf.getTimestampFieldName.getOrElse(DEFAULT_TIMESTAMP_FIELD_NAME)
    val timestampFilterPrefix = s"$timestampField:"

    // Get all partitions from cluster state
    val allPartitions: List[String] = getPartitions(true)

    if (query.getFilterQueries == null) {
      logger.warn(s"No filter query found in ${query}")
      return allPartitions
    }
    val rangeQueries = query.getFilterQueries
        .filter(fq => fq != null && !fq.isEmpty)
        .filter(fq => fq.startsWith(timestampFilterPrefix) && fq.substring(timestampFilterPrefix.length) != "[* TO *]")
    // TODO: What to do if there are multiple filter queries
    if (rangeQueries.isEmpty) {
      logger.warn(s"No range queries found in filter queries. Returning all partitions: ${allPartitions}")
      return allPartitions
    }
    logger.debug(s"All partitions returned for query are: ${allPartitions}")
    getCollectionsForRangeQueries(rangeQueries, allPartitions)
  }

  def getPartitions(activeOnly: Boolean): List[String] = {
    val partitions: List[String] = findAllPartitions
    if (activeOnly) {
      if (solrConf.getMaxActivePartitions.isDefined) {
        val maxActivePartitions = solrConf.getMaxActivePartitions.get.toInt
        if (maxActivePartitions < 1) {
          throw new IllegalArgumentException(s"Invalid value ${maxActivePartitions} for ${ConfigurationConstants.MAX_ACTIVE_PARTITIONS}. Set to positive number ")
        }
        val numOfPartitionsToRemove = partitions.size - maxActivePartitions
        if (numOfPartitionsToRemove > 0) {
          return partitions.slice(numOfPartitionsToRemove, partitions.size)
        }
      }
    }
    partitions
  }

  def findAllPartitions: List[String] = {
    val collections: Set[String] = SolrSupport.getCachedCloudClient(solrConf.getZkHost.get)
        .getZkStateReader.getClusterState.getCollectionsMap.keySet().asScala.toSet
    val partitionMatchRegex: Pattern = getPartitionMatchRegex

    val partitionListBuffer: ListBuffer[String] = ListBuffer.empty[String]
    collections.foreach(coll => {
      val matcher = partitionMatchRegex.matcher(coll)
      if (matcher.matches()) {
        partitionListBuffer.+=(coll)
      }
    })
    partitionListBuffer.toList.sorted
  }

  def getCollectionsForRangeQueries(rangeQueries: Array[String], partitions: List[String]): List[String] = {
    if (rangeQueries.length > 2)
      throw new IllegalArgumentException("Please consolidate date range filter criteria to at most 2 clauses!")
    if (rangeQueries.length == 2) {
      val query1Slice = getCollectionsForRangeQuery(rangeQueries(0), partitions)
      val query2Slice = getCollectionsForRangeQuery(rangeQueries(1), partitions)
      return query1Slice.intersect(query2Slice)
    }
    getCollectionsForRangeQuery(rangeQueries(0), partitions)
  }

  def getCollectionsForRangeQuery(rangeQuery: String, partitions: List[String]): List[String] = {
    val luceneQuery: Query = (new StandardQueryParser).parse(rangeQuery, rangeQuery.substring(0, rangeQuery.indexOf(":")))
    if (!luceneQuery.isInstanceOf[TermRangeQuery]) throw new IllegalArgumentException("Failed to parse " + rangeQuery + " into a Lucene range query!")
    val rq: TermRangeQuery = luceneQuery.asInstanceOf[TermRangeQuery]
    val lower: String = bref2str(rq.getLowerTerm)
    val upper: String = bref2str(rq.getUpperTerm)

    if (lower == null && upper == null) {
      return partitions
    }
    val fromIndex: Int = if (lower != null) mapToExistingCollIndex(lower, partitions) else 0
    val toIndex:Int = if (upper != null) mapToExistingCollIndex(upper, partitions) else partitions.size - 1
    logger.debug(s"Partitions fromIndex: ${fromIndex}. toIndex: ${toIndex}")
    partitions.slice(fromIndex, toIndex + 1)
  }

  def mapToExistingCollIndex(crit: String, partitions: List[String]): Int = {
    val collDate = DateMathParser.parseMath(null, crit.toUpperCase)
    val coll: String = getCollectionNameForDate(collDate)

    val size = partitions.size
    val lastIndex = size - 1
    if (coll.compareTo(partitions(lastIndex)) > 0) {
      return lastIndex
    }
    partitions.zipWithIndex.foreach{case(partition, index) =>
      if (coll == partition) return index
      if (index < lastIndex) if (coll.compareTo(partitions(index+1)) < 0) return index
    }
    -1
  }

  def getPartitionMatchRegex: Pattern = {
    var dtRegex: String = getDateTimePattern
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
    Pattern.compile(solrConf.getCollection.get + "_" + dtRegex)
  }

  def getDateTimePattern: String = {
    val timeUnit = getTimeUnit
    val defaultDateTimePattern = {
      if (timeUnit eq TimeUnit.HOURS) "yyyy_MM_dd_HH"
      else if (timeUnit eq TimeUnit.MINUTES) "yyyy_MM_dd_HH_mm"
      else DEFAULT_DATETIME_PATTERN
    }
    solrConf.getDateTimePattern.getOrElse(defaultDateTimePattern)
  }

  def getTimeUnit: TimeUnit = {
    val timePeriod = solrConf.getTimePeriod.getOrElse(DEFAULT_TIME_PERIOD)
    val matcher = TIMEPERIOD_PATTERN.matcher(timePeriod)
    if (!matcher.matches) {
      throw new IllegalArgumentException("Invalid timePeriod "+ timePeriod)
    }
    TimeUnit.valueOf(matcher.group(2))
  }

  def getCollectionNameForDate(date: Date): String = {
    solrConf.getCollection.get + "_" + dateFormatter.get().format(date)
  }
}

object TimePartitioningQuery {
  val TIMEPERIOD_PATTERN: Pattern = Pattern.compile("^(\\d{1,4})(MINUTES|HOURS|DAYS)$")

  def bref2str(bytesRef: BytesRef): String = {
    if (bytesRef != null) bytesRef.utf8ToString
    else null
  }
}
