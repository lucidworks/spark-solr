package com.lucidworks.spark

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Collections, Date, TimeZone}
import com.lucidworks.spark.util.{SolrQuerySupport, SolrRelationUtil, SolrSupport}
import org.apache.solr.client.solrj.SolrQuery
import com.lucidworks.spark.util.QueryConstants._
import scala.util.control._
import java.util.regex.Pattern
import scala.collection.JavaConversions._
import java.util.regex.Matcher
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser
import org.apache.lucene.search.{Query, TermRangeQuery}
import org.apache.lucene.util.BytesRef
import org.apache.spark.Logging
import org.apache.solr.util.DateMathParser;


/**
  * This class is used to query multiple collections of time series data given a range query.
  */
class PartitionByTimeQuerySupport(val feature: PartitionByTimeQueryParams,val conf: SolrConf) extends Logging{

  val solrCloudClient = SolrSupport.getCachedCloudClient(conf.getZkHost.get)
  val query:SolrQuery=buildQuery
  val loop = new Breaks
  var queryFilters: Array[String] = if (query.getFilterQueries != null) query.getFilterQueries else Array.empty[String]

  private def buildQuery: SolrQuery = {
    val query = SolrQuerySupport.toQuery(conf.getQuery.getOrElse("*:*"))


    query.setRows(scala.Int.box(conf.getRows.getOrElse(DEFAULT_PAGE_SIZE)))
    query.add(conf.getArbitrarySolrParams)
    query.set("collection", conf.getCollection.get)
    query
  }

  def getPartitionsForQuery():List[String]= {
    val prefixMatch=conf.getTimeStampFieldName.getOrElse(DEFAULT_TIME_STAMP_FIELD_NAME) +":"
    var rangeQuery:String=null
    if(!queryFilters.isEmpty) {
      loop.breakable {
        for (filter <- queryFilters) {
          if (filter.startsWith(conf.getTimeStampFieldName.getOrElse(DEFAULT_TIME_STAMP_FIELD_NAME) +":")) {
            val rangeCrit = filter.substring(prefixMatch.length)
            if (!("[* TO *]" == rangeCrit)) {
              rangeQuery = filter
              loop.break
            }

          }
        }
      }
    }

    val allPartitions:List[String] = getPartitions(true)

    if (allPartitions.isEmpty) {
      if(log.isWarnEnabled) {
        log.warn(s"No filter query found to determine partitions and no time-based partitions exist in Solr returning base collection: ${conf.getCollection.get}")
      }
      return List(conf.getCollection.get)
    }

    if (rangeQuery == null) {

      if (log.isWarnEnabled) {
        log.warn(s"No filter query available to select partitions, so using all partitions in Solr: ${allPartitions}")
      }
      return allPartitions
    }

     getCollectionsForDateRangeQuery(rangeQuery, allPartitions)
  }

  def getPartitions(activeOnly: Boolean):List[String]= {
    var partitions: List[String] = findPartitions
     if (activeOnly) {
      if (feature.getMaxActivePartitions != -1) {
        val numToRemove: Int = partitions.size - feature.getMaxActivePartitions
        if (numToRemove > 0) partitions = partitions.slice(numToRemove, partitions.size)
      }
    }

   partitions
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

    partitions
  }

  protected def getPartitionMatchRegex: Pattern = {
    var dtRegex: String =feature.getDateTimePattern
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
    Pattern.compile(conf.getCollection.get+"_"+dtRegex)
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
    partitions.slice(fromIndex, toIndex + 1)
  }

  @throws[ParseException]
  protected def mapDateToExistingCollectionIndex(dateCrit: String, partitions: List[String]): Int = {

    val collDate =DateMathParser.parseMath(null.asInstanceOf[Date], dateCrit.toUpperCase);
    val coll: String = feature.getCollectionNameForDate(collDate)
    val size: Int = partitions.size
    val lastIndex: Int = size - 1
    if (coll.compareTo(partitions.get(lastIndex)) > 0) {
      return lastIndex
    }
    var index: Int = -1
    var a: Int = 0
    loop.breakable{
    for (a <- 0 to size) {
      if (coll == partitions.get(a)) {
        index = a
        loop.break
      }
      else {
        if (a < lastIndex) {
          if (coll.compareTo(partitions.get(a + 1)) < 0) {
            index = a
            loop.break
          }
        }
      }
    }
  }
     index
  }


  private def bref2str(bytesRef: BytesRef): String = {
    if (bytesRef != null) bytesRef.utf8ToString
    else null
  }
}

