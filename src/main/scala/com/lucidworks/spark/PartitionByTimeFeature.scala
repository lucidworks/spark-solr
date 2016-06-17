package com.lucidworks.spark

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import java.util.concurrent.TimeUnit
import java.util.regex.{Matcher, Pattern}

import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util.{SolrQuerySupport, SolrSupport}
import org.apache.solr.client.solrj.SolrQuery

/**
  * Created by akashmehta on 6/17/16.
  */
class PartitionByTimeFeature(val conf:SolrConf) {
  private val TIME_PERIOD_PATTERN: Pattern = Pattern.compile("^(\\d{1,4})(MINUTES|HOURS|DAYS)$")
  private val dateFormatter: ThreadLocal[SimpleDateFormat] = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue: SimpleDateFormat = {
      val sdf: SimpleDateFormat = new SimpleDateFormat(conf.getDateTimePattern.getOrElse(DEFAULT_DATETIME_PATTERN))
      sdf.setTimeZone(TimeZone.getTimeZone(conf.getTimeZoneId.getOrElse(DEFAULT_TIMEZONE_ID)))
      sdf
    }
  }
  private var timestampFieldName: String = null
  private var timezoneId: String = null
  private var timeFrame: Int = 0
  private var timeUnit: TimeUnit = null
  private var timeFrameMs: Long = 0L
  private var dateTimePattern: String = null
  private var partitionNamePrefix: String = null
  private var maxActivePartitions: String = null

  val timePeriod: String = conf.getTimePeriod.getOrElse(DEFAULT_TIME_PERIOD)
  val matcher: Matcher = TIME_PERIOD_PATTERN.matcher(timePeriod)
  if (!matcher.matches) {
    throw new IllegalArgumentException("Invalid timePeriod "+ timePeriod)
  }

  timestampFieldName = conf.getTSFieldName.getOrElse(DEFAULT_TS_FIELD_NAME)
  timezoneId = conf.getTimeZoneId.getOrElse(DEFAULT_TIMEZONE_ID)
  partitionNamePrefix = conf.getCollection.get+ "_"
  timeFrame = matcher.group(1).toInt
  timeUnit = TimeUnit.valueOf(matcher.group(2))
  timeFrameMs = TimeUnit.MILLISECONDS.convert(timeFrame, timeUnit)
  dateTimePattern = conf.getDateTimePattern.getOrElse(getDefaultDateTimePattern(timeUnit))




  maxActivePartitions = conf.getMaxActivePartitions.getOrElse(null)
  if (maxActivePartitions!= null && maxActivePartitions.toInt <= 0) {
    throw new IllegalArgumentException("Value for  MAX_ACTIVE_PARTITIONS  must be strictly greater than zero!")
  }

  val solrCloudClient = SolrSupport.getCachedCloudClient(conf.getZkHost.get)
  val query:SolrQuery=SolrQuerySupport.toQuery(conf.getQuery.getOrElse("*:*"))
  val queryFilters: Array[String] = if (query.getFilterQueries != null) query.getFilterQueries else Array.empty[String]

  protected def getDefaultDateTimePattern(timeUnit: TimeUnit): String = {
    if (timeUnit eq TimeUnit.HOURS) {
      return "yyyy_MM_dd_HH"
    }
    else if (timeUnit eq TimeUnit.MINUTES) {
      return "yyyy_MM_dd_HH_mm"
    }
    return DEFAULT_DATETIME_PATTERN
  }


  def getCollectionNameForDate(date: Date):String ={
    return partitionNamePrefix + dateFormatter.get.format(date)
  }

}
